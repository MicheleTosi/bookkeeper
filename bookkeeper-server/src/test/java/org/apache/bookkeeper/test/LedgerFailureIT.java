package org.apache.bookkeeper.test;

import org.apache.bookkeeper.bookie.BookKeeperClusterTestCase;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

public class LedgerFailureIT extends BookKeeperClusterTestCase {
    private Logger LOG = LoggerFactory.getLogger(LedgerFailureIT.class);

    public static AsyncCallback.AddCallback generateMockedNopCb() {
        AsyncCallback.AddCallback nopCb = mock(AsyncCallback.AddCallback.class);
        /* A NOP callback should do nothing when invoked */
        doNothing().when(nopCb).addComplete(isA(Integer.class), isA(LedgerHandle.class), isA(Long.class),
                isA(Object.class));

        return nopCb;
    }

    private BookKeeper bookKeeper;
    private LedgerHandle ledgerHandle;
    private List<Long> entriesId;

    public LedgerFailureIT() {
        super(3);
    }

    @Before
    public void setup() throws Exception {
        // Inizializza BookKeeper utilizzando la configurazione
        ClientConfiguration bkConf = TestBKConfiguration.newClientConfiguration();
        bkConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        bookKeeper = new BookKeeper(bkConf);

        // Crea un nuovo ledger di test
        ledgerHandle = bookKeeper.createLedger(BookKeeper.DigestType.CRC32, "testPassword".getBytes());
        entriesId=new ArrayList<>();
    }

    @After
    public void cleanup() throws Exception {
        // Chiude il ledger e BookKeeper
        if (ledgerHandle != null) {
            ledgerHandle.close();
        }
        if (bookKeeper != null) {
            bookKeeper.close();
        }
    }

    @Test
    public void testLedgerIntegration() throws IOException, BookieException {
        // Scrivi le voci nel ledger
        try {
            writeEntriesToLedger(10);
        } catch (InterruptedException | BKException | ExecutionException e) {
            e.printStackTrace();
        }

        // Verifica che il ripristino sia stato eseguito correttamente
        verifyLedgerRecovery();

        for (ServerTester server: servers) {
            //LOG.info(server.getServer().getBookie().toString());
            try {
                LOG.info(new String(server.getServer().getBookie().readEntry(ledgerHandle.getId(), 1).array(), StandardCharsets.UTF_8));
            }catch(Exception e){
                //nop
            }
        }
    }

    private void writeEntriesToLedger(int numEntries) throws ExecutionException, InterruptedException, BKException {
        // Write entries to the ledger
        CompletableFuture<Void> writesFuture = new CompletableFuture<>();
        for (int i = 0; i < numEntries; i++) {
            String entryData = "Entry " + i;
            byte[] entryBytes = entryData.getBytes(StandardCharsets.UTF_8);
            ledgerHandle.asyncAddEntry(entryBytes, new AsyncCallback.AddCallback() {
                @Override
                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    if (entryId == numEntries - 1) {
                        writesFuture.complete(null); // All writes completed
                    }
                    entriesId.add(entryId);
                }
            }, null);

            // Simulate ledger failure after a certain number of writes
            if (i == 3 - 1) {
                simulateLedgerFailure();
            }
        }
        writesFuture.get();
    }

    private void simulateLedgerFailure() throws BKException, InterruptedException {
        // Simula il fallimento del ledger dopo un certo numero di scritture
        int numFailures = 5;

        if (ledgerHandle.getLastAddConfirmed() >= numFailures) {
            ledgerHandle.close();
            /*throw new Bookie.NoLedgerException(ledgerHandle.getId())*/
            throw new RuntimeException("Simulated ledger failure");
        }
    }

    private void recoverLedger() {
        CountDownLatch recoveryLatch = new CountDownLatch(1);

        // Avvia il ripristino del ledger
        bookKeeper.asyncOpenLedger(ledgerHandle.getId(), BookKeeper.DigestType.CRC32, "testPassword".getBytes(),
                (rc, recoveredLedgerHandle, ctx) -> {
                    if (rc == 0) {
                        // Ripristino completato con successo
                        ledgerHandle = recoveredLedgerHandle;
                    } else {
                        // Ripristino fallito
                        throw new RuntimeException("Failed to recover ledger: " + rc);
                    }
                    recoveryLatch.countDown();
                }, null);

        // Attendere il completamento del ripristino
        try {
            if (!recoveryLatch.await(5, TimeUnit.SECONDS)) {
                LOG.error("Ledger recovery timeout");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void verifyLedgerRecovery() throws IOException, BookieException {
        // Verifica che il ripristino sia stato eseguito correttamente
        long expectedEntries = ledgerHandle.getLastAddConfirmed()+1;
        int i=0;
        LOG.info(servers.size()+" hey" );
        assertEquals(10, expectedEntries);
    }
}
