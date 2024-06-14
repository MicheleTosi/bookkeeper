package org.apache.bookkeeper.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class EntryBuilder {

    private static final byte[] buff = "Hello, world".getBytes();

    public static ByteBuf getValidEntry(){
        ByteBuf validEntry= Unpooled.buffer(3*Long.BYTES+buff.length);
        validEntry.writeLong(0L);
        validEntry.writeLong(0L);
        validEntry.writeLong(0L);
        validEntry.writeBytes(buff);
        return validEntry;
    }

    public static ByteBuf getInvalidEntry(){
        ByteBuf invalidEntry= Unpooled.buffer(buff.length);
        invalidEntry.writeBytes(buff);
        return invalidEntry;
    }

}
