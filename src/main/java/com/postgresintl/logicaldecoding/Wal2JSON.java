package com.postgresintl.logicaldecoding;

import java.nio.ByteBuffer;

public class Wal2JSON {
    ByteBuffer byteBuffer;
    public Wal2JSON(ByteBuffer b){
        byteBuffer = b;
    }

    @Override
    public String toString() {
        int offset = byteBuffer.arrayOffset();
        byte[] source = byteBuffer.array();
        int length = source.length - offset;

        return new String(source, offset, length);
    }
}
