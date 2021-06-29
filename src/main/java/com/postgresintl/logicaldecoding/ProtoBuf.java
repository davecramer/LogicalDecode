package com.postgresintl.logicaldecoding;

import com.google.protobuf.Message;
import com.postgresintl.logicaldecoding.proto.PgProto;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ProtoBuf {
    ByteBuffer byteBuffer;

    public ProtoBuf(ByteBuffer b){
        byteBuffer = b;
    }
    public String toString() {
        int offset = byteBuffer.arrayOffset();
        byte[] source = byteBuffer.array();
        int length = source.length - offset;
        final byte[] content = Arrays.copyOfRange(source, offset, length);
        try {
            final PgProto.RowMessage message = PgProto.RowMessage.parseFrom(content);
            return message.toString();
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }


    }
}
