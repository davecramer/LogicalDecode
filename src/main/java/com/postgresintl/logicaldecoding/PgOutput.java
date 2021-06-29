package com.postgresintl.logicaldecoding;

import com.postgresintl.logicaldecoding.model.Attribute;
import com.postgresintl.logicaldecoding.model.Relation;
import org.postgresql.replication.LogSequenceNumber;

import java.nio.ByteBuffer;
import java.sql.Timestamp;

public class PgOutput {
    ByteBuffer buffer;
    long nanoTime = 0L;
    public PgOutput(ByteBuffer b){
      buffer = b;
    }
     public String toString() {

        byte cmd = buffer.get();
        switch (cmd) {
            case 'R':
                Relation relation = new Relation();
                relation.setOid(buffer.getInt());

                relation.setSchema(getString(buffer));
                relation.setName(getString(buffer));
                byte replicaIdent = buffer.get();
                short numAttrs = buffer.getShort();
                for (int i = 0; i < numAttrs;i++){
                    byte replicaIdentityFull = buffer.get();
                    String attrName=getString(buffer);
                    int attrId = buffer.getInt();
                    int attrMode = buffer.getInt();
                    relation.addAttribute(new Attribute(attrId, attrName, attrMode, replicaIdentityFull));
                }

                return "SCHEMA: " + relation.toString();

            case 'B':
                nanoTime = System.nanoTime();
                System.out.println("Begin Issued: " + nanoTime);
                LogSequenceNumber finalLSN = LogSequenceNumber.valueOf(buffer.getLong());
                Timestamp commitTime = new Timestamp(buffer.getLong());
                int transactionId = buffer.getInt();
                return "BEGIN final LSN: " + finalLSN.toString() + " Commit Time: " + commitTime + " XID: " + transactionId;

            case 'C':
                // COMMIT
                System.out.println("Commit Issued: " + (System.nanoTime() -  nanoTime));
                byte unusedFlag = buffer.get();
                LogSequenceNumber commitLSN = LogSequenceNumber.valueOf( buffer.getLong() );
                LogSequenceNumber endLSN = LogSequenceNumber.valueOf( buffer.getLong() );
                commitTime = new Timestamp(buffer.getLong());
                return "COMMIT commit LSN:" + commitLSN.toString() + " end LSN:" + endLSN.toString() + " commitTime: " + commitTime;

            case 'U': // UPDATE
            case 'D': // DELETE
                StringBuffer sb = new StringBuffer(cmd=='U'?"UPDATE: ":"DELETE: ");
                int oid = buffer.getInt();
                /*
                 this can be O or K if Delete or possibly N if UPDATE
                 K means key
                 O means old data
                 N means new data
                 */
                char keyOrTuple = (char)buffer.get();
                getTuple(buffer, sb);
                return sb.toString();

            case 'I':
                sb = new StringBuffer("INSERT: ");
                // oid of relation that is being inserted
                oid = buffer.getInt();
                // should be an N
                char isNew = (char)buffer.get();
                getTuple(buffer, sb);
                return sb.toString();
        }
        return "";
    }

    private void getTuple(ByteBuffer buffer, StringBuffer sb) {
        short numAttrs;
        numAttrs = buffer.getShort();
        for (int i = 0; i < numAttrs; i++) {
            byte c = buffer.get();
            switch (c) {
                case 'n': // null
                    sb.append("NULL, ");
                    break;
                case 'u': // unchanged toast column
                    break;
                case 't': // textual data
                    int strLen = buffer.getInt();
                    byte[] bytes = new byte[strLen];
                    buffer.get(bytes, 0, strLen);
                    String value = new String(bytes);
                    sb.append(value).append(", ");
                    break;
                default:
                    sb.append("command: ").append((char) c);

            }
        }
    }
    private  String getString(ByteBuffer buffer){
        StringBuffer sb = new StringBuffer();
        while ( true ){
            byte c = buffer.get();
            if ( c == 0 ) {
                break;
            }
            sb.append((char)c);
        }
        return sb.toString();
    }
}
