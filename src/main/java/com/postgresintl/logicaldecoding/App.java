package com.postgresintl.logicaldecoding;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.postgresintl.logicaldecoding.model.Attribute;
import com.postgresintl.logicaldecoding.model.Relation;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.ServerVersion;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;



/**
 * Logical Decoding App
 *
 */
public class App 
{
    private final static String SLOT_NAME="slot";
    private final static String HOST="127.0.0.1";
    private final static String PORT="5432";
    private final static String DATABASE="test";

    Connection connection;
    Connection replicationConnection;


    private static String toString(ByteBuffer buffer) {

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

                return relation.toString();

            case 'B':
               LogSequenceNumber finalLSN = LogSequenceNumber.valueOf(buffer.getLong());
               Timestamp commitTime = new Timestamp(buffer.getLong());
               int transactionId = buffer.getInt();
               return "BEGIN finalLSN: " + finalLSN.toString() + " Commit Time: " + commitTime + " XID: " + transactionId;

            case 'C':
                // COMMIT
                byte unusedFlag = buffer.get();
                LogSequenceNumber commitLSN = LogSequenceNumber.valueOf( buffer.getLong() );
                LogSequenceNumber endLSN = LogSequenceNumber.valueOf( buffer.getLong() );
                commitTime = new Timestamp(buffer.getLong());
                return "COMMIT commitLSN:" + commitLSN.toString() + " endLSN:" + endLSN.toString() + " commitTime: " + commitTime;

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
                sb = new StringBuffer();
                // oid of relation that is being inserted
                oid = buffer.getInt();
                // should be an N
                char isNew = (char)buffer.get();
                getTuple(buffer, sb);
                return sb.toString();
        }
        return "";
    }

    private static void getTuple(ByteBuffer buffer, StringBuffer sb) {
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

    private static String getString(ByteBuffer buffer){
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
    private String createUrl(){
        return "jdbc:postgresql://"+HOST+':'+PORT+'/'+DATABASE;
    }
    public void createConnection()
    {
        try
        {
            connection = DriverManager.getConnection(createUrl(),"test","test");
        }
        catch (SQLException ex)
        {
            ex.printStackTrace();
        }

    }

    public void dropPublication(String publication) throws SQLException {

        try (PreparedStatement preparedStatement =
                 connection.prepareStatement("DROP PUBLICATION " + publication ) )
        {
            preparedStatement.execute();
        }
    }
    public void createPublication(String publication) throws SQLException {

        try (PreparedStatement preparedStatement =
                 connection.prepareStatement("CREATE PUBLICATION " + publication + " FOR ALL TABLES") )
        {
            preparedStatement.execute();
        }
    }


    public void createLogicalReplicationSlot(String slotName, String outputPlugin ) throws InterruptedException, SQLException, TimeoutException
    {
        //drop previous slot
        dropReplicationSlot(connection, slotName);

        try (PreparedStatement preparedStatement =
                     connection.prepareStatement("SELECT * FROM pg_create_logical_replication_slot(?, ?)") )
        {
            preparedStatement.setString(1, slotName);
            preparedStatement.setString(2, outputPlugin);
            try (ResultSet rs = preparedStatement.executeQuery())
            {
                while (rs.next())
                {
                    System.out.println("Slot Name: " + rs.getString(1));
                    System.out.println("Xlog Position: " + rs.getString(2));
                }
            }

        }
    }

    public void dropReplicationSlot(Connection connection, String slotName)
            throws SQLException, InterruptedException, TimeoutException
    {
        try (PreparedStatement preparedStatement = connection.prepareStatement(
                        "select pg_terminate_backend(active_pid) from pg_replication_slots "
                                + "where active = true and slot_name = ?"))
        {
            preparedStatement.setString(1, slotName);
            preparedStatement.execute();
        }

        waitStopReplicationSlot(connection, slotName);

        try (PreparedStatement preparedStatement = connection.prepareStatement("select pg_drop_replication_slot(slot_name) "
                            + "from pg_replication_slots where slot_name = ?")) {
            preparedStatement.setString(1, slotName);
            preparedStatement.execute();
        }
    }

    public  boolean isReplicationSlotActive(Connection connection, String slotName)
            throws SQLException
    {

        try (PreparedStatement preparedStatement = connection.prepareStatement("select active from pg_replication_slots where slot_name = ?")){
            preparedStatement.setString(1, slotName);
            try (ResultSet rs = preparedStatement.executeQuery())
            {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    private  void waitStopReplicationSlot(Connection connection, String slotName)
            throws InterruptedException, TimeoutException, SQLException
    {
        long startWaitTime = System.currentTimeMillis();
        boolean stillActive;
        long timeInWait = 0;

        do {
            stillActive = isReplicationSlotActive(connection, slotName);
            if (stillActive) {
                TimeUnit.MILLISECONDS.sleep(100L);
                timeInWait = System.currentTimeMillis() - startWaitTime;
            }
        } while (stillActive && timeInWait <= 30000);

        if (stillActive) {
            throw new TimeoutException("Wait stop replication slot " + timeInWait + " timeout occurs");
        }
    }
    public void receiveChangesOccursBeforStartReplication() throws Exception {
        PGConnection pgConnection = (PGConnection) replicationConnection;

        LogSequenceNumber lsn = getCurrentLSN();

        Statement st = connection.createStatement();
        st.execute("insert into test_logical_table(name) values('previous value')");
        st.execute("insert into test_logical_table(name) values('previous value')");
        st.execute("insert into test_logical_table(name) values('previous value')");
        st.close();

        PGReplicationStream stream =
                pgConnection
                        .getReplicationAPI()
                        .replicationStream()
                        .logical()
                        .withSlotName(SLOT_NAME)
                        .withStartPosition(lsn)
                        .withSlotOption("proto_version",1)
                        .withSlotOption("publication_names", "pub1")
                     //  .withSlotOption("include-xids", true)
                    //    .withSlotOption("skip-empty-xacts", true)
                        .withStatusInterval(10, TimeUnit.SECONDS)
                        .start();
        ByteBuffer buffer;
        while(true)
        {
            buffer = stream.readPending();
            if (buffer == null) {
                TimeUnit.MILLISECONDS.sleep(10L);
                continue;
            }

            System.out.println( toString(buffer));
            //feedback
            stream.setAppliedLSN(stream.getLastReceiveLSN());
            stream.setFlushedLSN(stream.getLastReceiveLSN());
        }

    }

    private LogSequenceNumber getCurrentLSN() throws SQLException
    {
        try (Statement st = connection.createStatement())
        {
            try (ResultSet rs = st.executeQuery("select "
                    + (((BaseConnection) connection).haveMinimumServerVersion(ServerVersion.v10)
                    ? "pg_current_wal_lsn()" : "pg_current_xlog_location()"))) {

                if (rs.next()) {
                    String lsn = rs.getString(1);
                    return LogSequenceNumber.valueOf(lsn);
                } else {
                    return LogSequenceNumber.INVALID_LSN;
                }
            }
        }
    }

    private void openReplicationConnection() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user","test");
        properties.setProperty("password","test");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
        PGProperty.REPLICATION.set(properties, "database");
        PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
        replicationConnection = DriverManager.getConnection(createUrl(),properties);
    }
    private boolean isServerCompatible() {
        return ((BaseConnection)connection).haveMinimumServerVersion(ServerVersion.v9_5);
    }
    public static void main( String[] args )
    {
        String pluginName = "pgoutput";

        App app = new App();
        app.createConnection();
        if (!app.isServerCompatible() ) {
            System.err.println("must have server version greater than 9.4");
            System.exit(-1);
        }
        try {
            app.createLogicalReplicationSlot(SLOT_NAME, pluginName );
           app.dropPublication("pub1");
            app.createPublication("pub1");
            app.openReplicationConnection();
            app.receiveChangesOccursBeforStartReplication();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
