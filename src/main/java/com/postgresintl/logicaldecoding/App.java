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
    private final static String HOST="localhost";
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

                return "SCHEMA: " + relation.toString();

            case 'B':
               LogSequenceNumber finalLSN = LogSequenceNumber.valueOf(buffer.getLong());
               Timestamp commitTime = new Timestamp(buffer.getLong());
               int transactionId = buffer.getInt();
               return "BEGIN final LSN: " + finalLSN.toString() + " Commit Time: " + commitTime + " XID: " + transactionId;

            case 'C':
                // COMMIT
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
            connection.createStatement().execute("set wal_debug=true");
        }
        catch (SQLException ex)
        {

        }

    }

    public void dropPublication(String publication) throws SQLException {

        ResultSet rs = null;
        try {
            /* check to make sure it's really there */
            rs = connection.createStatement().executeQuery("select * from pg_publication where pubname='" + publication + "'");
            if (rs.next()) {
                try (PreparedStatement preparedStatement =
                             connection.prepareStatement("DROP PUBLICATION " + publication)) {
                    preparedStatement.execute();
                }
            }
        }
        finally {
            if (rs !=null) rs.close();
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
    static String [] commands = {
        "drop table if exists t0",
        "create table if not exists t0(pk serial primary key, val integer)",
        "alter table t0 replica identity full",
        "insert into t0 values( 1, 1)",
        "insert into t0 values( 2, 1)",
        "insert into t0 values( 3, 1)",
        "insert into t0 values( 4, 1)",
        "insert into t0 values( 5, 1)",
        "alter table t0 alter column val type bigint",
        "alter table t0  add column val2 integer",
        "insert into t0 values( 6, 1,1)",
        "drop table t0",
        "create table t0(pk serial primary key, val3 bigint)",
        "alter table t0 replica identity full",
        "insert into t0 values( 7, 1)",
        "insert into t0 values( 8, 1)"
    };
    static String [] commands2 = {
        "insert into t0 values( 9, 1)",
        "insert into t0 values( 10, 1)",
        "drop table t0",
        "create table t0(pk serial primary key, val3 int)",
        "alter table t0 replica identity full",
        "insert into t0 values( 11, 1)"
    };

    public void dosomestuff(String []cmds) throws Exception {
        Statement st = connection.createStatement();
        for (int i =0; i< cmds.length; i++) {
            st.execute(cmds[i]);
        }
    }
    static PGReplicationStream stream;

    public void openStream() throws Exception {
        PGConnection pgConnection = (PGConnection) replicationConnection;

        LogSequenceNumber lsn = LogSequenceNumber.INVALID_LSN; //getCurrentLSN();

        /*
        Statement st = connection.createStatement();
        st.execute("insert into test_logical_table(name) values('previous value')");
        st.execute("insert into test_logical_table(name) values('previous value')");
        st.execute("insert into test_logical_table(name) values('previous value')");
        st.close();
*/
        stream =
                pgConnection
                        .getReplicationAPI()
                        .replicationStream()
                        .logical()
                        .withSlotName(SLOT_NAME)
                        .withStartPosition(lsn)
                        .withSlotOption("proto_version",1)
                        .withSlotOption("publication_names", "pub1")
                    //   .withSlotOption("include-xids", true)
                    //    .withSlotOption("skip-empty-xacts", true)
                        .withStatusInterval(10, TimeUnit.SECONDS)
                        .start();

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

    static boolean active = true;

    public static class ReadChanges implements Runnable {

        ByteBuffer buffer;
        public void run() {
            while (true) {
                try {
                    buffer = stream.readPending();
                    if (buffer == null) {
                        TimeUnit.MILLISECONDS.sleep(10L);
                        continue;
                    }

                    System.out.println(App.toString(buffer));
                    //feedback
                    stream.setAppliedLSN(stream.getLastReceiveLSN());
                    stream.setFlushedLSN(stream.getLastReceiveLSN());
                } catch ( Exception ex ) {

                }
            }
        }
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
            app.dosomestuff(commands);
            app.openReplicationConnection();
            app.openStream();
            new Thread(new ReadChanges()).start();
            Thread.sleep(5000);
            app.replicationConnection.close();
            app.dosomestuff(commands2);
            app.openReplicationConnection();
            app.openStream();
            Thread.sleep(5000);
            app.replicationConnection.close();
            app.dropPublication("pub1");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            app.dosomestuff(commands2);
            app.openReplicationConnection();
            app.openStream();
            app.replicationConnection.close();
            app.dropPublication("pub1");
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
