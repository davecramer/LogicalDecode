/*
BSD 3-Clause License

Copyright (c) 2017, Dave Cramer
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.postgresintl.logicaldecoding.exercises._03;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.ServerVersion;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Logical Decoding App
 *
 */
public class App 
{
    private final static String SLOT_NAME="slot";
    private final static String PORT="15432";
    private final static String HOST="localhost";
    private final static String DB = "test";

    Connection connection;


    Connection replicationConnection;

    private static String toString(ByteBuffer buffer) {
        int offset = buffer.arrayOffset();
        byte[] source = buffer.array();
        int length = source.length - offset;

        return new String(source, offset, length);
    }

    public void createConnection()
    {
        try
        {
            Properties props = new Properties();
            props.setProperty(PGProperty.USER.getName(),"test");
            props.setProperty(PGProperty.PASSWORD.getName(),"");

            connection = DriverManager.getConnection("jdbc:postgresql://"+HOST+':'+PORT+'/'+DB, props);
        }
        catch (SQLException ex)
        {
            ex.printStackTrace();
        }

    }
    public void createLogicalReplicationSlot(String slotName, String outputPlugin ) throws InterruptedException, SQLException, TimeoutException
    {
        //drop previos slot
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
        st.close();

        PGReplicationStream stream =
                pgConnection
                        .getReplicationAPI()
                        .replicationStream()
                        .logical()
                        .withSlotName(SLOT_NAME)
                        .withStartPosition(lsn)
                        .withSlotOption("include-xids", true)
                        .withSlotOption("skip-empty-xacts", true)
                        .withStatusInterval(20, TimeUnit.SECONDS)
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
                    ? "pg_current_wal_location()" : "pg_current_xlog_location()"))) {

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
 /*
 open a connection capable of replication
  */

    }

    public static void main( String[] args )
    {
        String pluginName = "test_decoding";

        App app = new App();
        app.createConnection();
        try {
            app.createLogicalReplicationSlot(SLOT_NAME, pluginName );
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
