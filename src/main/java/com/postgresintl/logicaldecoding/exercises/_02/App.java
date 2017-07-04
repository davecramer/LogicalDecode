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
package com.postgresintl.logicaldecoding.exercises._02;

import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    }    public void createLogicalReplicationSlot(String slotName, String outputPlugin ) throws InterruptedException, SQLException, TimeoutException
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

        /*

        drop any existing slots
        hint: have to terminate any existing backends using the slots see pg_replication_slots view
        wait for the slot to terminate
        see pg_drop_replication_slot(slot_name_
         */
    }

    public  boolean isReplicationSlotActive(Connection connection, String slotName)
            throws SQLException
    {

        /*
        check pg_replication_slots for active slot
         */
        return true;
    }

    private  void waitStopReplicationSlot(Connection connection, String slotName)
            throws InterruptedException, TimeoutException, SQLException
    {
        /*
        wait for active slots to die
         */

    }

    public static void main( String[] args )
    {
        String pluginName = "test_decoding";

        App app = new App();
        app.createConnection();
        try {
            app.createLogicalReplicationSlot(SLOT_NAME, pluginName );
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
