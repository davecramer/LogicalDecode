# LogicalDecode
## Demo for PostgreSQL Logical Decoding with JDBC

### What is Logical decoding?

It's useful to understand what physical replication is in order to understand logical decoding.

Physical replication extends the functionality of recovery mode.  Write Ahead Logs are written to disk before the actual database. These files contain enough information to recreate the transaction in the event of a catastrophic shutdown

In the event of an emergency shutdown (power fail, OOM kill) when the server comes back online it will attempt to apply the outstanding WAL up to the point of the shutdown. This is referred to as recovery mode.

Physical replication takes advantage of this infrastructure built into the server. The standby is started in recovery mode and WAL created by the master are applied to the standby. How that occurs is beyond the scope but you can read about it [here](https://www.postgresql.org/docs/9.6/static/continuous-archiving.html) .

The interesting bit here is that we have a mechanism by which to access the changes in the heap without connecting to the database.

There are a few caveats though which is where Logical Decoding comes to the rescue. First; WAL's are binary and their format is not guaranteed to be stable (in other words they can change from version to version) and second they contain changes for every database in the server.

Logical decoding changes all of that by

Providing changes for only one database per slot
Defining an API which facilitates writing an output plugin to output the changes in any format you define.


### Concepts of Logical Decoding

Above I mentioned two new concepts slots, and plugins

A slot is a stream of changes in a database. As previously mentioned logical decoding works on a single database. A slot represents a sequence of changes in that database. There can be more than one slot per database. The slot manages a set of changes sent over a particular stream such as which transaction is currently being streamed and which transaction has been acknowledged.

A plugin is a library which accepts the changes and decodes the changes into a format of your choosing. Plugins need to be compiled and installed before they can be utilized by a slot. 

Creating a slot with JDBC:

```java
public void createLogicalReplicationSlot(String slotName, String outputPlugin ) throws InterruptedException, SQLException, TimeoutException
{
    //drop previous slot
    dropReplicationSlot(mgmntConnection, slotName);

    try (PreparedStatement preparedStatement =
                 mgmntConnection.prepareStatement("SELECT * FROM pg_create_logical_replication_slot(?, ?)") )
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
```

The function `pg_create_logical_replication_slot(<slot_name>, <plugin>)` 
returns slot_name and xlog_position

From the manual:
 
* Creates a new logical (decoding) replication slot named slot_name using the output plugin plugin. 
A call to this function has the same effect as the replication protocol command 
CREATE_REPLICATION_SLOT <slot_name> LOGICAL <output_plugin>

* *Note* in the above example we are dropping the replication slot if it exists, in production you 
would not do this. The code below first terminates any existing replication connection and then drops 
the slot


```java
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
```

### What have we done so far? 

*  We have created a replication slot
*  We know the current xlog location. In order to read the xlog location the driver provides a helper 
class LogicalSequenceNumber. It is public so you can use it easily.

```java
package org.postgresql.replication;
/**
 * LSN (Log Sequence Number) data which is a pointer to a location in the XLOG
 */
public final class LogSequenceNumber {
  /**
   * Zero is used indicate an invalid pointer. Bootstrap skips the first possible WAL segment,
   * initializing the first WAL page at XLOG_SEG_SIZE, so no XLOG record can begin at zero.
   */
  public static final LogSequenceNumber INVALID_LSN = LogSequenceNumber.valueOf(0);

  private final long value;

  private LogSequenceNumber(long value) {
    this.value = value;
  }

  /**
   * @param value numeric represent position in the write-ahead log stream
   * @return not null LSN instance
   */
  public static LogSequenceNumber valueOf(long value) {
    return new LogSequenceNumber(value);
  }

  /**
   * Create LSN instance by string represent LSN
   *
   * @param strValue not null string as two hexadecimal numbers of up to 8 digits each, separated by
   *                 a slash. For example {@code 16/3002D50}, {@code 0/15D68C50}
   * @return not null LSN instance where if specified string represent have not valid form {@link
   * LogSequenceNumber#INVALID_LSN}
   */
  public static LogSequenceNumber valueOf(String strValue) {
    int slashIndex = strValue.lastIndexOf('/');

    if (slashIndex <= 0) {
      return INVALID_LSN;
    }

    String logicalXLogStr = strValue.substring(0, slashIndex);
    int logicalXlog = (int) Long.parseLong(logicalXLogStr, 16);
    String segmentStr = strValue.substring(slashIndex + 1, strValue.length());
    int segment = (int) Long.parseLong(segmentStr, 16);

    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putInt(logicalXlog);
    buf.putInt(segment);
    buf.position(0);
    long value = buf.getLong();

    return LogSequenceNumber.valueOf(value);
  }

  /**
   * @return Long represent position in the write-ahead log stream
   */
  public long asLong() {
    return value;
  }

  /**
   * @return String represent position in the write-ahead log stream as two hexadecimal numbers of
   * up to 8 digits each, separated by a slash. For example {@code 16/3002D50}, {@code 0/15D68C50}
   */
  public String asString() {
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putLong(value);
    buf.position(0);

    int logicalXlog = buf.getInt();
    int segment = buf.getInt();
    return String.format("%X/%X", logicalXlog, segment);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LogSequenceNumber that = (LogSequenceNumber) o;

    return value == that.value;

  }

  @Override
  public int hashCode() {
    return (int) (value ^ (value >>> 32));
  }

  @Override
  public String toString() {
    return "LSN{" + asString() + '}';
  }
}
```

### An example how to use this class:

```java
private LogSequenceNumber getCurrentLSN() throws SQLException
    {
        try (Statement st = mgmntConnection.createStatement())
        {
            try (ResultSet rs = st.executeQuery("select "
                    + (((BaseConnection) mgmntConnection).haveMinimumServerVersion(ServerVersion.v10)
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
```

`LogSequenceNumber` knows how to parse the string returned by `pg_current_xlog_location()

### Now to actually read changes from a database

* We need to create a connection capable of replication.
* The replication protocol only understands the Simple Query protocol

```java
private void openReplicationConnection() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user","rep");
        properties.setProperty("password","test");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
        PGProperty.REPLICATION.set(properties, "database");
        PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
        replicationConnection = DriverManager.getConnection("jdbc:postgresql://localhost:15432/test",properties);
    }
```

* note we ensure that we have a backend version > 9.4 as logical decoding was not supported before 
that
* the user rep is a specific user which has the replication role. You must create this using 
` create user rep role with replication` additionally you must add this user to pg_hba.conf 
`host    replication     rep             0.0.0.0/0               md5`
* as mentioned above we must set PREFER_QUERY_MODE TO "simple"
* REPLICATION must be set to "database": this instructs the driver to connect to the database specified 
in the connection URL.



### We have a replication connection now what?

The following code can be used to read changes from the database

```java
public void receiveChangesOccursBeforStartReplication() throws Exception {
    PGConnection pgConnection = (PGConnection) replicationConnection;

    LogSequenceNumber lsn = getCurrentLSN();

    Statement st = dmlConnection.createStatement();
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
//                        .withSlotOption("pretty-print",true)
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
```

So lets break this down.

1) First get the current lsn *before* we modify the database
2) Modify some data
3) Get a replication stream. This uses a fluent style and has a number of steps
   1) ask the connection for the replicationAPI, and a stream
   2) ask for logical
   3) specify the slot that we want to read changes from
   4) specify where we want to start from
   5) Options for the decoder which are specific to the output plugin
   6) set the status update timeout interval to 20 seconds, this is how often we will
   send the status update message back to the server
4) read data from the stream. This uses a non-blocking call ```readPending```
5) do something with the data. In this case we simply display it.
6) Now tell the server that we have read the changes so that it is free to release the WAL buffers
7) This will automatically be sent to the server by the driver when we send the status update message

   
### Notes 
* withStartPosition can be left out; in which case replication would start from the 
current position
* the options for each output plugin are unique to that plugin, if using an existing plugin you will
 have to look at the source code for details for instance wal2json has the following 
 ```
 	data->include_xids = false;
 	data->include_timestamp = false;
 	data->include_schemas = true;
 	data->include_types = true;
 	data->pretty_print = false;
 	data->write_in_chunks = false;
 	data->include_lsn = false;

 ```
# Requirements
 
 As the postgres user 
 ```postgresql

 create user rep with replication;
 alter user rep password 'test';
 
 create user test with password 'test';
 create database test owner test;
```
As the user test
 ```postgresql
 create table test_logical_table(id serial, name text);
 ```
 
 postgresql.conf
 max_replication_slots > 0
 max_wal_senders > 0
 wal_level = logical
 
 
 host    replication     rep        0.0.0.0/0    md5
 