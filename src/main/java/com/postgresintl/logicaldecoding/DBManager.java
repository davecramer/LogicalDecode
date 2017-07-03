package com.postgresintl.logicaldecoding;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by davec on 2017-06-29.
 */
public class DBManager implements AutoCloseable {

  private final Connection connection;

  public DBManager(ConnectionProvider connectionProvider) throws SQLException
  {
    this.connection = connectionProvider.getConnection();
  }

  public Connection getConnection() {return connection;}

  @Override
  public void close() throws SQLException {
    this.connection.close();
  }
}
