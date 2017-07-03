package com.postgresintl.logicaldecoding;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

/**
 * Created by davec on 2017-06-29.
 */

@FunctionalInterface
public interface ConnectionProvider {
  public Connection getConnection(Optional<Properties> properties) throws SQLException;

  default public Connection getConnection() throws SQLException {
    return getConnection(Optional.empty());
  }

  default public Connection getConnection(Properties properties) throws SQLException {
    return getConnection(Optional.of(properties));
  }
}
