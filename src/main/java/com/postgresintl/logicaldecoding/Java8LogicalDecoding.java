package com.postgresintl.logicaldecoding;

import org.postgresql.replication.LogSequenceNumber;


import com.postgresintl.logicaldecoding.config.PropertiesFileDbConfig;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Matteo Melli <matteom@8kdata.com>
 *
 * You should enable logical decoding and allow access to replication connection for user worlduser.
 *
 * Set following configuration in your postgresql.conf:
 *
 * max_wal_senders = 4             # max number of walsender processes
 * wal_keep_segments = 4           # in logfile segments, 16MB each; 0 disables
 * wal_level = logical             # minimal, replica, or logical
 * max_replication_slots = 4       # max number of replication slots
 *
 * Add following lines to your pg_hba.conf:
 *
 * host    replication   worlduser   127.0.0.1/32    md5
 * host    replication   worlduser   ::1/128         md5
 *
 */
public class Java8LogicalDecoding {
  public static void main(String[] args) {
    PropertiesFileDbConfig config;
    try {
      config = new PropertiesFileDbConfig();
    } catch (IOException e) {
      throw new RuntimeException("Error opening or reading the properties config file");
    }

    try(
        // Create 2 connections to the database for manage a slot and to read changes. Slot and connections are closed automagically

        DBManager dbManager = new DBManager(properties -> {
          Properties wrappedProperties = new Properties();
          if (properties.isPresent()) {
            wrappedProperties.putAll(properties.get());
          }
          wrappedProperties.setProperty("user", config.getDbUser());
          wrappedProperties.setProperty("password", config.getDbPassword());
          return DriverManager.getConnection(
              config.getPostgresJdbcUrl(), wrappedProperties
          );
        });
        // Create 2 connections to the database for manage a slot and to read changes. Slot and connections are closed automagically
        LogicalDecodingManager logicalDecodingManager = new LogicalDecodingManager("worldlogical", "test_decoding", properties -> {
          Properties wrappedProperties = new Properties();
          if (properties.isPresent()) {
            wrappedProperties.putAll(properties.get());
          }
          wrappedProperties.setProperty("user", config.getRepUser());
          wrappedProperties.setProperty("password", config.getDbPassword());
          return DriverManager.getConnection(
              config.getPostgresJdbcUrl(), wrappedProperties
          );
        }, dbManager.getConnection());
    ) {
      LogSequenceNumber lsn;

      // Create a list of languages
      List<CountryLanguage> countryLanguages = Stream
          .of("Napolitan", "Sicilian", "Emilian-Romagnol")
          .map(language -> new CountryLanguage("ITA", language, false, 0.02))
          .collect(Collectors.toList());

      CountryLanguageDAO countryLanguageDAO = new CountryLanguageDAO(dbManager.getConnection());

      // Read current LSN
      lsn = logicalDecodingManager.getCurrentLSN();
      countryLanguageDAO.deleteCountriesLanguages(countryLanguages);
      countryLanguageDAO.insertCountriesLanguages(countryLanguages);
      logicalDecodingManager.receiveNextChangesTo(lsn, System.out);
      System.out.println();

      // Read current LSN
      lsn = logicalDecodingManager.getCurrentLSN();
      countryLanguageDAO.deleteCountriesLanguages(countryLanguages);
      logicalDecodingManager.receiveNextChangesTo(lsn, System.out);
      System.out.println();
    } catch (SQLException | IOException | InterruptedException | TimeoutException e) {
      System.err.println("Error connecting to, creating slot or reading changes or disconnecting from the database");
      e.printStackTrace();
    }
  }
}
