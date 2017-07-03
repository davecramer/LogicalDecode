/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.postgresintl.logicaldecoding;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class CountryLanguageDAO {
    public enum Columns {
        COUNTRY_CODE("countrycode"),
        LANGUAGE("language"),
        IS_OFFICIAL("isofficial"),
        PERCENTAGE("percentage");

        private final String columnName;

        private Columns(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnName() {
            return columnName;
        }
    }

    private static final String COUNTRY_LANGUAGES_QUERY = "SELECT " + Columns.COUNTRY_CODE.columnName + ", " + Columns.LANGUAGE.columnName + ", "
        + Columns.IS_OFFICIAL.columnName + ", " + Columns.PERCENTAGE.columnName
        + " FROM countrylanguage WHERE " + Columns.COUNTRY_CODE.columnName + " = ?"
        + " ORDER BY " + Columns.PERCENTAGE.columnName + " DESC";
    
    private static final String INSERT_STATEMENT = "INSERT INTO countrylanguage (countrycode,\"language\",isofficial,percentage) VALUES (?,?,?,?)";
    private static final String DELETE_STATEMENT = "DELETE FROM countrylanguage WHERE countrycode = ? AND \"language\" = ?";
    
    private static final String COPY_FROM_STATEMENT = "COPY countrylanguage FROM STDIN WITH CSV HEADER";
    
    private final Connection connection;

    public CountryLanguageDAO(Connection connection) {
        this.connection = connection;
    }

    private CountryLanguage getInstanceFromResultSet(ResultSet rs) throws SQLException {
        return new CountryLanguage(
                rs.getString(1), rs.getString(2), rs.getBoolean(3), rs.getDouble(4)
        );
    }

    public List<CountryLanguage> getCountryLanguages(String countryCode) throws SQLException {
        try(
            // Perform a query and extract some data. Statement is freed (closed) automagically too
            PreparedStatement statement = connection.prepareStatement(COUNTRY_LANGUAGES_QUERY)
        ) {
            // Set the parameters and execute the query
            statement.setString(1, countryCode);

            List<CountryLanguage> countryLanguages = new ArrayList<>();
            QueryExecutor.executeQuery(statement, (rs) -> {
                countryLanguages.add(getInstanceFromResultSet(rs));
            });

            return countryLanguages;
        }
    }
    
    public void insertCountriesLanguages(List<CountryLanguage> countryLanguages) throws SQLException, IOException {
      try(
          // Perform a batch of inserts. Statement is freed (closed) automagically too
          PreparedStatement statement = connection.prepareStatement(INSERT_STATEMENT)
      ) {
          try {
            countryLanguages.forEach(cl -> {
              try {
              // Set the parameters for an element of a batch
              statement.setString(1, cl.getCountryCode());
              statement.setString(2, cl.getLanguage());
              statement.setBoolean(3, cl.isOfficial());
              statement.setDouble(4, cl.getPercentage());
              statement.addBatch();
              } catch(SQLException e) {
                throw new RuntimeException(e);
              }
            });
          } catch(RuntimeException e) {
            if (e.getCause() instanceof SQLException) {
              throw (SQLException) e.getCause();
            }
            throw e;
          }

          statement.executeBatch();
      }
    }
    
    public void deleteCountriesLanguages(List<CountryLanguage> countryLanguages) throws SQLException, IOException {
      try(
          // Perform a batch of deletes. Statement is freed (closed) automagically too
          PreparedStatement statement = connection.prepareStatement(DELETE_STATEMENT)
      ) {
          try {
            countryLanguages.forEach(cl -> {
              try {
              // Set the parameters for an element of a batch
              statement.setString(1, cl.getCountryCode());
              statement.setString(2, cl.getLanguage());
              statement.addBatch();
              } catch(SQLException e) {
                throw new RuntimeException(e);
              }
            });
          } catch(RuntimeException e) {
            if (e.getCause() instanceof SQLException) {
              throw (SQLException) e.getCause();
            }
            throw e;
          }

          statement.executeBatch();
      }
    }

    public void copyCountriesLanguagesFrom(List<CountryLanguage> countryLanguages) throws SQLException, IOException {
        PGConnection connection = this.connection.unwrap(PGConnection.class);
        CopyManager copyManager = connection.getCopyAPI();
        
        try(
            // Prepare copy statement and set parameter using JDBC. Statement is freed (closed) automagically too
            PreparedStatement statement = this.connection.prepareStatement(COPY_FROM_STATEMENT);
        ) {
          // Retrieve statement with parameter substitution using toString
          // ...err, ok it is a bit hackish, but it is simple and use the driver code to do the job
          copyManager.copyIn(statement.toString(), new ByteArrayInputStream((
              "countrycode,language,isofficial,percentage\n"
              + countryLanguages.stream()
                .map(cl -> '"' + cl.getCountryCode() + '"' + "," + '"' + cl.getLanguage() + '"' + "," + cl.isOfficial() + "," + cl.getPercentage())
                .collect(Collectors.joining("\n"))).getBytes()));
        }
    }
}
