/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.postgresintl.logicaldecoding;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class CountriesLanguageDAO {
    public enum Columns {
        COUNTRIES("countries"),
        LANGUAGE("language"),
        AVG_PERCENTAGE("avg_percentage");

        private final String columnName;

        private Columns(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnName() {
            return columnName;
        }
    }

    private static final String COUNTRIES_LANGUAGES_QUERY = "SELECT array_agg(countrycode) AS " + Columns.COUNTRIES.columnName
            + ", " + Columns.LANGUAGE.columnName + ", to_char(avg(percentage), '999.00') AS "
            + Columns.AVG_PERCENTAGE.columnName
            + " FROM countrylanguage WHERE isofficial GROUP BY language HAVING avg(percentage) > ?"
            + " ORDER BY avg(percentage) DESC";

    private static final String COPY_TO_STATEMENT = "COPY (" + COUNTRIES_LANGUAGES_QUERY + ") TO STDOUT WITH CSV HEADER";
    
    private final Connection connection;

    public CountriesLanguageDAO(Connection connection) {
        this.connection = connection;
    }

    private CountriesLanguage getInstanceFromResultSet(ResultSet rs) throws SQLException {
        return new CountriesLanguage(
                (String[]) rs.getArray(1).getArray(), rs.getString(2), rs.getDouble(3)
        );
    }

    public List<CountriesLanguage> getCountriesLanguages(int percentage) throws SQLException {
        try(
            // Perform a query and extract some data. Statement is freed (closed) automagically too
            PreparedStatement statement = connection.prepareStatement(COUNTRIES_LANGUAGES_QUERY)
        ) {
            // Set the parameters and execute the query
            statement.setInt(1, percentage);

            List<CountriesLanguage> countriesLanguages = new ArrayList<>();
            QueryExecutor.executeQuery(statement, (rs) -> {
                countriesLanguages.add(getInstanceFromResultSet(rs));
            });

            return countriesLanguages;
        }
    }

    public void copyCountriesLanguagesTo(int percentage, OutputStream to) throws SQLException, IOException {
        PGConnection connection = this.connection.unwrap(PGConnection.class);
        CopyManager copyManager = connection.getCopyAPI();
        
        try(
            // Prepare copy statement and set parameter using JDBC. Statement is freed (closed) automagically too
            PreparedStatement statement = this.connection.prepareStatement(COPY_TO_STATEMENT);
        ) {
          // Set the parameters and execute the query
          statement.setInt(1, percentage);
        
          // Retrieve statement with parameter substitution using toString
          // ...err, ok it is a bit hackish, but it is simple and use the driver code to do the job
          copyManager.copyOut(statement.toString(), to);
        }
    }

}
