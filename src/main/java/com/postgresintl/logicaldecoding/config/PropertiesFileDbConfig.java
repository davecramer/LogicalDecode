/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.postgresintl.logicaldecoding.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class PropertiesFileDbConfig {
    private static final String DEFAULT_PROPERTIES_FILE = "/db.properties";

    private static final String JDBC_POSTGRES_SUBPROTOCOL = "postgresql";
    private static final String JDBC_PROTOCOL = "jdbc";

    private final String dbHost;
    private final int dbPort;
    private final String dbName;
    private final String dbUser;
    private final String repUser;
    private final String dbPassword;

    public PropertiesFileDbConfig() throws IOException {
        this(DEFAULT_PROPERTIES_FILE);
    }

    public PropertiesFileDbConfig(String propertiesFile) throws IOException {
        Properties properties = new Properties();

        try(InputStream is = getClass().getResourceAsStream(propertiesFile)) {
            properties.load(is);
        }

        this.dbHost = properties.getProperty("dbHost");
        try {
            this.dbPort = Integer.parseInt(properties.getProperty("dbPort"));
            // More strict validation may be done here, such as 1 <= port <= 2^16 - 1
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "dbPort '" + properties.getProperty("dbPort")
                            + "' is not a valid port number. Should be an integer value"
            );
        }
        this.dbName = properties.getProperty("dbName");
        this.dbUser = properties.getProperty("dbUser");
        this.repUser = properties.getProperty("repUser");
        this.dbPassword = properties.getProperty("dbPassword");
        if (null == this.dbHost || null == this.dbName || null == this.dbUser || null == this.dbPassword) {
            throw new IllegalArgumentException("dbHost, dbName, dbUser and dbPassword must be all set in the properties file");
        }
    }

    public String getDbHost() {
        return dbHost;
    }

    public int getDbPort() {
        return dbPort;
    }

    public String getDatabase() {
        return dbName;
    }

    public String getDbUser() {
        return dbUser;
    }

    public String getRepUser() { return repUser;}
    public String getDbPassword() {
        return dbPassword;
    }

    public String getPostgresJdbcUrl() {
        return JDBC_PROTOCOL + ":" + JDBC_POSTGRES_SUBPROTOCOL + "://" + getDbHost()
                + ":" + getDbPort() + "/" + getDatabase();
    }
}
