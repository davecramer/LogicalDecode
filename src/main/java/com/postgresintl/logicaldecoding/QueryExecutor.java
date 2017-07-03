/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.postgresintl.logicaldecoding;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created: 20/05/14
 *
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class QueryExecutor {
    public static void executeQuery(PreparedStatement preparedStatement, QueryProcessor queryProcessor)
    throws SQLException {
        try(ResultSet rs = preparedStatement.executeQuery()) {
            while(rs.next()) {
                queryProcessor.process(rs);
            }
        }
    }
}
