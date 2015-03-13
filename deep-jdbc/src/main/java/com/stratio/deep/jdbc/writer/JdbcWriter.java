/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.jdbc.writer;

import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.sql.*;
import java.util.*;

/**
 * Creates a new JDBC connection and provides methods for writing.
 */
public class JdbcWriter<T> implements IJdbcWriter {

    /**
     * JDBC Deep Job configuration.
     */
    private JdbcDeepJobConfig<T> jdbcDeepJobConfig;

    /**
     * The log.
     */
    private  static final Logger LOG = LoggerFactory.getLogger(JdbcWriter.class);

    /**
     * JDBC connection.
     */
    protected Connection conn;

    /**
     * Default constructor
     */
    protected JdbcWriter() {

    }

    /**
     * Instantiates a new JdbcWriter.
     * @param jdbcDeepJobConfig Deep Job configuration.
     * @throws Exception
     */
    public JdbcWriter(JdbcDeepJobConfig jdbcDeepJobConfig) {

        try {
            this.jdbcDeepJobConfig = jdbcDeepJobConfig;
            Class.forName(jdbcDeepJobConfig.getDriverClass());
            this.conn = DriverManager.getConnection(jdbcDeepJobConfig.getConnectionUrl(),
                    jdbcDeepJobConfig.getUsername(),
                    jdbcDeepJobConfig.getPassword());
        } catch (ClassNotFoundException | SQLException e) {
            String message = "A exceptions happens while we create a JdbcWriter."+e.toString();
            LOG.error(message);
            throw new DeepGenericException(message,e);

        }
    }

    /**
     * Saves data.
     * @param row Data structure representing a row as a Map of column_name:column_value
     * @throws SQLException
     */
    public void save(Map<String, Object> row)  {

        try {
            Tuple2<List<String>, String> data = sqlFromRow(row);
            PreparedStatement statement = conn.prepareStatement(data._2());;
            int i = 1;
            for(String columnName:data._1()) {
                statement.setObject(i, row.get(columnName));
                i++;
            }
            statement.executeUpdate();
        } catch (SQLException e) {
            String message = "A SQLException happens while we are saving a row in JDBCWritter."+e.getMessage();
            LOG.error(message);
            throw new DeepGenericException(message,e);
        }
    }

    /**
     * Closes the JDBC Connection.
     * @throws SQLException
     */
    public void close() throws SQLException {
        conn.close();
    }

    private Tuple2<List<String>, String> sqlFromRow(Map<String, Object> row) {

        List<String> params = new ArrayList<>();
        for(int i=0; i<row.size(); i++) {
            params.add("?");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        if(jdbcDeepJobConfig.getQuoteSql()) {
            sb.append("\"");
        }
        sb.append(jdbcDeepJobConfig.getDatabase());
        if(jdbcDeepJobConfig.getQuoteSql()) {
            sb.append("\"");
        }
        sb.append(".");
        if(jdbcDeepJobConfig.getQuoteSql()) {
            sb.append("\"");
        }
        sb.append(jdbcDeepJobConfig.getTable());
        if(jdbcDeepJobConfig.getQuoteSql()) {
            sb.append("\"");
        }
        sb.append("(");
        List<String> columns = new ArrayList<>(row.keySet());
        List<String> quotedColumns = new ArrayList<>();
        for(String column:columns) {
            if(jdbcDeepJobConfig.getQuoteSql()) {
                quotedColumns.add(String.format("\"%s\"", column));
            } else {
                quotedColumns.add(String.format("%s", column));
            }
        }
        sb.append(StringUtils.join(quotedColumns, ","));
        sb.append(" ) VALUES (");
        sb.append(StringUtils.join(params, ","));
        sb.append(")");
        Tuple2<List<String>, String> result = new Tuple2<>(columns, sb.toString());
        return result;
    }



}
