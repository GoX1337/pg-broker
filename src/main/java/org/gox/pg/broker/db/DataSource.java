package org.gox.pg.broker.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public class DataSource {

    private static HikariDataSource dataSource;

    public static Connection getConnection(boolean autocommit) throws SQLException {
        if(dataSource == null) {
            initDataSource();
        }
        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(autocommit);
        return connection;
    }

    public static Connection getConnection() throws SQLException {
        if(dataSource == null) {
            initDataSource();
        }
        return dataSource.getConnection();
    }

    private static void initDataSource() {
        URL fileUrl = DataSource.class.getResource("/datasource.properties");
        if(fileUrl != null) {
            dataSource = new HikariDataSource(new HikariConfig(fileUrl.getPath()));
        } else {
            logger.error("Fail to load datasource.properties for HikariCP");
        }
    }
}