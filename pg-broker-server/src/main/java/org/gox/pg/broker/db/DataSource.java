package org.gox.pg.broker.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public class DataSource {

    private static DataSource instance;
    private static HikariDataSource dataSource;

    private DataSource() {
        URL fileUrl = DataSource.class.getResource("/datasource.properties");
        if(fileUrl != null) {
            dataSource = new HikariDataSource(new HikariConfig(fileUrl.getPath()));
        } else {
            logger.error("Fail to load datasource.properties for HikariCP");
        }
    }

    public Connection getConnection(boolean autocommit) throws SQLException {
        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(autocommit);
        return connection;
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static DataSource getInstance() {
        if(instance == null) {
            instance = new DataSource();
        }
        return instance;
    }
}