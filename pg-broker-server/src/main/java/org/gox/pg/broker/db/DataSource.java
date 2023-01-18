package org.gox.pg.broker.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

@Slf4j
public class DataSource {

    private static DataSource instance;
    private static HikariDataSource dataSource;
    private static final String DATASOURCE_PROPERTIES = "datasource.properties";

    private DataSource() {
        dataSource = new HikariDataSource(new HikariConfig(getDatasourceProperties()));
    }

    private Properties getDatasourceProperties() {
        Properties properties = new Properties();
        try(InputStream in = getClass().getClassLoader().getResourceAsStream(DATASOURCE_PROPERTIES)) {
            properties.load(in);
        } catch (IOException e) {
            logger.error("Fail to load datasource.properties for HikariCP");
            throw new RuntimeException(e);
        }
        return properties;
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