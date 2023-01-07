package org.gox.pg.broker.server.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gox.pg.broker.db.DataSource;
import org.gox.pg.broker.server.handler.ConsumerHandler;
import org.postgresql.PGNotification;
import org.postgresql.jdbc.PgConnection;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@Slf4j
public class EventListener implements Runnable {

    private PgConnection connection;

    @Override
    public void run() {
        try {
            executeListenNotificationQuery();
            while (true) {
                PGNotification[] notifications = connection.getNotifications(0);
                if (notifications != null) {
                    processNotification(notifications);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void processNotification(PGNotification[] notifications) {
        for (PGNotification notification : notifications) {
            logger.info("PG notification {} {}", notification.getName(), notification.getParameter());
            if (StringUtils.isNotEmpty(notification.getParameter())) {
                String[] params = notification.getParameter().split(" ");
                if (params.length > 1) {
                    ConsumerHandler.getInstance().notifyConsumers(params[1]);
                }
            }
        }
    }

    private void executeListenNotificationQuery() throws SQLException {
        connection = DataSource.getConnection().unwrap(PgConnection.class);
        String SQL_QUERY = "LISTEN event_notification";
        PreparedStatement pst = connection.prepareStatement(SQL_QUERY);
        pst.execute();
    }
}
