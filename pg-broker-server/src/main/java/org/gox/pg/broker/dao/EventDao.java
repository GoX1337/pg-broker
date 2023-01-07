package org.gox.pg.broker.dao;

import lombok.extern.slf4j.Slf4j;
import org.gox.pg.broker.db.DataSource;
import org.gox.pg.broker.model.EventEntity;
import org.gox.pg.broker.model.Status;

import java.sql.*;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
public class EventDao {

    private static EventDao instance;

    private static final String CREATE_TABLE_SQL_QUERY = """
            CREATE TABLE IF NOT EXISTS events (
               id VARCHAR (50) UNIQUE NOT NULL,
               timestamp TIMESTAMP NOT NULL,
               topic VARCHAR (50) NOT NULL,
               status VARCHAR (20) NOT NULL,
               payload TEXT NOT NULL
            )""";

    private static final String CREATE_FUNCTION_SQL_QUERY = """
            CREATE OR REPLACE FUNCTION EVENT_NOTIFY_INSERT_FCT()
                RETURNS TRIGGER AS
                $BODY$
                BEGIN
                EXECUTE 'NOTIFY event_notification, ''WORK ' || NEW.TOPIC || '''';
                RETURN NEW;
                END;
                $BODY$
                LANGUAGE PLPGSQL;
            """;

    private static final String CREATE_TRIGGER_SQL_QUERY = """
            CREATE OR REPLACE TRIGGER EVENT_NOTIFY_INSERT_TRG
                AFTER INSERT ON events
                FOR EACH ROW
                EXECUTE PROCEDURE EVENT_NOTIFY_INSERT_FCT();
            """;

    private EventDao() {
    }

    public static EventDao getInstance() {
        if(instance == null) {
            instance = new EventDao();
        }
        return instance;
    }

    public void init() throws SQLException {
        executeInitQuery(CREATE_TABLE_SQL_QUERY);
        executeInitQuery(CREATE_FUNCTION_SQL_QUERY);
        executeInitQuery(CREATE_TRIGGER_SQL_QUERY);
    }

    public void executeInitQuery(String query) throws SQLException {
        try (Connection con = DataSource.getInstance().getConnection();
             PreparedStatement pst = con.prepareStatement(query)) {
            pst.execute();
        }
    }

    public void saveEvent(EventEntity event) throws SQLException {
        String SQL_QUERY = "INSERT INTO events (id, timestamp, topic, status, payload) VALUES (?, ?, ?, ?, ?)";

        try (Connection con = DataSource.getInstance().getConnection();
             PreparedStatement pst = con.prepareStatement(SQL_QUERY)) {
            pst.setString(1, event.id().toString());
            pst.setTimestamp(2, Timestamp.valueOf(event.timestamp().atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime()));
            pst.setString(3, event.topic());
            pst.setString(4, Status.PENDING.toString());
            pst.setString(5, event.payload());
            pst.execute();
        }
    }

    public void updateEvents(List<EventEntity> eventEntities, Status status) throws SQLException {
        Object[] listIds = eventEntities.stream()
                .map(EventEntity::id)
                .map(UUID::toString)
                .toArray();
        String SQL_QUERY = "UPDATE events set status = ? where id = ANY (?)";
        Connection con = DataSource.getInstance().getConnection(false);
        try (PreparedStatement pst = con.prepareStatement(SQL_QUERY)) {
            pst.setString(1, status.toString());
            pst.setArray(2, con.createArrayOf("VARCHAR", listIds));
            pst.execute();
        }
        con.commit();
        con.close();
    }

    public List<EventEntity> findAllPendingForTopic(String topic) throws SQLException {
        String SQL_QUERY = "select * from events e where e.topic = ? and e.status = ? for update skip locked";
        List<EventEntity> events = new ArrayList<>();

        try (Connection con = DataSource.getInstance().getConnection(false);
             PreparedStatement pst = con.prepareStatement(SQL_QUERY)) {
            pst.setString(1, topic);
            pst.setString(2, Status.PENDING.name());
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                events.add(buildEventEntity(rs));
            }
            rs.close();
        }
        return events;
    }

    private static EventEntity buildEventEntity(ResultSet rs) throws SQLException {
        UUID id = UUID.fromString(rs.getString(1));
        OffsetDateTime timestamp = OffsetDateTime.ofInstant(rs.getTimestamp(2).toInstant(), ZoneOffset.UTC);
        String topic = rs.getString(3);
        Status status = Status.valueOf(rs.getString(4));
        String payload = rs.getString(5);
        return new EventEntity(topic, id, timestamp, status, payload);
    }
}
