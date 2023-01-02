package org.gox.pg.broker.server.thread;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.gox.pg.broker.dao.EventDao;
import org.gox.pg.broker.mapper.EventMapper;
import org.gox.pg.broker.model.Event;
import org.gox.pg.broker.server.PgBroker;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.SQLException;
import java.util.UUID;

@Slf4j
public class ProducerTask extends BrokerTask {

    private final ObjectMapper objectMapper;
    private final EventDao eventDao;

    public ProducerTask(PgBroker pgBroker, UUID uuid, Socket socket, BufferedReader in, PrintWriter out) {
        super(pgBroker, uuid, socket, in, out);
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        eventDao = EventDao.getInstance();
    }

    @Override
    public void handleRequest(String payload) {
        try {
            Event event = objectMapper.readValue(payload, Event.class);
            eventDao.saveEvent(EventMapper.map(event));
        } catch (JsonProcessingException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
