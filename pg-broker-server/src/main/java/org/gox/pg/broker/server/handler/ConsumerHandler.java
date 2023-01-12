package org.gox.pg.broker.server.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.gox.pg.broker.dao.EventDao;
import org.gox.pg.broker.model.EventEntity;
import org.gox.pg.broker.model.Status;
import org.gox.pg.broker.server.thread.ConsumerTask;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

@Slf4j
public class ConsumerHandler {

    private static ConsumerHandler instance;

    private final ObjectMapper objectMapper;
    private final Map<String, List<PrintWriter>> consumers = new HashMap<>();

    private ConsumerHandler() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    public static ConsumerHandler getInstance() {
        if(instance == null) {
            instance = new ConsumerHandler();
        }
        return instance;
    }

    public void registerConsumer(String topic, ConsumerTask consumerTask) {
        List<PrintWriter> consumerWriters = consumers.computeIfAbsent(topic, k -> new ArrayList<>());
        consumerWriters.add(consumerTask.getOut());
        logger.info("New client subscribed to topic {}", topic);
    }

    public void notifyConsumers(String topic) {
        try {
            final List<EventEntity> events = EventDao.getInstance().findAllPendingForTopic(topic);
            List<PrintWriter> consumersDestinations = Optional.ofNullable(consumers.get(topic)).orElse(Collections.emptyList());
            consumersDestinations.forEach(sendNotifications(events));
            logger.info("Sent {} event to {} consumers subscribed to {}", events.size(), consumersDestinations.size(), topic);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Consumer<PrintWriter> sendNotifications(List<EventEntity> events) {
        return printWriter -> {
            events.stream()
                    .map(eventEntity -> Thread.startVirtualThread(() -> sendNotification(printWriter, eventEntity)))
                    .forEach(this::waitThreadEnd);
            updateEventStatues(events);
        };
    }

    private static void updateEventStatues(List<EventEntity> events) {
        try {
            EventDao.getInstance().updateEvents(events, Status.CONSUMED);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void waitThreadEnd(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendNotification(PrintWriter consumerWriter, EventEntity eventEntity) {
        try {
            consumerWriter.println(objectMapper.writeValueAsString(eventEntity));
        } catch (Exception e) {
            logger.error("Fail to send a notification", e);
        }
    }
}
