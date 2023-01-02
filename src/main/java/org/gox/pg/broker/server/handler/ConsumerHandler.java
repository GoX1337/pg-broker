package org.gox.pg.broker.server.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gox.pg.broker.dao.EventDao;
import org.gox.pg.broker.model.EventEntity;
import org.gox.pg.broker.model.Status;
import org.gox.pg.broker.server.thread.ConsumerTask;

import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

@Slf4j
public class ConsumerHandler {

    private static ConsumerHandler instance;

    private final ObjectMapper objectMapper;
    private final Map<String, List<ConsumerTask>> consumers = new HashMap<>();

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
        List<ConsumerTask> consumerTasks = consumers.computeIfAbsent(topic, k -> new ArrayList<>());
        consumerTasks.add(consumerTask);
        logger.info("New client subscribed to topic {}", topic);
    }

    public void notifyConsumers(String topic) {
        try {
            final List<EventEntity> events = EventDao.getInstance().findAllPendingForTopic(topic);
            Optional.ofNullable(consumers.get(topic))
                    .orElse(Collections.emptyList())
                    .forEach(sendNotifications(events));

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Consumer<ConsumerTask> sendNotifications(List<EventEntity> events) {
        return consumerTask -> Thread.startVirtualThread(() -> events.forEach(eventEntity -> {
            sendNotification(consumerTask, eventEntity);
            try {
                EventDao.getInstance().updateEvents(events, Status.CONSUMED);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    private void sendNotification(ConsumerTask consumerTask, EventEntity eventEntity) {
        try {
            consumerTask.getOut().println(objectMapper.writeValueAsString(eventEntity));
        } catch (Exception e) {
            logger.error("Fail to send a notification", e);
        }
    }
}
