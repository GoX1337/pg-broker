package org.gox.pg.broker.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.gox.pg.broker.client.consumer.ConsumerClient;
import org.gox.pg.broker.client.producer.ProducerClient;
import org.gox.pg.broker.model.Event;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class Tester {

    private final static String HOSTNAME = "127.0.0.1";
    private final static int PORT = 1337;

    public static void main(String[] args) throws IOException {
        String topic = "test";
        List<ConsumerClient> consumers = new ArrayList<>();

        for(int i = 0; i < 10000; i++) {
            ConsumerClient consumer = ConsumerClient.builder()
                    .hostname(HOSTNAME)
                    .port(PORT)
                    .build()
                    .connect();
            consumers.add(consumer);
        }

        logger.info("{} consumers created", consumers.size());

        for(ConsumerClient consumer : consumers) {
            Thread.startVirtualThread(() -> {
                try {
                    consumer.subscribe(topic, eventEntity -> {
                        logger.info("Received new event for topic '{}' : {}", topic, eventEntity);
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }


        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        while(true) {
            ProducerClient producer = ProducerClient.builder()
                    .hostname(HOSTNAME)
                    .port(PORT)
                    .build()
                    .connect();
            Event event = new Event(topic, OffsetDateTime.now(), "Hello " + Math.random());
            producer.sendEvent(objectMapper.writeValueAsString(event));
        }
    }
}
