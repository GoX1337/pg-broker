package org.gox.pg.broker.client.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gox.pg.broker.model.EventEntity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.function.Consumer;

@Slf4j
public class ConsumerClient {

    private String hostname;
    private int port;

    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;
    private ObjectMapper objectMapper;

    @Builder
    private ConsumerClient(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    public ConsumerClient connect() throws IOException {
        clientSocket = new Socket(hostname, port);
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        out.println("1");
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            if ("ACKCONNECT".equals(inputLine)) {
                logger.info("Consumer connected");
                break;
            }
        }
        return this;
    }

    public static void main(String[] args) throws IOException {
        if(args.length < 3){
            throw new IllegalArgumentException("Not enough parameters (usage : consumer <host> <port> <topic>)");
        }
        String topic = args[2];

        ConsumerClient.builder()
                .hostname(args[0])
                .port(Integer.parseInt(args[1]))
                .build()
                .connect()
                .subscribe(topic, eventEntity -> {
                    logger.info("Received new event for topic '{}' : {}", topic, eventEntity);
                });
    }

    private void subscribe(String topic, Consumer<EventEntity> eventHandler) throws IOException {
        out.println("TOPIC " + topic);
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            if ("ACK".equals(inputLine)) {
                logger.info("Consumer subscribed to topic {}", topic);
            } else {
                EventEntity event = objectMapper.readValue(inputLine, EventEntity.class);
                eventHandler.accept(event);
            }
        }
    }
}
