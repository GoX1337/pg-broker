package org.gox.pg.broker.client.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

@Slf4j
public class ProducerClient {

    private String hostname;
    private int port;

    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;
    private ObjectMapper objectMapper;

    @Builder
    private ProducerClient(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    public ProducerClient connect() throws IOException {
        clientSocket = new Socket(hostname, port);
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        out.println("2");
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            if ("ACKCONNECT".equals(inputLine)) {
                logger.info("Consumer connected");
                break;
            }
        }
        return this;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if(args.length < 2){
            throw new IllegalArgumentException("Not enough parameters (usage : consumer <host> <port>)");
        }

        ProducerClient producer = ProducerClient.builder()
                .hostname(args[0])
                .port(Integer.parseInt(args[1]))
                .build()
                .connect();

        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String eventStr = scanner.nextLine();
            producer.sendEvent(eventStr);
        }
    }

    private void sendEvent(String eventStr) throws IOException {
        out.println(eventStr);
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            if ("ACK".equals(inputLine)) {
                logger.info("Event sent");
                break;
            }
        }
    }
}
