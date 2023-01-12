package org.gox.pg.broker.server;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gox.pg.broker.dao.EventDao;
import org.gox.pg.broker.exception.PgBrokerRuntimeException;
import org.gox.pg.broker.model.ClientType;
import org.gox.pg.broker.model.Command;
import org.gox.pg.broker.server.listener.EventListener;
import org.gox.pg.broker.server.thread.ConsumerTask;
import org.gox.pg.broker.server.thread.ProducerTask;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public class PgBroker {

    private final ServerSocket serverSocket;
    private final int port;
    private int nbConsumers = 0;
    private int nbProducers = 0;
    private final Map<UUID, Thread> threadsMap = new HashMap<>();
    private final Map<UUID, ClientType> threadsTypeMap = new HashMap<>();

    public PgBroker(int port) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.port = port;
    }

    public void start() throws IOException {
        Thread.startVirtualThread(buildMaintenanceTask());
        Thread.startVirtualThread(new EventListener());
        initEventDao();
        logger.info("Server started on port {}, waiting for new connection...", port);
        while (true) {
            Socket clientSocket = serverSocket.accept();
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            Thread.startVirtualThread(connexionRequestHandler(clientSocket, out, in));
        }
    }

    private static void initEventDao() {
        try {
            EventDao.getInstance().init();
        } catch (SQLException e) {
            String message = "Fail to init event DAO";
            logger.error(message);
            throw new PgBrokerRuntimeException(message);
        }
    }

    private Runnable buildMaintenanceTask() {
        return () -> {
            int producerCount = nbProducers;
            int consumerCount = nbConsumers;
            while (true) {
                if(nbProducers != producerCount || nbConsumers != consumerCount) {
                    logger.info("{} producers connected, {} consumers connected", nbProducers, nbConsumers);
                    producerCount = nbProducers;
                    consumerCount = nbConsumers;
                }
                sleep();
            }
        };
    }

    private static void sleep() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Runnable connexionRequestHandler(Socket clientSocket, PrintWriter out, BufferedReader in) {
        return () -> {
            ClientType clientType = getClientType(in, out);
            if (clientType == null) {
                logger.error("Client disconnected");
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return;
            }
            switch (clientType) {
                case CONSUMER -> createConsumerThread(clientSocket, in, out);
                case PRODUCER -> createProducerThread(clientSocket, in, out);
            }
        };
    }

    public ClientType getClientType(BufferedReader in, PrintWriter out) {
        try {
            String inputLine;
            int nbTry = 0;
            while ((inputLine = in.readLine()) != null) {
                if(StringUtils.isNotBlank(inputLine) && StringUtils.isNumeric(inputLine)) {
                    Optional<ClientType> clientTypeOpt = ClientType.valueOf(Integer.parseInt(inputLine));
                    if(clientTypeOpt.isPresent()) {
                        out.println(Command.ACKCONNECT);
                        return clientTypeOpt.get();
                    }
                }
                if (++nbTry == 3) {
                    break;
                }
            }
        } catch (Exception e){
            logger.error("Error with client connection : {}", e.getMessage(), e);
        }
        return null;
    }

    private void createConsumerThread(Socket clientSocket, BufferedReader in, PrintWriter out) {
        UUID uuid = UUID.randomUUID();
        Thread thread = Thread.startVirtualThread(new ConsumerTask(this, uuid, clientSocket, in, out));
        nbConsumers++;
        threadsMap.put(uuid, thread);
        threadsTypeMap.put(uuid, ClientType.CONSUMER);
        logger.info("New consumer listener created and started");
    }

    private void createProducerThread(Socket clientSocket, BufferedReader in, PrintWriter out) {
        UUID uuid = UUID.randomUUID();
        Thread thread = Thread.startVirtualThread(new ProducerTask(this, uuid, clientSocket, in, out));
        nbProducers++;
        threadsMap.put(uuid, thread);
        threadsTypeMap.put(uuid, ClientType.PRODUCER);
        logger.info("New producer listener created and started");
    }

    public void notifyClientDisconnected(UUID uuid) {
        Thread thread = threadsMap.get(uuid);
        if (thread != null) {
            threadsMap.remove(uuid);
            switch (threadsTypeMap.get(uuid)) {
                case CONSUMER -> nbConsumers--;
                case PRODUCER -> nbProducers--;
            }
            threadsTypeMap.remove(uuid);
        }
    }
}