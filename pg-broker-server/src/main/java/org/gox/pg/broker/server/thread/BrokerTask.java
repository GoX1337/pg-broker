package org.gox.pg.broker.server.thread;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gox.pg.broker.model.Command;
import org.gox.pg.broker.server.PgBroker;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.UUID;

@AllArgsConstructor
@Slf4j
public abstract class BrokerTask implements Runnable {

    private final PgBroker pgBroker;
    private final UUID uuid;
    private final Socket socket;
    private final BufferedReader in;
    private final PrintWriter out;

    @Override
    public void run() {
        try {
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                logger.info("Received event {}", inputLine);
                handleRequest(inputLine);
                out.println(Command.ACK);
            }
            logger.info("Client disconnected");
            socket.close();
            pgBroker.notifyClientDisconnected(uuid);
        } catch (Exception e) {
            logger.info("Error with a client connection", e);
        }
    }

    public abstract void handleRequest(String payload);

    public BufferedReader getIn() {
        return in;
    }

    public PrintWriter getOut() {
        return out;
    }
}
