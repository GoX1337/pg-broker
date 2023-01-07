package org.gox.pg.broker.server.thread;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gox.pg.broker.model.Command;
import org.gox.pg.broker.server.PgBroker;
import org.gox.pg.broker.server.handler.ConsumerHandler;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.UUID;

@Slf4j
public class ConsumerTask extends BrokerTask {

    public ConsumerTask(PgBroker pgBroker, UUID uuid, Socket socket, BufferedReader in, PrintWriter out) {
        super(pgBroker, uuid, socket, in, out);
    }

    @Override
    public void handleRequest(String payload) {
        if(StringUtils.isNotEmpty(payload)) {
            String[] commandArgs = payload.trim().split(" ");
            if(commandArgs.length > 1 && Command.TOPIC.name().equals(commandArgs[0])) {
                String topic = commandArgs[1].toLowerCase();
                ConsumerHandler.getInstance().registerConsumer(topic, this);
            }
        }
    }
}
