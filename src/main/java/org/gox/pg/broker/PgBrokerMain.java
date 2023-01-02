package org.gox.pg.broker;

import org.gox.pg.broker.server.PgBroker;

import java.io.IOException;

public class PgBrokerMain {

    public static void main(String[] args) throws IOException {
        new PgBroker(1337).start();
    }
}
