package org.gox.pg.broker;

import org.apache.commons.lang3.StringUtils;
import org.gox.pg.broker.exception.PgBrokerRuntimeException;
import org.gox.pg.broker.server.PgBroker;

import java.io.IOException;

public class PgBrokerMain {

    public static void main(String[] args) throws IOException {
        if(args.length == 0 || StringUtils.isEmpty(args[0]) || !StringUtils.isNumeric(args[0])) {
            throw new PgBrokerRuntimeException("Incorrect port number parameter");
        }
        new PgBroker(Integer.parseInt(args[0])).start();
    }
}
