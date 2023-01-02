package org.gox.pg.broker.model;

import java.util.Map;
import java.util.Optional;

public enum ClientType {

    CONSUMER(1),
    PRODUCER(2);

    public final int type;

    private static final Map<Integer, ClientType> TYPES = Map.of(1, ClientType.CONSUMER, 2, ClientType.PRODUCER);

    ClientType(int type) {
        this.type = type;
    }

    public static Optional<ClientType> valueOf(int type) {
        return Optional.ofNullable(TYPES.get(type));
    }
}
