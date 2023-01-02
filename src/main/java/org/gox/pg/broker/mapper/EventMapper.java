package org.gox.pg.broker.mapper;

import org.gox.pg.broker.model.Event;
import org.gox.pg.broker.model.EventEntity;
import org.gox.pg.broker.model.Status;

import java.util.UUID;

public class EventMapper {

    public static EventEntity map(Event event) {
        return new EventEntity(
                event.topic(),
                UUID.randomUUID(),
                event.timestamp(),
                Status.PENDING,
                event.payload()
        );
    }
}
