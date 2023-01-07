package org.gox.pg.broker.model;

import java.time.OffsetDateTime;

public record Event(String topic, OffsetDateTime timestamp, String payload) {}
