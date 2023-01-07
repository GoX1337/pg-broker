package org.gox.pg.broker.model;

import java.time.OffsetDateTime;
import java.util.UUID;

public record EventEntity(String topic, UUID id, OffsetDateTime timestamp, Status status, String payload) {}
