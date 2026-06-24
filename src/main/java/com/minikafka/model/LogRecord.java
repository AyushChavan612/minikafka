package com.minikafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class LogRecord implements Serializable {
    private final long offset;
    private final long timestamp;
    private final String topic;
    private final int partition;
    private final String key;
    private final byte[] payload;
}