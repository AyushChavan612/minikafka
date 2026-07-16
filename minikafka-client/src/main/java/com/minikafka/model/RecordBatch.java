package com.minikafka.model;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class RecordBatch {
    private final long createdAt;
    private int totalBytes;
    private final List<byte[]> payloads;
    private final List<String> keys;

    public RecordBatch() {
        this.createdAt = System.currentTimeMillis();
        this.totalBytes = 0;
        this.payloads = new ArrayList<>();
        this.keys = new ArrayList<>();
    }

    public void add(String key, byte[] payload) {
        String safeKey = (key == null) ? "" : key;
        this.keys.add(safeKey);
        this.payloads.add(payload);
        this.totalBytes += payload.length + safeKey.length();
    }
}