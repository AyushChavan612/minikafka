package com.minikafka.common.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LogRecordTest {

    @Test
    void logRecordStoresDataCorrectly() {
        byte[] payload = "test-message".getBytes();
        LogRecord record = new LogRecord(100L, 1680000000L, "orders", 0, "order-1", payload);

        assertEquals(100L, record.getOffset());
        assertEquals(1680000000L, record.getTimestamp());
        assertEquals("orders", record.getTopic());
        assertEquals(0, record.getPartition());
        assertEquals("order-1", record.getKey());
        assertArrayEquals(payload, record.getPayload());
        System.out.println("All tested executed successfully");
    }
}