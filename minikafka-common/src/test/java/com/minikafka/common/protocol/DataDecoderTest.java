package com.minikafka.common.protocol;

import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DataDecoderTest {

    @Test
    void testDecodeValidRecord() {
        String expectedTopic = "system-events";
        String expectedKey = "event-001";
        String expectedPayload = "Testing the binary decoder isolation!";

        byte[] topicBytes = expectedTopic.getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = expectedKey.getBytes(StandardCharsets.UTF_8);
        byte[] payloadBytes = expectedPayload.getBytes(StandardCharsets.UTF_8);

        int totalSize = 12 + topicBytes.length + keyBytes.length + payloadBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);

        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);

        buffer.putInt(payloadBytes.length);
        buffer.put(payloadBytes);

        buffer.flip();

        DataDecoder.DecodedRecord record = DataDecoder.decode(buffer);

        assertNotNull(record);
        assertEquals(expectedTopic, record.topic, "Topic did not match!");
        assertEquals(expectedKey, record.key, "Key did not match!");
        assertEquals(expectedPayload, record.payload, "Payload did not match!");
        System.out.println("All Data matched");
    }
}