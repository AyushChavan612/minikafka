package com.minikafka.common.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DataDecoder{

    public static DecodedRecord decode(ByteBuffer buffer) {
        int topicLen = buffer.getInt();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        int keyLen = buffer.getInt();
        byte[] keyBytes = new byte[keyLen];
        buffer.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        int payloadLen = buffer.getInt();
        byte[] payloadBytes = new byte[payloadLen];
        buffer.get(payloadBytes);
        String payload = new String(payloadBytes, StandardCharsets.UTF_8);

        return new DecodedRecord(topic, key, payload);
    }

    public static class DecodedRecord {
        public final String topic;
        public final String key;
        public final String payload;

        public DecodedRecord(String topic, String key, String payload) {
            this.topic = topic;
            this.key = key;
            this.payload = payload;
        }
    }
}