package com.minikafka.broker.storage;

import com.minikafka.common.model.LogRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PartitionReader {

    private final FileChannel fileChannel;

    public PartitionReader(Path partitionPath) throws IOException {
        this.fileChannel = FileChannel.open(partitionPath, StandardOpenOption.READ);
    }

    public LogRecord readRecordAt(long bytePosition) throws IOException {
        if (bytePosition >= fileChannel.size()) {
            return null;
        }

        fileChannel.position(bytePosition);

        ByteBuffer baseHeader = ByteBuffer.allocate(24);
        int bytesRead = fileChannel.read(baseHeader);
        
        if (bytesRead < 24) {
            return null;
        }

        baseHeader.flip();
        long offset = baseHeader.getLong();
        long timestamp = baseHeader.getLong();
        int partition = baseHeader.getInt();
        int topicLen = baseHeader.getInt();

        String topic = null;
        if (topicLen > 0) {
            ByteBuffer topicBuffer = ByteBuffer.allocate(topicLen);
            fileChannel.read(topicBuffer);
            topicBuffer.flip();
            topic = new String(topicBuffer.array());
        }

        ByteBuffer keyLenBuffer = ByteBuffer.allocate(4);
        fileChannel.read(keyLenBuffer);
        keyLenBuffer.flip();
        int keyLen = keyLenBuffer.getInt();

        String key = null;
        if (keyLen > 0) {
            ByteBuffer keyBuffer = ByteBuffer.allocate(keyLen);
            fileChannel.read(keyBuffer);
            keyBuffer.flip();
            key = new String(keyBuffer.array());
        }

        ByteBuffer payloadLenBuffer = ByteBuffer.allocate(4);
        fileChannel.read(payloadLenBuffer);
        payloadLenBuffer.flip();
        int payloadLen = payloadLenBuffer.getInt();

        byte[] payload = new byte[0];
        if (payloadLen > 0) {
            ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadLen);
            fileChannel.read(payloadBuffer);
            payloadBuffer.flip();
            payload = payloadBuffer.array();
        }

        return new LogRecord(offset, timestamp, topic, partition, key, payload);
    }

    public void close() throws IOException {
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.close();
        }
    }
}