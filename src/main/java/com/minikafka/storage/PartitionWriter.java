package com.minikafka.storage;

import com.minikafka.model.LogRecord;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PartitionWriter {
    
    private final FileChannel fileChannel;

    public PartitionWriter(Path partitionPath) throws IOException {
        this.fileChannel = FileChannel.open(partitionPath, 
            StandardOpenOption.CREATE, 
            StandardOpenOption.APPEND, 
            StandardOpenOption.WRITE);
    }

    public void append(LogRecord record) throws IOException {
        byte[] topicBytes = record.getTopic() != null ? record.getTopic().getBytes() : new byte[0];
        byte[] keyBytes = record.getKey() != null ? record.getKey().getBytes() : new byte[0];
        byte[] payloadBytes = record.getPayload() != null ? record.getPayload() : new byte[0];

        int headerSize = 32;
        int totalSize = headerSize + topicBytes.length + keyBytes.length + payloadBytes.length;
        
        ByteBuffer buffer = ByteBuffer.allocateDirect(totalSize);

        buffer.putLong(record.getOffset());
        buffer.putLong(record.getTimestamp());
        buffer.putInt(record.getPartition());
        
        buffer.putInt(topicBytes.length);
        if (topicBytes.length > 0) {
            buffer.put(topicBytes);
        }

        buffer.putInt(keyBytes.length);
        if (keyBytes.length > 0) {
            buffer.put(keyBytes);
        }

        buffer.putInt(payloadBytes.length);
        if (payloadBytes.length > 0) {
            buffer.put(payloadBytes);
        }

        buffer.flip();
        
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }
    }

    public void close() throws IOException {
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.close();
        }
    }
}