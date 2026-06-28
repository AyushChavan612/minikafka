package com.minikafka.broker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PartitionIndex {
    private final FileChannel indexChannel;
    private static final int ENTRY_SIZE = 16; 

    public PartitionIndex(Path indexPath) throws IOException {
        this.indexChannel = FileChannel.open(indexPath, 
            StandardOpenOption.CREATE, 
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND); 
    }

    public void append(long offset, long physicalPosition) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(ENTRY_SIZE);
        buffer.putLong(offset);
        buffer.putLong(physicalPosition);
        buffer.flip();

        long endOfFile = indexChannel.size();
        indexChannel.position(endOfFile);
        
        while (buffer.hasRemaining()) {
            indexChannel.write(buffer); 
        }
    }

    public long lookupPosition(long targetOffset) throws IOException {
        long fileSize = indexChannel.size();
        long numberOfEntries = fileSize / ENTRY_SIZE;

        long low = 0;
        long high = numberOfEntries - 1;

        while (low <= high) {
            long mid = (low + high) >>> 1;
            
            ByteBuffer buffer = ByteBuffer.allocate(ENTRY_SIZE);
            indexChannel.read(buffer, mid * ENTRY_SIZE);
            buffer.flip();
            
            long currentOffset = buffer.getLong();
            long currentPosition = buffer.getLong();

            if (currentOffset == targetOffset) {
                return currentPosition;
            } else if (currentOffset < targetOffset) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        
        return -1; 
    }

    public void close() throws IOException {
        if (indexChannel != null && indexChannel.isOpen()) {
            indexChannel.close();
        }
    }
}