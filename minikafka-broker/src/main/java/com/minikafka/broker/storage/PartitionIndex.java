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
                StandardOpenOption.READ, 
                StandardOpenOption.WRITE);
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

        System.out.println("Index size: " + fileSize + " bytes, Entries: " + numberOfEntries);

        while (low <= high) {
            long mid = (low + high) >>> 1;
            System.out.println("Binary search - Low: " + low + " Mid: " + mid + " High: " + high);

            ByteBuffer buffer = ByteBuffer.allocate(ENTRY_SIZE);
            long readPosition = mid * ENTRY_SIZE;

            // 1. ROBUST READ LOOP: Keep reading until we get exactly 16 bytes
            while (buffer.hasRemaining()) {
                int bytesRead = indexChannel.read(buffer, readPosition);
                if (bytesRead <= 0) {
                    break; // EOF reached unexpectedly
                }
                readPosition += bytesRead;
            }

            buffer.flip();

            // 2. SAFETY CHECK: If the file was corrupted and we didn't get 16 bytes, bail out cleanly
            if (buffer.remaining() < ENTRY_SIZE) {
                System.err.println("WARNING: Corrupted index entry at mid " + mid + ". Not enough bytes.");
                return -1;
            }

            long currentOffset = buffer.getLong();
            long currentPosition = buffer.getLong();

            System.out.println("Found Entry - Offset: " + currentOffset + " | Byte Position: " + currentPosition);

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