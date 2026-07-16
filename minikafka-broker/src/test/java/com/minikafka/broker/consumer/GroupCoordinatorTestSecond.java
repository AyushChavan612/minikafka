package com.minikafka.broker.consumer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GroupCoordinatorTestSecond {

    @Test
    void testCommitAndRecovery() throws InterruptedException {
        System.out.println("--- STARTING BROKER SIMULATION 1 ---");
        // 1. Simulate Broker Startup
        GroupCoordinator broker1 = new GroupCoordinator();
        
        String testGroup = "test-recovery-group";
        String testTopic = "test-topic";
        int partitionId = 0;
        long offsetToCommit = 1042L;

        // 2. Commit a new offset (This saves to RAM and streams to your HDD text file)
        broker1.commitOffset(testGroup, testTopic, partitionId, offsetToCommit);
        
        // 3. Verify it is instantly available in RAM
        long fetchedOffset = broker1.fetchOffset(testGroup, testTopic, partitionId);
        assertEquals(offsetToCommit, fetchedOffset, "Broker 1 should return the offset we just committed.");
        
        // 4. Simulate Broker Shutdown / Crash
        broker1.close();
        System.out.println("--- BROKER 1 SHUT DOWN ---");
        
        // Brief pause to ensure the OS has finished flushing the FileChannel to the hard drive
        Thread.sleep(100);

        System.out.println("--- STARTING BROKER SIMULATION 2 ---");
        // 5. Simulate Broker Restart
        // Because we hardcoded the path, this will automatically read the file Broker 1 just wrote to!
        GroupCoordinator broker2 = new GroupCoordinator();
        
        // 6. Verify it successfully loaded the offset back from the text file into the new RAM map
        long recoveredOffset = broker2.fetchOffset(testGroup, testTopic, partitionId);
        System.out.println("Recovered offset after restart: " + recoveredOffset);
        
        assertEquals(offsetToCommit, recoveredOffset, "Broker 2 should have loaded the offset from disk after restart.");
        
        // 7. Cleanup
        broker2.close();
        System.out.println("--- TEST PASSED: DURABILITY CONFIRMED ---");
    }
}