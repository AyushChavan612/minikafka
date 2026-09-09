package com.minikafka.broker.cluster;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ClusterManagerFailureTest {

    @SuppressWarnings("unchecked")
    private Map<Integer, List<Integer>> getClusterMap(ClusterManager cm) throws Exception {
        Field field = ClusterManager.class.getDeclaredField("clusterMap");
        field.setAccessible(true);
        return (Map<Integer, List<Integer>>) field.get(cm);
    }

    @SuppressWarnings("unchecked")
    private Map<Integer, Long> getHeartbeatMap(ClusterManager cm) throws Exception {
        Field field = ClusterManager.class.getDeclaredField("lastHeartbeatTimestamp");
        field.setAccessible(true);
        return (Map<Integer, Long>) field.get(cm);
    }

    // ==========================================
    // TEST 1: Rebalance Math & Leader Election
    // ==========================================
    @Test
    void testBrokerFailureTriggersLeaderElection() throws Exception {
        // Boot a Controller
        ClusterManager controller = new ClusterManager(0, 9092, null);
        controller.startup(3, 3, 2); 
        // Initial Map for RF=2:
        // Partition 0 -> Leader: 0, Follower: 1
        // Partition 1 -> Leader: 1, Follower: 2
        // Partition 2 -> Leader: 2, Follower: 0

        // 1. Manually add brokers to the Active registries (simulating registration)
        Field activeBrokersField = ClusterManager.class.getDeclaredField("activeBrokers");
        activeBrokersField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, Integer> activeBrokers = (Map<Integer, Integer>) activeBrokersField.get(controller);
        activeBrokers.put(1, 9093);
        activeBrokers.put(2, 9094);
        
        getHeartbeatMap(controller).put(1, System.currentTimeMillis());
        getHeartbeatMap(controller).put(2, System.currentTimeMillis());

        // 2. Simulate the Janitor detecting Broker 1 is dead
        Method handleFailureMethod = ClusterManager.class.getDeclaredMethod("handleBrokerFailure", int.class);
        handleFailureMethod.setAccessible(true);
        handleFailureMethod.invoke(controller, 1);

       // 3. Verify the Rebalance & Replenishment
        Map<Integer, List<Integer>> updatedMap = getClusterMap(controller);
        
        // P0 had [0, 1]. Broker 1 died. Broker 2 was pulled in as standby.
        assertEquals(List.of(0, 2), updatedMap.get(0));
        
        // P1 had [1, 2]. Broker 1 died. Broker 2 promoted to leader, Broker 0 pulled in as standby.
        assertEquals(List.of(2, 0), updatedMap.get(1));
        
        // P2 had [2, 0]. Broker 1 wasn't involved, so it remains untouched.
        assertEquals(List.of(2, 0), updatedMap.get(2));

        // 4. Verify Broker 1 was wiped from Address Book and Clocks
        assertFalse(activeBrokers.containsKey(1));
        assertFalse(getHeartbeatMap(controller).containsKey(1));
    }

    // ==========================================
    // TEST 2: Heartbeat Sensor Logic
    // ==========================================
    @Test
    void testHeartbeatUpdatesClock() throws Exception {
        ClusterManager controller = new ClusterManager(0, 9092, null);
        Map<Integer, Long> clocks = getHeartbeatMap(controller);
        
        // Set an artificially old timestamp for Broker 1
        long oldTime = System.currentTimeMillis() - 20000;
        clocks.put(1, oldTime);
        
        // Simulate an incoming heartbeat packet from Broker 1 over the network
        ByteBuffer pingBuffer = ByteBuffer.allocate(4);
        pingBuffer.putInt(1); // Worker ID = 1
        pingBuffer.flip();
        
        controller.handleBrokerHeartbeat(pingBuffer);
        
        // Verify the clock was updated to "now"
        long newTime = clocks.get(1);
        assertTrue(newTime > oldTime);
        assertTrue((System.currentTimeMillis() - newTime) < 100); // Should be practically instant
    }

    // ==========================================
    // TEST 3: Worker Dynamic Map Parsing
    // ==========================================
    @Test
    void testWorkerParsesDynamicMapUpdate() throws Exception {
        // Boot a Worker (pointing to a fake controller address)
        ClusterManager worker = new ClusterManager(1, 9093, "localhost:9092");
        
        // Simulate the byte buffer sent by the Controller AFTER the short header is read
        // Format: [Int: Total Partitions] -> Loop { [Int: PartID], [Int: NumBrokers], [Int: Broker1], ... }
        ByteBuffer mapUpdateBuffer = ByteBuffer.allocate(24);
        mapUpdateBuffer.putInt(1); // Total Partitions = 1
        
        mapUpdateBuffer.putInt(0); // Partition ID = 0
        mapUpdateBuffer.putInt(2); // Num Brokers assigned = 2
        mapUpdateBuffer.putInt(1); // Leader = 1
        mapUpdateBuffer.putInt(2); // Follower = 2
        mapUpdateBuffer.flip();
        
        // Worker receives the sudden update
        worker.handleDynamicMapUpdate(mapUpdateBuffer);
        
        // Verify the worker's internal map completely overwrote its old state
        Map<Integer, List<Integer>> workerMap = getClusterMap(worker);
        assertEquals(1, workerMap.size());
        assertEquals(List.of(1, 2), workerMap.get(0));
    }
}