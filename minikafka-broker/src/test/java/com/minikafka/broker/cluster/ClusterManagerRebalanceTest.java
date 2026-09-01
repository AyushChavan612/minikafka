package com.minikafka.broker.cluster;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ClusterManagerRebalanceTest {

    @SuppressWarnings("unchecked")
    private Map<Integer, List<Integer>> getClusterMap(ClusterManager cm) throws Exception {
        Field field = ClusterManager.class.getDeclaredField("clusterMap");
        field.setAccessible(true);
        return (Map<Integer, List<Integer>>) field.get(cm);
    }

    @SuppressWarnings("unchecked")
    private Map<Integer, Integer> getActiveBrokers(ClusterManager cm) throws Exception {
        Field field = ClusterManager.class.getDeclaredField("activeBrokers");
        field.setAccessible(true);
        return (Map<Integer, Integer>) field.get(cm);
    }

    private void invokeHandleBrokerFailure(ClusterManager cm, int deadBrokerId) throws Exception {
        Method method = ClusterManager.class.getDeclaredMethod("handleBrokerFailure", int.class);
        method.setAccessible(true);
        method.invoke(cm, deadBrokerId);
    }

    private void invokeReplenish(ClusterManager cm) throws Exception {
        Method method = ClusterManager.class.getDeclaredMethod("replenishUnderReplicatedPartitions");
        method.setAccessible(true);
        method.invoke(cm);
    }

    // ==========================================
    // TEST 1: Standby Broker Immediate Promotion
    // ==========================================
    @Test
    void testStandbyBrokerPromotedOnCrash() throws Exception {
        ClusterManager controller = new ClusterManager(0, 9092, null);
        
        // 1 Partition, 2 initial brokers (0 and 1), Target RF = 2
        controller.startup(1, 2, 2); 
        // Initial Map: P0 -> [0, 1]

        Map<Integer, Integer> activeBrokers = getActiveBrokers(controller);
        activeBrokers.put(1, 9093); // Worker 1 is alive
        activeBrokers.put(2, 9094); // Worker 2 is alive (Idle Standby)

        // Simulate Worker 1 crashing. 
        // handleBrokerFailure should remove Worker 1, shrink to [0], 
        // then replenish() should immediately pull Worker 2 in.
        invokeHandleBrokerFailure(controller, 1);

        Map<Integer, List<Integer>> updatedMap = getClusterMap(controller);
        
        // P0 should now be [0, 2]
        List<Integer> replicas = updatedMap.get(0);
        assertEquals(2, replicas.size(), "Partition should have replenished back to RF=2");
        assertEquals(0, replicas.get(0), "Leader should be Broker 0");
        assertEquals(2, replicas.get(1), "Standby Broker 2 should have been promoted to replica");
    }

    // ==========================================
    // TEST 2: Crashed Broker Rejoin Replenishment
    // ==========================================
    @Test
    void testRejoiningBrokerGetsReassigned() throws Exception {
        ClusterManager controller = new ClusterManager(0, 9092, null);
        
        // 1 Partition, 2 brokers, RF = 2
        controller.startup(1, 2, 2); 
        // Initial Map: P0 -> [0, 1]

        Map<Integer, Integer> activeBrokers = getActiveBrokers(controller);
        activeBrokers.put(1, 9093); // Worker 1 is alive

        // 1. Worker 1 crashes.
        invokeHandleBrokerFailure(controller, 1);
        
        // Verify it shrank to [0]
        assertEquals(1, getClusterMap(controller).get(0).size());
        assertEquals(0, getClusterMap(controller).get(0).get(0));

        // 2. Worker 1 reboots and registers (Adding back to activeBrokers)
        activeBrokers.put(1, 9093); 
        
        // Simulate the Controller calling replenish (which happens inside handleIncomingWorkerRegistration)
        invokeReplenish(controller);

        // 3. Verify Worker 1 got its job back!
        Map<Integer, List<Integer>> restoredMap = getClusterMap(controller);
        List<Integer> replicas = restoredMap.get(0);
        
        assertEquals(2, replicas.size(), "Partition should have restored to RF=2");
        assertTrue(replicas.contains(0));
        assertTrue(replicas.contains(1), "Broker 1 should be added back as a follower");
    }
}