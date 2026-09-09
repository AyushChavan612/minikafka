package com.minikafka.broker.cluster;

import com.minikafka.broker.network.MiniKafkaServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ClusterManagerTest {

    private MiniKafkaServer controllerServer;

    @AfterEach
    void tearDown() throws IOException {
        if (controllerServer != null) {
            controllerServer.stop();
        }
    }

    private int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

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

    // ==========================================
    // EDGE CASE 1: Validation (RF > Brokers)
    // ==========================================
    @Test
    void testInvalidReplicationFactorThrowsException() {
        ClusterManager cm = new ClusterManager(0, 9092, null);

        // 3 partitions, 2 brokers, RF = 3 (Invalid: 3 > 2)
        assertThrows(IllegalArgumentException.class, () -> {
            cm.startup(3, 2, 3);
        });
    }

    // ==========================================
    // EDGE CASE 2: Partitions > Brokers
    // ==========================================
    @Test
    void testMorePartitionsThanBrokers() throws Exception {
        ClusterManager controller = new ClusterManager(0, 9092, null);
        
        // 5 partitions across 2 brokers with RF = 2
        controller.startup(5, 2, 2);

        Map<Integer, List<Integer>> map = getClusterMap(controller);
        assertEquals(5, map.size());

        // Partition 0 -> Leader: 0 (0%2), Follower: 1 ((0+1)%2)
        assertEquals(List.of(0, 1), map.get(0));
        // Partition 1 -> Leader: 1 (1%2), Follower: 0 ((1+1)%2)
        assertEquals(List.of(1, 0), map.get(1));
        // Partition 2 -> Leader: 0 (2%2), Follower: 1 ((2+1)%2)
        assertEquals(List.of(0, 1), map.get(2));
        // Partition 3 -> Leader: 1 (3%2), Follower: 0 ((3+1)%2)
        assertEquals(List.of(1, 0), map.get(3));
        // Partition 4 -> Leader: 0 (4%2), Follower: 1 ((4+1)%2)
        assertEquals(List.of(0, 1), map.get(4));
    }

    // ==========================================
    // EDGE CASE 3: Brokers > Partitions
    // ==========================================
    @Test
    void testMoreBrokersThanPartitions() throws Exception {
        ClusterManager controller = new ClusterManager(0, 9092, null);
        
        // 2 partitions across 5 brokers with RF = 2
        controller.startup(2, 5, 2);

        Map<Integer, List<Integer>> map = getClusterMap(controller);
        assertEquals(2, map.size());

        // Partition 0 -> Leader: Broker 0, Follower: Broker 1
        assertEquals(List.of(0, 1), map.get(0));
        // Partition 1 -> Leader: Broker 1, Follower: Broker 2
        assertEquals(List.of(1, 2), map.get(1));

        // Brokers 3 and 4 should not be in the mapping (idle standbys)
        for (List<Integer> assigned : map.values()) {
            assertFalse(assigned.contains(3));
            assertFalse(assigned.contains(4));
        }
    }

    // ==========================================
    // INTEGRATION TEST: Network Registration & Map Transfer
    // ==========================================
    @Test
    void testWorkerRegistrationAndMapSyncOverNetwork() throws Exception {
        int controllerPort = findFreePort();
        int workerPort = findFreePort();

        // 1. Boot up Controller Server in background
        controllerServer = new MiniKafkaServer(0, controllerPort, 3, 3, 2, null);
        Thread controllerThread = new Thread(() -> {
            try {
                controllerServer.start();
            } catch (IOException ignored) {}
        });
        controllerThread.start();
        Thread.sleep(100); // Allow selector to bind

        // 2. Boot up Worker ClusterManager pointing to controller
        ClusterManager worker = new ClusterManager(1, workerPort, "localhost:" + controllerPort);
        worker.startup(3, 3, 2);

        // 3. Verify Worker received and parsed the cluster map correctly
        Map<Integer, List<Integer>> workerMap = getClusterMap(worker);
        assertEquals(3, workerMap.size());
        assertEquals(List.of(0, 1), workerMap.get(0));
        assertEquals(List.of(1, 2), workerMap.get(1));
        assertEquals(List.of(2, 0), workerMap.get(2));

        // 4. Verify Controller updated its Address Book
        Field cmField = MiniKafkaServer.class.getDeclaredField("clusterManager");
        cmField.setAccessible(true);
        ClusterManager controllerCM = (ClusterManager) cmField.get(controllerServer);

        Map<Integer, Integer> activeBrokers = getActiveBrokers(controllerCM);
        assertEquals(controllerPort, activeBrokers.get(0));
        assertEquals(workerPort, activeBrokers.get(1));
    }

    // ==========================================
    // EDGE CASE 4: Worker Reconnect / Port Overwrite
    // ==========================================
    @Test
    void testWorkerRestartWithNewPortUpdatesAddressBook() throws Exception {
        int controllerPort = findFreePort();
        int initialWorkerPort = findFreePort();
        int newWorkerPort = findFreePort();

        // 1. Boot Controller
        controllerServer = new MiniKafkaServer(0, controllerPort, 3, 3, 2, null);
        new Thread(() -> {
            try {
                controllerServer.start();
            } catch (IOException ignored) {}
        }).start();
        Thread.sleep(100);

        // 2. Worker 1 boots initially
        ClusterManager worker1 = new ClusterManager(1, initialWorkerPort, "localhost:" + controllerPort);
        worker1.startup(3, 3, 2);

        Field cmField = MiniKafkaServer.class.getDeclaredField("clusterManager");
        cmField.setAccessible(true);
        ClusterManager controllerCM = (ClusterManager) cmField.get(controllerServer);

        assertEquals(initialWorkerPort, getActiveBrokers(controllerCM).get(1));

        // 3. Worker 1 restarts on a new port
        ClusterManager worker1Restarted = new ClusterManager(1, newWorkerPort, "localhost:" + controllerPort);
        worker1Restarted.startup(3, 3, 2);

        // 4. Verify Address Book retained identity '1' but updated port to newWorkerPort
        Map<Integer, Integer> updatedBrokers = getActiveBrokers(controllerCM);
        assertEquals(2, updatedBrokers.size()); // Exactly broker 0 and broker 1
        assertEquals(newWorkerPort, updatedBrokers.get(1)); // Port overwritten cleanly
    }
}