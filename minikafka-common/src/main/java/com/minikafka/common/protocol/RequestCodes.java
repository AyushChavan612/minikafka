package com.minikafka.common.protocol;

public class RequestCodes {
    public static final short PRODUCE = 1;
    public static final short FETCH = 2;
    public static final short FETCH_OFFSET = 3; 
    public static final short COMMIT_OFFSET = 4; 
    public static final short JOIN_GROUP = 5;
    public static final short HEARTBEAT = 6;
    public static final short REGISTER_BROKER = 7;
    public static final short BROKER_HEARTBEAT = 8;
    public static final short CLUSTER_MAP_UPDATE = 9;
    public static final short FETCH_METADATA = 10;
}