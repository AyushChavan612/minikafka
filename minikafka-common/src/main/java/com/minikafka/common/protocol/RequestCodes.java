package com.minikafka.common.protocol;

public class RequestCodes {
    public static final short PRODUCE = 1;
    public static final short FETCH = 2;
    public static final short FETCH_OFFSET = 3; // "Where did I leave off?"
    public static final short COMMIT_OFFSET = 4; // "I successfully read up to offset X."
    public static final short JOIN_GROUP = 5;
}