package com.minikafka.common.protocol;

public class NumberOfPartitions {
    private static int paritionCount=3;

    public static void setNumberOfPartition(int count){
        NumberOfPartitions.paritionCount = count;
    }

    public static int getNumberOfPartitions(){
        return paritionCount;
    } 
}
