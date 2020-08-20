package com.zookeeper.zkclient;

import org.I0Itec.zkclient.ZkClient;

public class ZkClientConnect {

    public static void main(String[] args) {
        ZkClient zkClient = new ZkClient("127.0.0.1:2181", 1000);
        zkClient.createPersistent("/group/interface/com.xu.dubbo", true);
        zkClient.delete("/group/interface/com.xu.dubbo");
    }

}
