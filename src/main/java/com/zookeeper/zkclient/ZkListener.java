package com.zookeeper.zkclient;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import java.io.IOException;
import java.util.List;

public class ZkListener {

    public static void main(String[] args) throws IOException {
        ZkClient zkClient = new ZkClient("127.0.0.1:2181", 5000);
        List<String> list = zkClient.subscribeChildChanges("/rpc/com.xu.demoservice", new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                System.out.println(parentPath);
                System.out.println(currentChilds);
                System.out.println(currentChilds.isEmpty());
                System.out.println(currentChilds == null);
            }
        });
        System.out.println(list);
        System.in.read();
    }

}
