package com.dubbo.service.impl;

import com.dubbo.service.common.CallbackListener;
import com.dubbo.service.common.CallbackService;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CallbackServiceImpl implements CallbackService {

    private Map<String, CallbackListener> listeners = new HashMap<>();

    public CallbackServiceImpl() {
        new Thread(() -> {
            while (true){
                for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                    entry.getValue().changed(getChangedTime(entry.getKey()));
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    @Override
    public void addListener(String key, CallbackListener listener) {
        listeners.put(key, listener);
        listener.changed(getChangedTime(key));
        System.out.println("okk");
    }

    public String getChangedTime(String key){
        return key + " changed : " + new Date();
    }
}
