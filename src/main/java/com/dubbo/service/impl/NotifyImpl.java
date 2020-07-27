package com.dubbo.service.impl;

import com.dubbo.service.type.Notify;

import java.util.HashMap;
import java.util.Map;

public class NotifyImpl implements Notify {

    public Map<Integer, Object> map = new HashMap<>();

    @Override
    public void onReturn(String name, int id) {
        map.put(id, name);
        System.out.println("onReturn: " + name);
    }

    @Override
    public void onThrow(Throwable t, int id) {
        map.put(id, t);
        System.out.println("onThrow: " + t);
    }
}
