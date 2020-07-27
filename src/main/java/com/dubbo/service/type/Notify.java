package com.dubbo.service.type;

public interface Notify {

    public void onReturn(String name, int id);

    public void onThrow(Throwable t, int id);

}
