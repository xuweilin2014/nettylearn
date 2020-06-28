package com.dubbo.activate.impl;

import com.alibaba.dubbo.common.extension.Activate;
import com.dubbo.activate.common.ActivateExt;

@Activate(order = 1, group = {"order"})
public class OrderActivateExtImpl1 implements ActivateExt {
    @Override
    public String echo(String msg) {
        return null;
    }
}
