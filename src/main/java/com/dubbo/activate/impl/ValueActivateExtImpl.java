package com.dubbo.activate.impl;

import com.alibaba.dubbo.common.extension.Activate;
import com.dubbo.activate.common.ActivateExt;

@Activate(value = {"value"}, group = {"group"})
public class ValueActivateExtImpl implements ActivateExt {
    @Override
    public String echo(String msg) {
        return null;
    }
}
