package com.dubbo.activate.impl;

import com.alibaba.dubbo.common.extension.Activate;
import com.dubbo.activate.common.ActivateExt;

@Activate(group = {"default"})
public class ActivateExtImpl implements ActivateExt {
    @Override
    public String echo(String msg) {
        return null;
    }
}
