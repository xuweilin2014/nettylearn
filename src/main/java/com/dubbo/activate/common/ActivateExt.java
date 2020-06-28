package com.dubbo.activate.common;

import com.alibaba.dubbo.common.extension.SPI;

@SPI
public interface ActivateExt {

    String echo(String msg);

}
