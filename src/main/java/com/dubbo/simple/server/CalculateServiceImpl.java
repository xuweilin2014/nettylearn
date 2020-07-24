package com.dubbo.simple.server;

import com.dubbo.simple.common.CalculateService;

public class CalculateServiceImpl implements CalculateService {

    @Override
    public int add(int a, int b) {
        return a + b;
    }

    @Override
    public int multiply(int a, int b) {
        return a * b;
    }

}
