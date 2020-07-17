package com.dubbo.filter.mfilter;

import com.alibaba.dubbo.rpc.*;


public class FilterFour implements Filter {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        System.out.println("before filter four");
        Result result = invoker.invoke(invocation);
        System.out.println("after filter four");
        return result;
    }
}
