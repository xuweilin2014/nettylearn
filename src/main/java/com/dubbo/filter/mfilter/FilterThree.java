package com.dubbo.filter.mfilter;

import com.alibaba.dubbo.rpc.*;

public class FilterThree implements Filter {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        System.out.println("before filter three");
        Result result = invoker.invoke(invocation);
        System.out.println("after filter three");
        return result;
    }
}
