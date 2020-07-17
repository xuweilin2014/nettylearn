package com.dubbo.filter.mfilter;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.rpc.*;

@Activate(group = {Constants.CONSUMER})
public class FilterOne implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        System.out.println("before filter one");
        Result result = invoker.invoke(invocation);
        System.out.println("after filter one");
        return result;
    }
}
