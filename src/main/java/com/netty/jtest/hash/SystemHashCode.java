package com.netty.jtest.hash;

import com.alibaba.dubbo.rpc.cluster.support.wrapper.MockClusterInvoker;
import com.dubbo.simple.common.DemoService;

import java.util.LinkedList;
import java.util.List;

public class SystemHashCode {

    public static void main(String[] args) {
        List<Invoker> invokers = new LinkedList<>();
        invokers.add(new Invoker(DemoService.class, "ok"));
        invokers.add(new Invoker(MockClusterInvoker.class, "ok"));
        System.out.println(invokers.hashCode());
        System.out.println(System.identityHashCode(invokers));
        invokers.get(0).setMethodName("good");
        System.out.println(invokers.hashCode());
        System.out.println(System.identityHashCode(invokers));
        invokers.get(0).setMethodName("da");
        System.out.println(invokers.hashCode());
        System.out.println(System.identityHashCode(invokers));
    }

    private static class Invoker{

        private Class<?> type;

        private String methodName;

        public Invoker(Class<?> type, String methodName) {
            this.type = type;
            this.methodName = methodName;
        }

        public void setType(Class<?> type) {
            this.type = type;
        }

        public void setMethodName(String methodName) {
            this.methodName = methodName;
        }

        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (methodName != null ? methodName.hashCode() : 0);
            return result;
        }
    }

}
