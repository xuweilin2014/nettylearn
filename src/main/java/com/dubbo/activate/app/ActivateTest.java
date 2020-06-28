package com.dubbo.activate.app;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.dubbo.activate.common.ActivateExt;

import java.util.List;

public class ActivateTest {

    public static void main(String[] args) {
        test3();
    }

    public static void test1(){
        ExtensionLoader<ActivateExt> loader = ExtensionLoader.getExtensionLoader(ActivateExt.class);
        URL url = URL.valueOf("test://localhost/test");
        List<ActivateExt> list = loader.getActivateExtension(url, new String[]{}, "default");
        System.out.println(list.size());
        list.forEach(item -> System.out.println(item.getClass()));
    }

    public static void test2(){
        ExtensionLoader<ActivateExt> loader = ExtensionLoader.getExtensionLoader(ActivateExt.class);
        URL url = URL.valueOf("test://localhost/test");
        List<ActivateExt> list = loader.getActivateExtension(url, new String[]{}, "group1");
        System.out.println(list.size());
        list.forEach(item -> System.out.println(item.getClass()));
    }

    public static void test3(){
        ExtensionLoader<ActivateExt> loader = ExtensionLoader.getExtensionLoader(ActivateExt.class);
        URL url = URL.valueOf("test://localhost/test");
        // 注意这里要使用url接收,不能直接url.addParameter()
        url = url.addParameter("value", "test");
        List<ActivateExt> list = loader.getActivateExtension(url, new String[]{"order1", "default"}, "group");
        System.out.println(list.size());
        list.forEach(item -> System.out.println(item.getClass()));
    }

    public static void test4(){
        ExtensionLoader<ActivateExt> loader = ExtensionLoader.getExtensionLoader(ActivateExt.class);
        URL url = URL.valueOf("test://localhost/test");
        List<ActivateExt> list = loader.getActivateExtension(url, new String[]{}, "order");
        System.out.println(list.size());
        list.forEach(item -> System.out.println(item.getClass()));
    }



}
