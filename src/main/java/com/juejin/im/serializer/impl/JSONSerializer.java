package com.juejin.im.serializer.impl;

import com.alibaba.fastjson.JSON;
import com.juejin.im.serializer.Serializer;
import com.juejin.im.serializer.SerializerAlgorithm;

public class JSONSerializer implements Serializer {
    @Override
    public byte getSerializerAlgorithm() {
        return SerializerAlgorithm.JSON;
    }

    @Override
    public byte[] serialize(Object obj) {
        return JSON.toJSONBytes(obj);
    }

    @Override
    public <T> T deserialize(Class<T> clazz, byte[] bytes) {
        return JSON.parseObject(bytes, clazz);
    }
}
