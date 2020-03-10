package com.juejin.im.serializer;

import com.juejin.im.serializer.impl.JSONSerializer;

public interface Serializer {

    /**
     * 返回序列化算法的标识
     */
    byte getSerializerAlgorithm();

    /**
     * 将java对象转换成二进制
     */
    byte[] serialize(Object obj);

    /**
     * 将二进制数组转换成java对象
     */
    <T> T deserialize(Class<T> clazz, byte[] bytes);

    Serializer DEFAULT = new JSONSerializer();

}
