package com.xu.netty.nio.firsttest;

import java.nio.ByteBuffer;


/**
 * Slice Buffer
 */
public class NioTestSix {
    public static void main(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocate(10);

        for (int i = 0; i < buffer.capacity(); i++){
            buffer.put((byte)i);
        }

        buffer.position(2);
        buffer.limit(6);

        /*
         * slice方法创建一个新的buffer，这个buffer和原来的buffer共享同一段内存，
         * 改变其中一个buffer中的值，那么另外一个buffer中的对应的值也会发生改变
         * 但是这两个buffer中的position、limit、capacity等属性是完全独立的，互不影响
         */
        ByteBuffer sliceBuffer = buffer.slice();

        for (int i = 0; i < sliceBuffer.capacity(); i++){
            byte b = sliceBuffer.get(i);
            b *= 2;
            sliceBuffer.put(i,b);
        }

        buffer.position(0);
        buffer.limit(buffer.capacity());

        while (buffer.hasRemaining()){
            System.out.println(buffer.get());
        }
    }
}
