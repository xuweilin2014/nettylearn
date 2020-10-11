package com.netty.example.nio.firsttest.exmaple;

import java.nio.ByteBuffer;

/**
 * ByteBuffer类型化的get和put方法
 */

public class NioTestFive {
    public static void main(String[] args) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(512);

        byteBuffer.putChar('傅');
        byteBuffer.putDouble(2356.2);
        byteBuffer.putInt(5410);
        byteBuffer.putChar('友');

        byteBuffer.flip();

        System.out.println(byteBuffer.getChar());
        System.out.println(byteBuffer.getDouble());
        System.out.println(byteBuffer.getInt());
        System.out.println(byteBuffer.getChar());
    }
}
