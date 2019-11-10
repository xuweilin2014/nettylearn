package com.xu.netty.nio.firsttest.exmaple;

import java.nio.IntBuffer;
import java.security.SecureRandom;

public class NioTestOne {
    public static void main(String[] args) {
        IntBuffer buffer = IntBuffer.allocate(10);

        System.out.println("cpacity：" + buffer.capacity());

        for (int i = 0; i < 5; i++){
            int randomNumber = new SecureRandom().nextInt(20);
            buffer.put(randomNumber);
        }

        System.out.println("before flip limit：" + buffer.limit());

        buffer.flip();

        System.out.println("after flip limit：" + buffer.limit());

        System.out.println("enter the loop");

        while (buffer.hasRemaining()){
            System.out.println("position：" + buffer.position());
            System.out.println("limit：" + buffer.limit());
            System.out.println("capcity：" + buffer.capacity());
            buffer.get();
            //System.out.println(buffer.get());
        }
    }
}
