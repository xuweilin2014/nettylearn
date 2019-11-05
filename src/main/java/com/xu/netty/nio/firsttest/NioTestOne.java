package com.xu.netty.nio.firsttest;

import java.nio.IntBuffer;
import java.security.SecureRandom;

public class NioTestOne {
    public static void main(String[] args) {
        IntBuffer buffer = IntBuffer.allocate(10);

        for (int i = 0; i < 10; i++){
            int randomNumber = new SecureRandom().nextInt(20);
            buffer.put(randomNumber);
        }

        buffer.flip();

        while (buffer.hasRemaining()){
            System.out.println(buffer.get());
        }
    }
}
