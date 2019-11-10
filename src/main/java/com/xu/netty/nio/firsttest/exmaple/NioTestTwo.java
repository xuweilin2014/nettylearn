package com.xu.netty.nio.firsttest.exmaple;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NioTestTwo {
    public static void main(String[] args) throws IOException {
        FileInputStream stream = new FileInputStream("hello.txt");
        FileChannel channel = stream.getChannel();

        ByteBuffer byteBuffer = ByteBuffer.allocate(512);
        channel.read(byteBuffer);

        byteBuffer.flip();

        while (byteBuffer.hasRemaining()){
            byte b = byteBuffer.get();
            System.out.println("Character：" + (char) b);
        }
    }
}
