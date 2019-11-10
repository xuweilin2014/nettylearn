package com.xu.netty.nio.firsttest.exmaple;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NioTestThree {
    public static void main(String[] args) throws IOException {
        FileOutputStream outputStream = new FileOutputStream("NioTestThree.txt");
        FileChannel channel = outputStream.getChannel();

        ByteBuffer byteBuffer = ByteBuffer.allocate(512);
        byte[] msg  = "welcome nihao".getBytes();

        for (int i = 0; i < msg.length; i++) {
            byteBuffer.put(msg[i]);
        }

        byteBuffer.flip();
        channel.write(byteBuffer);
        outputStream.close();
    }
}
