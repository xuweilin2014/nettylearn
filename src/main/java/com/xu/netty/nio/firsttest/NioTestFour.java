package com.xu.netty.nio.firsttest;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NioTestFour {
    public static void main(String[] args) throws IOException {
        FileInputStream inputStream = new FileInputStream("hello.txt");
        FileOutputStream outputStream = new FileOutputStream("output.txt");

        FileChannel inputChannel = inputStream.getChannel();
        FileChannel outputChannel = outputStream.getChannel();

        ByteBuffer buffer = ByteBuffer.allocate(512);

        /*inputChannel.read(buffer);
        System.out.println(buffer.limit());
        System.out.println(buffer.position());
        System.out.println(buffer.capacity());

        buffer.flip();

        System.out.println("----------------");
        System.out.println(buffer.limit());
        System.out.println(buffer.position());
        System.out.println(buffer.capacity());*/

        while (true){
            buffer.clear();

            int read = inputChannel.read(buffer);

            if (-1 == read){
                break;
            }

            buffer.flip();
            outputChannel.write(buffer);
        }

        inputChannel.close();
        outputChannel.close();
    }
}
