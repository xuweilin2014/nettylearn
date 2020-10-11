package com.netty.example.nio.firsttest.exmaple;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class NioTestEight {
    public static void main(String[] args) throws Exception {
        RandomAccessFile file = new RandomAccessFile("NioTest9.txt","rw");
        FileChannel channel = file.getChannel();

        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE,0,5);

        mappedByteBuffer.put(0, (byte)'a');
        mappedByteBuffer.put(3, (byte)'b');

        file.close();
    }
}
