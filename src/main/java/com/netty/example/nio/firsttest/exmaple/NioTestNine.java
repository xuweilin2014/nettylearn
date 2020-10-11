package com.netty.example.nio.firsttest.exmaple;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class NioTestNine {
    public static void main(String[] args) throws IOException {
        RandomAccessFile accessFile = new RandomAccessFile("NioTest10.txt","rw");
        FileChannel fileChannel = accessFile.getChannel();

        FileLock fileLock = fileChannel.lock(3,6,true);

        System.out.println("valid：" + fileLock.isValid());
        System.out.println("lock type：" + fileLock.isShared());

        fileLock.release();
        accessFile.close();
    }
}
