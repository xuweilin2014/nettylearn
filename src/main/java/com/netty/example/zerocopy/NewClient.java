package com.netty.example.zerocopy;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

public class NewClient {
    public static void main(String[] args) throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.connect(new InetSocketAddress("localhost",8899));

        //把SocketChannel设置为阻塞，才能读出原来文件中所有的字节
        socketChannel.configureBlocking(false);

        String filename = "ideaIU-2019.2.2.exe";
        FileChannel fileChannel = new FileInputStream(filename).getChannel();
        long startTime = System.currentTimeMillis();
        long total = 0;
        System.out.println("fileChannel size：" + fileChannel.size());

        long transferCount = 0;

        while (true){
            /*
             * 在Windows操作系统中，在传输大文件时，对于一次传输数据的大小有限制为8MB
             * 使用transferTo在传输数据时，得多次传输，这个时候必须要注意transferTo函数的position位置
             * 调用这个方法时，很多操作系统会把文件系统缓冲区中的字节直接传送到目标channel中
             * 而不需要再进行一次拷贝，所以会比传统的数据复制和发送要快的多
             */
            transferCount = fileChannel.transferTo(total, fileChannel.size(), socketChannel);
            if (transferCount <= 0){
                break;
            }
            total += transferCount;
        }


        System.out.println("耗时：" + (System.currentTimeMillis() - startTime));
        System.out.println("发送的总字节数：" + total);
    }
}
