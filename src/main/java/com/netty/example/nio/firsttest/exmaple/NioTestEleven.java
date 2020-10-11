package com.netty.example.nio.firsttest.exmaple;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

public class NioTestEleven {
    public static void main(String[] args) throws IOException {
        int[] ports = new int[5];

        ports[0] = 5000;
        ports[1] = 5001;
        ports[2] = 5002;
        ports[3] = 5003;
        ports[4] = 5004;

        Selector selector = Selector.open();

        for (int i = 0; i < ports.length; i++){
            ServerSocketChannel socketChannel  = ServerSocketChannel.open();
            ServerSocket socket = socketChannel.socket();
            socket.bind(new InetSocketAddress(ports[i]));
            socketChannel.configureBlocking(false);
            socketChannel.register(selector, SelectionKey.OP_ACCEPT);
        }

        while (true){
            int select = selector.select();
            System.out.println("selection operation获取的事件数：" + select);
            Set<SelectionKey> selectionKeys = selector.selectedKeys();
            Iterator<SelectionKey> iter = selectionKeys.iterator();

            while (iter.hasNext()){
                SelectionKey key = iter.next();

                if (key.isAcceptable()){
                    ServerSocketChannel channel = (ServerSocketChannel) key.channel();
                    SocketChannel socketChannel = channel.accept();
                    socketChannel.configureBlocking(false);
                    socketChannel.register(selector,SelectionKey.OP_READ);
                    System.out.println("客户端连接已经建立：" + socketChannel);
                    iter.remove();
                }

                if (key.isReadable()){
                    SocketChannel socketChannel = (SocketChannel) key.channel();
                    ByteBuffer byteBuffer = ByteBuffer.allocate(12);
                    int bytesRead = 0;

                    while (true){
                        byteBuffer.clear();
                        int read = socketChannel.read(byteBuffer);

                        if (read <= 0){
                            break;
                        }

                        byteBuffer.flip();
                        socketChannel.write(byteBuffer);
                        bytesRead += read;
                    }

                    System.out.println("读取了：" + bytesRead + ", 来自于：" + socketChannel);
                    iter.remove();
                }
            }
        }
    }
}
