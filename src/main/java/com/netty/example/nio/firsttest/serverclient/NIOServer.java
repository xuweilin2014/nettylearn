package com.netty.example.nio.firsttest.serverclient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.*;

public class NIOServer {
    private static Map<String, SocketChannel> socketChannelMap = new HashMap<>();

    public static void main(String[] args) throws IOException {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        ServerSocket serverSocket = serverSocketChannel.socket();
        serverSocket.bind(new InetSocketAddress(8899));
        serverSocketChannel.configureBlocking(false);

        Selector selector = Selector.open();
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        while (true){
            int eventNums = selector.select();
            Set<SelectionKey> selectionKeys = selector.selectedKeys();
            Iterator<SelectionKey> iter = selectionKeys.iterator();

            while (iter.hasNext()){
                SelectionKey key = iter.next();
                iter.remove();

                if (key.isAcceptable()){
                    ServerSocketChannel serverSocketChannel2 = (ServerSocketChannel) key.channel();
                    SocketChannel socketChannel = serverSocketChannel2.accept();
                    socketChannel.configureBlocking(false);
                    socketChannel.register(selector, SelectionKey.OP_READ);

                    String channelKey = "[" + UUID.randomUUID() + "]";
                    socketChannelMap.put(channelKey,socketChannel);
                }

                if (key.isReadable()) {
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    ByteBuffer writeBuffer = ByteBuffer.allocate(1024);
                    SocketChannel socketChannel = (SocketChannel) key.channel();

                    String socketKey = null;

                    for (Map.Entry<String, SocketChannel> entry : socketChannelMap.entrySet()) {
                        if (entry.getValue() == socketChannel) {
                            socketKey = entry.getKey();
                            break;
                        }
                    }

                    socketChannel.read(buffer);
                    buffer.flip();
                    String content = Charset.forName("utf-8").newDecoder().decode(buffer.asReadOnlyBuffer()).toString();
                    System.out.println(socketKey + " : " + content);

                    for (Map.Entry<String, SocketChannel> entry : socketChannelMap.entrySet()) {
                        writeBuffer.put((socketKey + content).getBytes());
                        writeBuffer.flip();
                        entry.getValue().write(writeBuffer);
                        writeBuffer.clear();
                    }
                }
            }
        }
    }
}
