package com.netty.example.nio.firsttest.serverclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NIOClient {
    public static void main(String[] args) throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        Selector selector = Selector.open();
        socketChannel.configureBlocking(false);
        socketChannel.register(selector, SelectionKey.OP_CONNECT);
        socketChannel.connect(new InetSocketAddress("127.0.0.1",8899));

        while (true){
            selector.select();
            Set<SelectionKey> selectionKeys = selector.selectedKeys();
            Iterator<SelectionKey> iter = selectionKeys.iterator();

            while (iter.hasNext()){
                SelectionKey key = iter.next();
                ByteBuffer msgBuffer = ByteBuffer.allocate(1024);
                iter.remove();

                if (key.isConnectable()){
                    SocketChannel client = (SocketChannel) key.channel();

                    if (client.isConnectionPending()){
                        client.finishConnect();

                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        ByteBuffer writeBuffer = ByteBuffer.allocate(1024);
                        buffer.put(("已经建立好连接").getBytes());
                        buffer.flip();
                        client.write(buffer);

                        ExecutorService executorService = Executors.newSingleThreadExecutor(Executors.defaultThreadFactory());
                        executorService.submit(new Runnable() {
                            @Override
                            public void run() {
                                while (true){
                                    try{
                                        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                                        String sendMsg = reader.readLine();
                                        writeBuffer.put(sendMsg.getBytes());
                                        writeBuffer.flip();
                                        client.write(writeBuffer);
                                        writeBuffer.clear();
                                    }catch (Exception ex){
                                        ex.printStackTrace();
                                    }
                                }
                            }
                        });
                    }

                    client.configureBlocking(false);
                    client.register(selector, SelectionKey.OP_READ);
                }

                if (key.isReadable()){
                    SocketChannel client = (SocketChannel) key.channel();

                    client.read(msgBuffer);

                    msgBuffer.flip();
                    String content = StandardCharsets.UTF_8.newDecoder().decode(msgBuffer.asReadOnlyBuffer()).toString();
                    System.out.println(content);
                }
            }
        }
    }
}
