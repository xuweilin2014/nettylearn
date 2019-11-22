package com.xu.netty.reactor.single;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class SimpleHandler implements Runnable {

    private SocketChannel socketChannel;
    private static State state = State.READING;
    private byte[] bytes;
    private Selector selector;

    SimpleHandler(Selector selector, SocketChannel sc) throws IOException {
        socketChannel = sc;
        socketChannel.configureBlocking(false);
        this.selector = selector;
        SelectionKey sk = socketChannel.register(selector, SelectionKey.OP_READ);
        sk.attach(this);
    }

    @Override
    public void run() {
        if (State.READING.equals(state)){
            try {
                read();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else if (State.WRITING.equals(state)){
            try {
                write();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void write() throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        byteBuffer.put(bytes);
        byteBuffer.flip();
        socketChannel.write(byteBuffer);

        SelectionKey sk = socketChannel.register(selector, SelectionKey.OP_WRITE);
        sk.attach(this);
        state = State.READING;
    }

    private void read() throws IOException {

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        socketChannel.read(buffer);
        buffer.flip();
        bytes = buffer.array();
        String msg = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("client：" + socketChannel.getLocalAddress() + " send " + msg);

        SelectionKey sk = socketChannel.register(selector, SelectionKey.OP_WRITE);
        sk.attach(this);
        state = State.WRITING;
    }
}
