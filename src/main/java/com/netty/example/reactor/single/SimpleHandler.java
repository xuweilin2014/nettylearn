package com.netty.example.reactor.single;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class SimpleHandler implements Runnable {

    private SocketChannel socketChannel;
    private static State state = State.READING;
    private byte[] bytes;
    private SelectionKey sk;

    public SimpleHandler(SocketChannel sc){
        this.socketChannel = sc;
    }

    public SimpleHandler(Selector selector, SocketChannel sc) throws IOException {
        socketChannel = sc;
        socketChannel.configureBlocking(false);
        //此Selector和Reactor模型中的Selector是一个对象，即一个Selector监视所有的事件
        sk = socketChannel.register(selector, SelectionKey.OP_READ);
        sk.attach(this);
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void setSk(SelectionKey sk) {
        this.sk = sk;
    }

    @Override
    public void run() {
        try{
            if (State.READING.equals(state)){
                read();
            }
            else if (State.WRITING.equals(state)){
                write();
            }
        }catch (Exception ex){
            try {
                //如果在读写数据的过程中发生了异常，则直接关闭此SocketChannel
                socketChannel.close();
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

        sk.interestOps(SelectionKey.OP_READ);
        state = State.READING;
    }

    private void read() throws IOException {

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        socketChannel.read(buffer);
        buffer.flip();
        bytes = buffer.array();
        String msg = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("client：" + socketChannel.getRemoteAddress() + " send " + msg);

        sk.interestOps(SelectionKey.OP_WRITE);
        state = State.WRITING;
    }
}
