package com.xu.netty.reactor.single;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class SingleThreadReactor implements Runnable{

    private Selector selector;

    //Reactor使用ServerSocket用来监听一个端口号
    private ServerSocketChannel ssc;

    public SingleThreadReactor(int port) throws IOException {
        ssc = ServerSocketChannel.open();
        selector = Selector.open();
        ssc.socket().bind(new InetSocketAddress(port));
        ssc.configureBlocking(false);
        SelectionKey sk = ssc.register(selector, SelectionKey.OP_ACCEPT);

        //关联事件处理程序，也就是Acceptor Handler
        sk.attach(new Acceptor());
        System.out.println("Reactor thread listening to port " + port);
    }

    @Override
    public void run() {
        while (true){
            try {
                int select = selector.select();
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> iter = keys.iterator();
                while (iter.hasNext()){
                    SelectionKey sk = iter.next();
                    dispatch(sk);
                    iter.remove();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void dispatch(SelectionKey sk){
        if (sk != null){
            //当一个事件发生时，获取这个事件所对应的Handler，来对事件进行处理
            Runnable obj = (Runnable) sk.attachment();
            if (obj != null){
                obj.run();
            }
        }
    }

    private class Acceptor implements Runnable{
        @Override
        public void run() {
            try {
                SocketChannel sc = ssc.accept();
                // 由于ssc是非阻塞的，所以当有socket连接到端口号的时候，就会返回SocketChannel
                // 如果没有socket连接的话，直接返回null
                if (sc != null){
                    new SimpleHandler(selector, sc);
                    System.out.println("Reactor accepts connection - " + sc.getRemoteAddress());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Thread t = new Thread(new SingleThreadReactor(8899));
        t.setName("reactor thread");
        t.start();
    }
}
