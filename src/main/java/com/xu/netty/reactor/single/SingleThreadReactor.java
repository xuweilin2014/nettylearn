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
    private ServerSocketChannel ssc;

    public SingleThreadReactor(int port) throws IOException {
        ssc = ServerSocketChannel.open();
        selector = Selector.open();
        ssc.socket().bind(new InetSocketAddress(port));
        ssc.configureBlocking(false);
        SelectionKey sk = ssc.register(selector, SelectionKey.OP_ACCEPT);
        sk.attach(new Acceptor());
        System.out.println("reactor thread listening to port " + port);
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
            Runnable obj = (Runnable) sk.attachment();
            obj.run();
        }
    }

    private class Acceptor implements Runnable{
        public Acceptor(){
        }

        @Override
        public void run() {
            try {
                SocketChannel sc = ssc.accept();
                new SimpleHandler(selector, sc);
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
