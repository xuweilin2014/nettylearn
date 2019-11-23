package com.xu.netty.reactor.multiple;

import com.xu.netty.reactor.single.SimpleHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * ReactorGroup中包含了1个Main Reactor，POOL_SIZE-1个Sub Reactor
 * Main Reactor：监听指定端口号，当有连接ACCEPT的时候，就会经由Acceptor将获取的连接注册到POOL_SIZE-1个Sub Reactor上，
 * 而这些连接上发生的事件，都将由Sub Reactor来处理
 * Sub Reactor：监听注册连接channel上发生的IO事件（READ/WRITE）
 */
public class ReactorGroup {

    private static final int POOL_SIZE = 3;

    //创建一个POOL_SIZE大小的线程池
    private Executor reactorPool = Executors.newFixedThreadPool(POOL_SIZE);

    //Main Reactor单独由一个线程来处理
    private Reactor mainReactor;

    //Sub Reactor有POOL_SIZE - 1个，每个单独由一个线程来处理
    private Reactor[] subReactors = new Reactor[POOL_SIZE - 1];
    private int index = 0;

    private ReactorGroup(){
        try{
            mainReactor = new Reactor();
            for (int i = 0; i < subReactors.length; i++){
                subReactors[i] = new Reactor();
            }
        }catch (Exception ex){
            System.out.println(ex.getMessage());
        }
    }

    /**
     * 启动主从 Reactor，初始化并注册 Acceptor，使得主Reactor监听指定的端口
     */
    public void bootstrap(int port){
        //让Main Reactor监听指定端口号上的ACCEPT事件
        new Acceptor(mainReactor.getSelector(), port);
        Thread mainReactorThread = new Thread(mainReactor);
        mainReactorThread.setName("mainReactor");
        reactorPool.execute(mainReactorThread);

        for (int i = 0; i < subReactors.length; i++){
            Thread subReactorThread = new Thread(subReactors[i]);
            subReactorThread.setName("subReactor-" + i);
            reactorPool.execute(subReactorThread);
        }
    }

    /**
     * 初始化并配置 ServerSocketChannel，注册到 mainReactor 的 Selector 上
     */
    class Acceptor implements Runnable {

        Selector selector;
        ServerSocketChannel serverSocketChannel;

        Acceptor(Selector sel, int port){
            try{
                selector = sel;
                serverSocketChannel = ServerSocketChannel.open();
                serverSocketChannel.socket().bind(new InetSocketAddress(port));
                serverSocketChannel.configureBlocking(false);
                SelectionKey sk = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
                sk.attach(this);

                System.out.println("Main Reactor listen to port" + port);
            }catch (Exception ex){
                System.out.println(ex.getMessage());
            }
        }

        @Override
        public void run() {
            try {
                SocketChannel sc = serverSocketChannel.accept();
                System.out.println("Reactor accepts connection - " + sc.getRemoteAddress());
                Reactor subReactor = subReactors[index];

                // 发现无法直接注册，一直获取不到锁，这是由于 从 Reactor 目前正阻塞在 select() 方法上，此方法已经
                // 锁定了 publicKeys（已注册的key)，直接注册会造成死锁
                // 如何解决呢，直接调用 wakeup，有可能还没有注册成功又阻塞了。

                /*
                 * subReactor#register不是直接将handler.socket注册到selector当中，而是将此handler添加到handlers队列中，
                 * 这是因为当执行Acceptor的run方法，希望把此SocketChannel通过register方法注册到subReactor中的selector上时，有可能这个subReactor
                 * 此刻因为调用select方法阻塞，这是因为register最终调用SelectorImpl#register方法，select最终调用
                 * SelectorImpl#lockAndDoSelect方法，但是这两个方法都会尝试获取publicKeys，因此会发生死锁。
                 *
                 * 而解决的办法就是使用队列和wakeup函数，wakeup会使selector方法立即返回。如果不调用wakeup，selector#select方法
                 * 将会一直阻塞，然后对应的SocketChannel和事件有没有注册到selector上面，selector无法进行监听，
                 * selector#select方法就会一直阻塞下去
                 */
                subReactor.register(sc);

                index = (++index) % (POOL_SIZE - 1);
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    static class Reactor implements Runnable {

        private ConcurrentLinkedQueue<SimpleHandler> handlers = new ConcurrentLinkedQueue<>();

        private Selector selector;

        Reactor() throws IOException {
            selector = Selector.open();
        }

        public Selector getSelector() {
            return selector;
        }

        public ConcurrentLinkedQueue<SimpleHandler> getHandlers() {
            return handlers;
        }

        @Override
        public void run() {
            while (!Thread.interrupted()){
                try {
                    SimpleHandler handler;
                    while ((handler = handlers.poll()) != null){
                        SocketChannel socketChannel = handler.getSocketChannel();
                        if (socketChannel != null){
                            socketChannel.configureBlocking(false);
                            SelectionKey sk = socketChannel.register(selector, SelectionKey.OP_READ);
                            sk.attach(handler);
                            handler.setSk(sk);
                        }
                    }

                    selector.select();
                    Set<SelectionKey> sks = selector.selectedKeys();
                    Iterator<SelectionKey> iter = sks.iterator();
                    while (iter.hasNext()){
                        SelectionKey sk = iter.next();
                        dispatch(sk);
                        iter.remove();
                    }
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            }
        }

        private void dispatch(SelectionKey sk){
            Runnable obj = (Runnable) sk.attachment();
            if (obj != null){
                obj.run();
            }
        }

        public void register(SocketChannel sc) throws IOException {
            handlers.offer(new SimpleHandler(sc));
            selector.wakeup();
        }
    }

    public static void main(String[] args) throws IOException {
        ReactorGroup reactorGroup = new ReactorGroup();
        reactorGroup.bootstrap(8899);
    }

}
