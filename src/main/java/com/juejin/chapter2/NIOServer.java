package com.juejin.chapter2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Set;

public class NIOServer {

    public static void main(String[] args) throws IOException {
        Selector serverSelector = Selector.open();
        Selector clientSelector = Selector.open();

        new Thread(() -> {
            try {
                // 对应IO编程中服务端启动
                ServerSocketChannel listenerChannel = ServerSocketChannel.open();
                listenerChannel.socket().bind(new InetSocketAddress(8000));
                listenerChannel.configureBlocking(false);
                listenerChannel.register(serverSelector, SelectionKey.OP_ACCEPT);

                while (true) {
                    // 监测是否有新的连接，这里的1指的是阻塞的时间为 1ms
                    if (serverSelector.select(1) > 0) {
                        Set<SelectionKey> set = serverSelector.selectedKeys();
                        Iterator<SelectionKey> keyIterator = set.iterator();

                        while (keyIterator.hasNext()) {
                            SelectionKey key = keyIterator.next();

                            if (key.isAcceptable()) {
                                try {
                                    // (1) 每来一个新连接，不需要创建一个线程，而是直接注册到clientSelector
                                    SocketChannel clientChannel = ((ServerSocketChannel) key.channel()).accept();
                                    clientChannel.configureBlocking(false);
                                    clientChannel.register(clientSelector, SelectionKey.OP_READ);
                                } finally {

                                    /**
                                     * The result of selectedKeys contains the keys to channels that are currently available for their selected operations (eg READ, WRITE).
                                     * If you don't remove the key, it will remain in the set, and a next call to select() (and family) will still include it,
                                     * even though the associated channel is not ready for the selected operation.
                                     *
                                     * A selectable channel's registration with a selector is represented by a SelectionKey object.
                                     * A selector maintains three sets of selection keys:
                                     *
                                     * 1.The key set contains the keys representing the current channel registrations of this selector.
                                     * This set is returned by the keys method.
                                     * 2.The selected-key set is the set of keys such that each key's channel was detected to be ready
                                     * for at least one of the operations identified in the key's interest set during a prior selection operation.
                                     * This set is returned by the selectedKeys method. The selected-key set is always a subset of the key set.
                                     *
                                     * Keys are added to the selected-key set by selection operations. A key may be removed directly from the selected-key
                                     * set by invoking the set's remove method or by invoking the remove method of an iterator obtained from the set.
                                     * Keys are never removed from the selected-key set in any other way; they are not, in particular,
                                     * removed as a side effect of selection operations. Keys may not be added directly to the selected-key set.
                                     */

                                    keyIterator.remove();
                                }
                            }

                        }
                    }
                }
            } catch (IOException ignored) {
            }

        }).start();


        new Thread(() -> {
            try {
                while (true) {
                    // (2) 批量轮询是否有哪些连接有数据可读，这里的1指的是阻塞的时间为 1ms
                    if (clientSelector.select(1) > 0) {
                        Set<SelectionKey> set = clientSelector.selectedKeys();
                        Iterator<SelectionKey> keyIterator = set.iterator();

                        while (keyIterator.hasNext()) {
                            SelectionKey key = keyIterator.next();

                            if (key.isReadable()) {
                                try {
                                    SocketChannel clientChannel = (SocketChannel) key.channel();
                                    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
                                    // (3) 面向 Buffer
                                    clientChannel.read(byteBuffer);
                                    byteBuffer.flip();
                                    System.out.println(Charset.defaultCharset().newDecoder().decode(byteBuffer)
                                            .toString());
                                } finally {
                                    keyIterator.remove();
                                    key.interestOps(SelectionKey.OP_READ);
                                }
                            }

                        }
                    }
                }
            } catch (IOException ignored) {
            }
        }).start();


    }

}
