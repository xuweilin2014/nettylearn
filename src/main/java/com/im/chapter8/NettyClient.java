package com.im.chapter8;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Scanner;

/**
 * @author xuwei_000
 */
public class NettyClient {

    public static void main(String[] args) {
        NioEventLoopGroup group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group).channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new ClientHandler());
                    }
                });

        connect(bootstrap, "127.0.0.1", 8888);
    }

    public static void connect(Bootstrap bootstrap, String addr, int port){
        bootstrap.connect(addr, port).addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                if (future.isSuccess()){
                    //连接服务端端成功的话，启动控制器线程
                    startConsoleThread(((ChannelFuture) future).channel());
                }else{
                    //连接服务端失败
                    System.out.println("客户端连接服务器失败");
                }
            }
        });
    }

    private static void startConsoleThread(Channel channel) {
        new Thread(() -> {
            while (!Thread.interrupted()){
                if (LoginUtil.hasLogin(channel)){
                    System.out.println(new Date() + "：输入消息发送至服务端");
                    Scanner scanner = new Scanner(System.in);

                    String line = scanner.nextLine();
                    MessageRequestPacket mrp = new MessageRequestPacket();
                    mrp.setMessage(line);

                    ByteBuf byteBuf = PacketCodeC.INSTANCE.encode(channel.alloc(), mrp);
                    channel.writeAndFlush(byteBuf);
                }
            }
        }).start();
    }

    static class FirstClientHandler extends ChannelInboundHandlerAdapter {

        /**
         * 当客户端连接上服务器端的时候，回调channelActive方法
         */
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            System.out.println(new Date() + ": 客户端写出数据");
            ByteBuf byteBuf = getByteBuf(ctx);
            ctx.pipeline().writeAndFlush(byteBuf);
        }

        private ByteBuf getByteBuf(ChannelHandlerContext ctx) {
            ByteBuf byteBuf = ctx.alloc().buffer();
            byte[] bytes = "人世难逢开口笑".getBytes(StandardCharsets.UTF_8);
            byteBuf.writeBytes(bytes);
            return byteBuf;
        }

        /**
         * 当服务器端向客户端发送数据时，在processSelectedKey函数中，调用unsafe.read来对数据进行处理。
         * unsafe.read会调用NioByteUnsafe中的read方法来进行处理，它会回调pipeline中所有ChannelHandler的
         * channelRead方法
         */
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf byteBuf = (ByteBuf) msg;
            System.out.println(new Date() + ": 客户端收到消息 -> :"
                    + byteBuf.toString(StandardCharsets.UTF_8));
        }
    }
}
