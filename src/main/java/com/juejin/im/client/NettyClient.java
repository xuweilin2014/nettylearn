package com.juejin.im.client;

import com.juejin.im.client.handler.LoginResponseHandler;
import com.juejin.im.client.handler.MessageResponseHandler;
import com.juejin.im.codec.PacketDecoder;
import com.juejin.im.codec.PacketEncoder;
import com.juejin.im.codec.Spliter;
import com.juejin.im.protocol.request.LoginRequestPacket;
import com.juejin.im.protocol.request.MessageRequestPacket;
import com.juejin.im.util.SessionUtil;
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
                        ch.pipeline().addLast(new Spliter());
                        ch.pipeline().addLast(new PacketDecoder());
                        ch.pipeline().addLast(new LoginResponseHandler());
                        ch.pipeline().addLast(new MessageResponseHandler());
                        ch.pipeline().addLast(new PacketEncoder());
                    }
                });

        connect(bootstrap, "127.0.0.1", 8888);
    }

    public static void connect(Bootstrap bootstrap, String addr, int port){
        bootstrap.connect(addr, port).addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                if (future.isSuccess()){
                    //连接服务端端成功的话，启动控制台线程，
                    //然后在控制台线程中，判断只要当前channel为登录状态，就允许客户端向服务器端发送消息
                    startConsoleThread(((ChannelFuture) future).channel());
                }else{
                    //连接服务端失败
                    System.out.println("客户端连接服务器失败");
                }
            }
        });
    }

    private static void startConsoleThread(Channel channel) {
        /**
         * 如果校验客户端的状态为【登录成功】之后，则发送消息数据包到服务器端，
         * 数据包中有发送给的用户ID和发送的具体消息
         * 如果校验客户端的状态为【首次等】之后，则要求用户输入登录的用户名，并且
         * 发送登录数据包给服务器端
         */
        new Thread(() -> {
            while (!Thread.interrupted()){
                Scanner scanner = new Scanner(System.in);
                if (!SessionUtil.hasLogin(channel)) {
                    System.out.println("输入用户名登录：");
                    String userName = scanner.nextLine();

                    LoginRequestPacket lrp = new LoginRequestPacket();
                    lrp.setUsername(userName);
                    lrp.setPassword("pwd");

                    channel.writeAndFlush(lrp);
                    waitForLoginResponse();
                }else{
                    MessageRequestPacket mrp = new MessageRequestPacket();
                    mrp.setToUserId(scanner.next());
                    mrp.setMessage(scanner.next());
                    channel.writeAndFlush(mrp);
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

    private static void waitForLoginResponse() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
    }
}
