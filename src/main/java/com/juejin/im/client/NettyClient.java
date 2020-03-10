package com.juejin.im.client;

import com.juejin.im.client.handler.LoginResponseHandler;
import com.juejin.im.client.handler.MessageResponseHandler;
import com.juejin.im.codec.PacketDecoder;
import com.juejin.im.codec.PacketEncoder;
import com.juejin.im.codec.Spliter;
import com.juejin.im.util.LoginUtil;
import com.juejin.im.protocol.request.MessageRequestPacket;
import com.juejin.im.protocol.PacketCodeC;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
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
         * 在校验完客户端的状态为【登录成功】之后，在控制台读取一条消息之后按回车，
         * 把消息发送至服务端，服务端收到消息之后，在控制台显示出来。并且返回一条
         * 消息给客户端，客户端在控制台进行显示
         */
        new Thread(() -> {
            while (!Thread.interrupted()){
                if (LoginUtil.hasLogin(channel)){
                    System.out.println(new Date() + "：输入消息发送至服务端");
                    Scanner scanner = new Scanner(System.in);
                    String line = scanner.nextLine();
                    MessageRequestPacket mrp = new MessageRequestPacket();
                    mrp.setMessage(line);

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
}
