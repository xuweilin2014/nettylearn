package com.xu.netty.eighthexample;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class NettyPipelineOutboundExample {

    public static void main(String[] args) {
        EventLoopGroup group = new NioEventLoopGroup(1);
        ServerBootstrap strap = new ServerBootstrap();
        strap.group(group)
                .channel(NioServerSocketChannel.class)
                .localAddress(new InetSocketAddress(8888))
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new OutboundHandlerA());
                        ch.pipeline().addLast(new OutboundHandlerB());
                        ch.pipeline().addLast(new OutboundHandlerC());
                    }
                });
        try {
            ChannelFuture future = strap.bind().sync();
            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }
    }
}

class OutboundHandlerA extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // 输出消息
        System.out.println("OutboundHandlerA: " + msg);
        // 传播write事件到下一个节点
        ctx.write(msg, promise);
    }
}

class OutboundHandlerB extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // 输出消息
        System.out.println("OutboundHandlerB: " + msg);
        // 传播write事件到下一个节点
        ctx.write(msg, promise);
    }


    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        // 待handlerAdded事件触发3s后，模拟触发一个
        /*ctx.executor().schedule(() -> {
            ctx.channel().write("Hello world ! ");
        }, 3, TimeUnit.SECONDS);*/

        ctx.channel().write("Hello world ! ");
    }
}

class OutboundHandlerC extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // 输出消息
        System.out.println("OutboundHandlerC: " + msg);
        // 传播write事件到下一个节点
        ctx.write(msg, promise);
    }
}
