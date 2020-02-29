package com.xu.netty.eighthexample;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

public class NettyPipelineInboundExample {

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
                        ch.pipeline().addLast(new InboundHandlerA());
                        ch.pipeline().addLast(new InboundHandlerB());
                        ch.pipeline().addLast(new InboundHandlerC());
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

class InboundHandlerA extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("InboundHandler A : " + msg);
        // 传播read事件到下一个channelhandler
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // channel激活，触发channelRead事件，从pipeline的heandContext节点开始往下传播
        ctx.channel().pipeline().fireChannelRead("Hello world");
    }

}

class InboundHandlerB extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("InboundHandler B : " + msg);
        // 传播read事件到下一个channelhandler
        ctx.fireChannelRead(msg);
    }

}

class InboundHandlerC extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("InboundHandler C : " + msg);
        // 传播read事件到下一个channelhandler
        ctx.fireChannelRead(msg);
    }
}

