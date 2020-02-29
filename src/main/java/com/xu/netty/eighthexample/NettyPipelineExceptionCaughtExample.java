package com.xu.netty.eighthexample;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.net.InetSocketAddress;

public class NettyPipelineExceptionCaughtExample {

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

    static class InboundHandlerA extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            throw new Exception("ERROR !!!");
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            System.out.println("InboundHandlerA.exceptionCaught:" + cause.getMessage());
            ctx.fireExceptionCaught(cause);
        }
    }

    static class InboundHandlerB extends ChannelInboundHandlerAdapter {

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            System.out.println("InboundHandlerB.exceptionCaught:" + cause.getMessage());
            ctx.fireExceptionCaught(cause);
        }
    }

    static class InboundHandlerC extends ChannelInboundHandlerAdapter {

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            System.out.println("InboundHandlerC.exceptionCaught:" + cause.getMessage());
            ctx.fireExceptionCaught(cause);
        }
    }


    static class OutboundHandlerA extends ChannelOutboundHandlerAdapter {

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            System.out.println("OutboundHandlerA.exceptionCaught:" + cause.getMessage());
            ctx.fireExceptionCaught(cause);
        }

    }

    static class OutboundHandlerB extends ChannelOutboundHandlerAdapter {

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            System.out.println("OutboundHandlerB.exceptionCaught:" + cause.getMessage());
            ctx.fireExceptionCaught(cause);
        }
    }

    static class OutboundHandlerC extends ChannelOutboundHandlerAdapter {

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            System.out.println("OutboundHandlerC.exceptionCaught:" + cause.getMessage());
            ctx.fireExceptionCaught(cause);
        }
    }

}

