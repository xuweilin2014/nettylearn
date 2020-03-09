package com.im.chapter4;

import com.sun.beans.editors.ByteEditor;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * @author xuwei_000
 */
public class NettyServer {

    public static void main(String[] args) {
        NioEventLoopGroup bossGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new SimpleServerHandler())
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new FirstServerHandler());
                    }
                });

        serverBootstrap.bind(8888);
    }

    static class FirstServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            //服务器端收到数据
            ByteBuf byteBuf = (ByteBuf) msg;
            System.out.println(new Date() + ": 服务端读到数据 -> ：" + byteBuf.toString(StandardCharsets.UTF_8));

            //服务器端写出数据
            System.out.println(new Date() + ": 服务端写出数据");
            ByteBuf buf = getByteBuf(ctx);
            ctx.pipeline().writeAndFlush(buf);
        }

        private ByteBuf getByteBuf(ChannelHandlerContext ctx) {
            ByteBuf byteBuf = ctx.alloc().buffer();
            byte[] bytes = "上疆场彼此弯弓月".getBytes(StandardCharsets.UTF_8);
            byteBuf.writeBytes(bytes);
            return byteBuf;
        }
    }

    private static class SimpleServerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            System.out.println("channelActive");
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            System.out.println("channelRegistered");
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            System.out.println("handlerAdded");
        }
    }

}
