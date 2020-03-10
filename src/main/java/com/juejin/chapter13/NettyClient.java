package com.juejin.chapter13;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.nio.charset.Charset;

public class NettyClient {

    public static void main(String[] args) throws InterruptedException {
        NioEventLoopGroup group = new NioEventLoopGroup();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group).channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new FirstClientHandler());
                    }
                });

        bootstrap.connect("127.0.0.1", 8888).sync();
    }

    static class FirstClientHandler extends ChannelInboundHandlerAdapter{

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            for (int i = 0; i < 1000; i++) {
                ByteBuf buffer = getByteBuf(ctx);
                ctx.channel().writeAndFlush(buffer);
            }
        }

        private ByteBuf getByteBuf(ChannelHandlerContext ctx) {
            byte[] bytes = "你好，欢迎关注我的微信公众号，《闪电侠的博客》!".getBytes(Charset.forName("utf-8"));
            ByteBuf buffer = ctx.alloc().buffer();
            buffer.writeBytes(bytes);

            return buffer;
        }
    }
}
