package com.juejin.im.server;

import com.juejin.im.codec.PacketCodecHandler;
import com.juejin.im.codec.PacketDecoder;
import com.juejin.im.codec.PacketEncoder;
import com.juejin.im.codec.Spliter;
import com.juejin.im.server.handler.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * @author xuwei_000
 */
public class NettyServer {

    public static final int PORT = 8888;

    public static void main(String[] args) {
        NioEventLoopGroup bossGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new Spliter());
                        ch.pipeline().addLast(PacketCodecHandler.INSTANCE);
                        ch.pipeline().addLast(LoginRequestHandler.INSTANCE);
                        ch.pipeline().addLast(AuthHandler.INSTANCE);
                        ch.pipeline().addLast(IMHandler.INSTANCE);
                    }
                });

        bind(serverBootstrap, PORT);
    }

    private static void bind(ServerBootstrap serverBootstrap, int port){
        serverBootstrap.bind(port).addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                if (future.isSuccess()){
                    System.out.println(new Date() + ": 端口[" + port + "]绑定成功!");
                }else{
                    System.out.println("端口[" + port + "]绑定失败!");
                }
            }
        });
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

}
