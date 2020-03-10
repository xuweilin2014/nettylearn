package com.juejin.chapter3;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class NettyServer {

    public static void main(String[] args) {
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        NioEventLoopGroup bossGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                    }
                });
        bind(bootstrap, 1000);
    }

    public static void bind(ServerBootstrap bootstrap, int port){
        bootstrap.bind(port).addListener(future -> {
            if (future.isSuccess()){
                System.out.println("绑定端口号：" + port + " 成功");
            }else{
                System.out.println("绑定端口号：" + port + " 失败");
                bind(bootstrap, port + 1);
            }
        });
    }

}
