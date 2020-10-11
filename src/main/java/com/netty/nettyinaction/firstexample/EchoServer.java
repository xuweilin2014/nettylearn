package com.netty.nettyinaction.firstexample;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

public class EchoServer {
    private int port;

    public EchoServer(int port){
        this.port = port;
    }

    private void start(){
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        EchoServerHandler esh = new EchoServerHandler();
        try{
            ServerBootstrap sb = new ServerBootstrap();
            sb.group(workerGroup).channel(NioServerSocketChannel.class)
                    .localAddress(new InetSocketAddress(port))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        public void initChannel(SocketChannel ch){
                            ch.pipeline().addLast(esh);
                        }
                    });
            ChannelFuture sync = sb.bind().sync();
            sync.channel().closeFuture().sync();
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) {
        /*if (args.length != 1){
            System.out.println("Usage：" + EchoServer.class.getSimpleName()
                    + " <port>");
        }

        int port = Integer.parseInt(args[0]);*/
        new EchoServer(8899).start();
    }
}
