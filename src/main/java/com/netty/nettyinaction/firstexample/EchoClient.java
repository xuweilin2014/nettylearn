package com.netty.nettyinaction.firstexample;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

public class EchoClient {
    private String host;
    private int port;

    public EchoClient(String host, int port){
        this.port = port;
        this.host = host;
    }

    private void start(){
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        try{
            Bootstrap b = new Bootstrap();
            b.group(workerGroup).channel(NioSocketChannel.class).remoteAddress(new InetSocketAddress(host,port))
                    .handler(new ChannelInitializer<SocketChannel>() {
                        public void initChannel(SocketChannel ch){
                            ch.pipeline().addLast(new EchoClientHandler());
                        }
                    });
            ChannelFuture sync = b.connect().sync();
            sync.channel().closeFuture().sync();
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) {
        /*if (args.length != 2){
            System.out.println("Usage：" + EchoClient.class.getSimpleName()
                    + " <host> <port>");
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);*/
        new EchoClient("localhost",8899).start();
    }
}
