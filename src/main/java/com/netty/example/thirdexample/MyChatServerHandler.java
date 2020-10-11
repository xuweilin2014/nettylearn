package com.netty.example.thirdexample;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

/**
 * 需求：
 * 1. A、B、C三个客户端与服务器端分别建好连接，如果A先建立好连接，那么服务器在控制台打印A已经上线了，
 * 如果B和服务器也建立好连接了，那么服务器不仅会在控制台打印B已经上线，同时会告诉A，B已经上线，
 * 如果C和服务器也建立好连接了，那么服务器不仅会在控制台打印C已经上线，同时会告诉A和B，C已经上线
 *
 * 2.如果A如果发出一条消息，那么A、B、C都会接收到A发出的消息，不过对于A来说，它会显示是自己发的消息
 */

public class MyChatServerHandler extends SimpleChannelInboundHandler<String> {

    private static ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        Channel channel = ctx.channel();

        channelGroup.forEach(ch -> {
            if (channel != ch){
                ch.writeAndFlush(channel.remoteAddress() + " 发送的消息：" + msg + "\n");
            }else{
                ch.writeAndFlush("【自己】" + msg + "\n");
            }
        });
    }

    /**
     * 回调函数handlerAdded表示客户端与服务器端的TCP连接已经建立好了
     */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        channelGroup.writeAndFlush("【服务器】- " + channel.remoteAddress() + " 加入\n");
        channelGroup.add(channel);
    }

    /**
     * 回调函数handlerRemoved表示客户端与服务器端的TCP连接已经断开了
     * 注意：当连接断开之后，Netty会自动的去channelGroup中移除掉断开连接的那个channel
     */
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        channelGroup.writeAndFlush("【服务器】- " + channel.remoteAddress() + " 离开\n");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        System.out.println(channel.remoteAddress() + "上线");
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        System.out.println(channel.remoteAddress() + "下线");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        //cause.printStackTrace();
        ctx.close();
    }
}
