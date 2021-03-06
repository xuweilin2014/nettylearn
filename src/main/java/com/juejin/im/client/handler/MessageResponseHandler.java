package com.juejin.im.client.handler;

import com.juejin.im.protocol.response.MessageResponsePacket;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class MessageResponseHandler extends SimpleChannelInboundHandler<MessageResponsePacket> {

    public static final MessageResponseHandler INSTANCE = new MessageResponseHandler();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MessageResponsePacket packet) throws Exception {
        String fromUserId = packet.getFromUserId();
        String fromUserName = packet.getFromUserName();
        System.out.println(fromUserId + ":" + fromUserName + " -> " + packet
                .getMessage());
    }
}
