package com.juejin.im.server.handler;

import com.juejin.im.protocol.request.MessageRequestPacket;
import com.juejin.im.protocol.response.MessageResponsePacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Date;

public class MessageRequestHandler extends SimpleChannelInboundHandler<MessageRequestPacket> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MessageRequestPacket packet) throws Exception {
        ctx.channel().writeAndFlush(receiveMsg(packet));
    }

    private MessageResponsePacket receiveMsg(MessageRequestPacket packet) {
        System.out.println(new Date() + "：收到客户端消息 " + packet.getMessage());
        MessageResponsePacket messageResponsePacket = new MessageResponsePacket();
        messageResponsePacket.setMessage("服务端回复【" + packet.getMessage() + "】");

        return messageResponsePacket;
    }
}
