package com.juejin.im.server.handler;

import com.juejin.im.protocol.request.MessageRequestPacket;
import com.juejin.im.protocol.response.MessageResponsePacket;
import com.juejin.im.session.Session;
import com.juejin.im.util.SessionUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Date;

public class MessageRequestHandler extends SimpleChannelInboundHandler<MessageRequestPacket> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MessageRequestPacket packet) throws Exception {
        //1.拿到消息发送方的会话消息
        Session session = SessionUtil.getSession(ctx.channel());

        //2.通过消息发送方的会话构造要发送的消息
        MessageResponsePacket mrp = new MessageResponsePacket();
        mrp.setMessage(packet.getMessage());
        mrp.setVersion(packet.getVersion());
        mrp.setFromUserId(session.getUserId());
        mrp.setFromUserName(session.getUserName());

        //3.拿到消息接收方的channel
        Channel toUserChannel = SessionUtil.getChannel(packet.getToUserId());

        //4.判断消息接收方的channel是否存在，以及消息接收方是否登录
        if (toUserChannel != null && SessionUtil.hasLogin(toUserChannel)){
            toUserChannel.writeAndFlush(mrp);
        }else{
            System.err.println("【" + packet.getToUserId() + "】 不在线，发送失败!");
        }
    }
}
