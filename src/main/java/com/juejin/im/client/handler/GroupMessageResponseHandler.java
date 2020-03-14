package com.juejin.im.client.handler;

import com.juejin.im.protocol.response.GroupMessageResponsePacket;
import com.juejin.im.session.Session;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class GroupMessageResponseHandler extends SimpleChannelInboundHandler<GroupMessageResponsePacket> {

    public static final GroupMessageResponseHandler INSTANCE = new GroupMessageResponseHandler();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, GroupMessageResponsePacket packet) throws Exception {
        String groupId = packet.getFromGroupId();
        Session fromUser = packet.getFromUser();
        System.out.println("收到群【" + groupId + "】中【" + fromUser + "】发来的消息：" + packet.getMsg());
    }

}
