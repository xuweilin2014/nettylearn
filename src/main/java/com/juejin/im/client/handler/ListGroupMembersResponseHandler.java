package com.juejin.im.client.handler;

import com.juejin.im.protocol.response.ListGroupMembersResponsePacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class ListGroupMembersResponseHandler extends SimpleChannelInboundHandler<ListGroupMembersResponsePacket> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ListGroupMembersResponsePacket packet) throws Exception {
        System.out.println("群【" + packet.getGroupId() + "】中的人包括：" + packet.getSessionList());
    }
}
