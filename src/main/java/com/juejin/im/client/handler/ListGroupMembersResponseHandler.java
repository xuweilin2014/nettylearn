package com.juejin.im.client.handler;

import com.juejin.im.protocol.response.ListGroupMembersResponsePacket;
import com.juejin.im.server.handler.ListGroupMembersRequestHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class ListGroupMembersResponseHandler extends SimpleChannelInboundHandler<ListGroupMembersResponsePacket> {

    public static final ListGroupMembersResponseHandler INSTANCE = new ListGroupMembersResponseHandler();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ListGroupMembersResponsePacket packet) throws Exception {
        System.out.println("群【" + packet.getGroupId() + "】中的人包括：" + packet.getSessionList());
    }
}
