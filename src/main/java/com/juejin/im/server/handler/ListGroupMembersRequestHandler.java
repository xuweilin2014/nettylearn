package com.juejin.im.server.handler;

import com.juejin.im.protocol.request.ListGroupMembersRequestPacket;
import com.juejin.im.protocol.response.ListGroupMembersResponsePacket;
import com.juejin.im.session.Session;
import com.juejin.im.util.SessionUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;

import java.util.ArrayList;
import java.util.List;

@ChannelHandler.Sharable
public class ListGroupMembersRequestHandler extends SimpleChannelInboundHandler<ListGroupMembersRequestPacket> {

    public static final ListGroupMembersRequestHandler INSTANCE = new ListGroupMembersRequestHandler();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ListGroupMembersRequestPacket packet) throws Exception {
        // 1.获取群的 channelGroup
        String groupId = packet.getGroupId();
        ChannelGroup channelGroup = SessionUtil.getChannelGroup(groupId);

        // 2.遍历群成员的 channel，找到和 channel 对应的 session
        List<Session> sessions = new ArrayList<>();
        for (Channel channel : channelGroup) {
            Session session = SessionUtil.getSession(channel);
            sessions.add(session);
        }

        // 3.构建获取成员列表响应写回到客户端
        ListGroupMembersResponsePacket lgmrp = new ListGroupMembersResponsePacket();
        lgmrp.setSessionList(sessions);
        lgmrp.setGroupId(groupId);
        ctx.writeAndFlush(lgmrp);
    }
}
