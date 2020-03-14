package com.juejin.im.server.handler;

import com.juejin.im.protocol.request.GroupMessageRequestPacket;
import com.juejin.im.protocol.response.GroupMessageResponsePacket;
import com.juejin.im.util.SessionUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;

@ChannelHandler.Sharable
public class GroupMessageRequestHandler extends SimpleChannelInboundHandler<GroupMessageRequestPacket> {

    public static final GroupMessageRequestHandler INSTANCE = new GroupMessageRequestHandler();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, GroupMessageRequestPacket packet) throws Exception {
        // 1.拿到群聊 groupId，构造群聊消息响应
        String groupId = packet.getGroupId();
        GroupMessageResponsePacket gmrp = new GroupMessageResponsePacket();
        gmrp.setFromGroupId(groupId);
        gmrp.setFromUser(SessionUtil.getSession(ctx.channel()));
        gmrp.setMsg(packet.getMsg());

        // 2.拿到群聊对应的 channelGroup，将消息写到每个客户端
        ChannelGroup channelGroup = SessionUtil.getChannelGroup(groupId);
        channelGroup.writeAndFlush(gmrp);
    }
}
