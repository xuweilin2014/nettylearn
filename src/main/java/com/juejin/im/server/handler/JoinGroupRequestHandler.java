package com.juejin.im.server.handler;

import com.juejin.im.protocol.request.JoinGroupRequestPacket;
import com.juejin.im.protocol.response.JoinGroupResponsePacket;
import com.juejin.im.util.SessionUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;

@ChannelHandler.Sharable
public class JoinGroupRequestHandler extends SimpleChannelInboundHandler<JoinGroupRequestPacket> {

    public static final JoinGroupRequestHandler INSTANCE = new JoinGroupRequestHandler();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, JoinGroupRequestPacket packet) throws Exception {
        // 1.获取群对应的 channelGroup，然后将当前用户的 channel 添加到其中
        String groupId = packet.getGroupId();
        ChannelGroup channelGroup = SessionUtil.getChannelGroup(groupId);
        channelGroup.add(ctx.channel());

        // 2.创建加入群聊的响应信息
        JoinGroupResponsePacket joinGroupResponsePacket = new JoinGroupResponsePacket();
        joinGroupResponsePacket.setSuccess(true);
        joinGroupResponsePacket.setGroupId(groupId);

        // 3.将响应信息发送给客户端
        ctx.writeAndFlush(joinGroupResponsePacket);
    }

}
