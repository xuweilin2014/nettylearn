package com.juejin.im.server.handler;

import com.juejin.im.protocol.request.QuitGroupRequestPacket;
import com.juejin.im.protocol.response.QuitGroupResponsePacket;
import com.juejin.im.util.SessionUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;

public class QuitGroupRequestHandler extends SimpleChannelInboundHandler<QuitGroupRequestPacket> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, QuitGroupRequestPacket packet) throws Exception {
        // 1.获取群对应的 channelGroup，然后将当前用户的 channel 从 channelGroup 中移除
        String groupId = packet.getGroupId();
        ChannelGroup channelGroup = SessionUtil.getChannelGroup(groupId);
        channelGroup.remove(ctx.channel());

        // 2.创建退出群聊的响应消息
        QuitGroupResponsePacket qgrp = new QuitGroupResponsePacket();
        qgrp.setGrouId(groupId);
        qgrp.setSuccess(true);

        // 3.将响应消息发送给客户端
        ctx.channel().writeAndFlush(qgrp);
    }
}
