package com.juejin.im.client.handler;

import com.juejin.im.protocol.response.LogoutResponsePacket;
import com.juejin.im.util.SessionUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class LogoutResponseHandler extends SimpleChannelInboundHandler<LogoutResponsePacket> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, LogoutResponsePacket packet) throws Exception {
        SessionUtil.unBindSession(ctx.channel());
    }
}
