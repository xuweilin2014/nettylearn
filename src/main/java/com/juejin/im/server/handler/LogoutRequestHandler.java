package com.juejin.im.server.handler;

import com.juejin.im.protocol.request.LogoutRequestPacket;
import com.juejin.im.protocol.response.LoginResponsePacket;
import com.juejin.im.protocol.response.LogoutResponsePacket;
import com.juejin.im.util.SessionUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * 登录请求
 */
@ChannelHandler.Sharable
public class LogoutRequestHandler extends SimpleChannelInboundHandler<LogoutRequestPacket> {

    public static final LogoutRequestHandler INSTANCE = new LogoutRequestHandler();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, LogoutRequestPacket packet) throws Exception {
        SessionUtil.unBindSession(ctx.channel());
        LogoutResponsePacket lrp = new LogoutResponsePacket();
        lrp.setSuccess(true);
        ctx.writeAndFlush(lrp);
    }
}
