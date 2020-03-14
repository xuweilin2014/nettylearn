package com.juejin.im.server.handler;

import com.juejin.im.protocol.request.HeartBeatRequestPacket;
import com.juejin.im.protocol.response.HeartBeatResponsePacket;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class HeartBeatRequestHandler extends SimpleChannelInboundHandler<HeartBeatRequestPacket> {

    public static final HeartBeatRequestHandler INSTANCE = new HeartBeatRequestHandler();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HeartBeatRequestPacket packet) throws Exception {
        ctx.channel().writeAndFlush(new HeartBeatResponsePacket());
    }
}
