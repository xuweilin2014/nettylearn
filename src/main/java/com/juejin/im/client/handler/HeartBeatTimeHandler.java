package com.juejin.im.client.handler;

import com.juejin.im.protocol.request.HeartBeatRequestPacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.TimeUnit;

public class HeartBeatTimeHandler extends ChannelInboundHandlerAdapter {

    public static final int HEARTBEAT_INTERVAL = 5;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        scheduleSendHeartBeat(ctx);
        super.channelActive(ctx);
    }

    public void scheduleSendHeartBeat(ChannelHandlerContext ctx){
        ctx.executor().schedule(new Runnable() {
            @Override
            public void run() {
                if (ctx.channel().isActive()){
                    ctx.writeAndFlush(new HeartBeatRequestPacket());
                    scheduleSendHeartBeat(ctx);
                }
            }
        }, HEARTBEAT_INTERVAL, TimeUnit.SECONDS);
    }
}
