package com.juejin.im.client.handler;

import com.juejin.im.protocol.response.JoinGroupResponsePacket;
import com.juejin.im.server.handler.JoinGroupRequestHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class JoinGroupResponseHandler extends SimpleChannelInboundHandler<JoinGroupResponsePacket> {

    public static final JoinGroupResponseHandler INSTANCE = new JoinGroupResponseHandler();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, JoinGroupResponsePacket packet) throws Exception {
        if (packet.isSuccess()){
            System.out.println("加入群【" + packet.getGroupId() + "】成功！");
        }else{
            System.err.println("加入群【" + packet.getGroupId() + "】失败，原因是：" + packet.getReason());
        }
    }

}
