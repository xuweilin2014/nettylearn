package com.juejin.im.client.handler;

import com.juejin.im.protocol.response.JoinGroupResponsePacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class JoinGroupResponseHandler extends SimpleChannelInboundHandler<JoinGroupResponsePacket> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, JoinGroupResponsePacket packet) throws Exception {
        if (packet.isSuccess()){
            System.out.println("加入群【" + packet.getGroupId() + "】成功！");
        }else{
            System.err.println("加入群【" + packet.getGroupId() + "】失败，原因是：" + packet.getReason());
        }
    }

}
