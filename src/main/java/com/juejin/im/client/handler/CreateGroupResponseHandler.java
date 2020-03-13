package com.juejin.im.client.handler;

import com.juejin.im.protocol.response.CreateGroupResponsePacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class CreateGroupResponseHandler extends SimpleChannelInboundHandler<CreateGroupResponsePacket> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CreateGroupResponsePacket packet) throws Exception {
        System.out.print("群聊创建成功，群聊id为【" + packet.getGroupId() + "】，");
        System.out.println("群里面有【" + packet.getUserNameList() + "】");
    }

}
