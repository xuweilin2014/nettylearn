package com.juejin.im.client.handler;

import com.juejin.im.protocol.response.QuitGroupResponsePacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class QuitGroupResponseHandler extends SimpleChannelInboundHandler<QuitGroupResponsePacket> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, QuitGroupResponsePacket packet) throws Exception {
        if (packet.isSuccess()){
            System.out.println("退出群聊【" + packet.getGrouId() + "】成功！");
        }else{
            System.out.println("退出群聊【" + packet.getGrouId() + "】失败，原因是：" + packet.getReason());
        }
    }
}
