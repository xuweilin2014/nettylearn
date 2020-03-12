package com.juejin.im.client.handler;

import com.juejin.im.protocol.request.LoginRequestPacket;
import com.juejin.im.protocol.response.LoginResponsePacket;
import com.juejin.im.session.Session;
import com.juejin.im.util.SessionUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Date;
import java.util.UUID;

public class LoginResponseHandler extends SimpleChannelInboundHandler<LoginResponsePacket> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, LoginResponsePacket packet) throws Exception {
        String userId = packet.getUserId();
        String userName = packet.getUserName();

        if (packet.isSuccess()){
            SessionUtil.bindSession(new Session(userId, userName), ctx.channel());
            System.out.println(new Date() + " 【" + userName + "】登录成功，userId 为: " + packet.getUserId());
        }else{
            System.out.println(new Date() + " 【" + userName + "】登录失败，原因：" + packet.getReason());
        }
    }
}
