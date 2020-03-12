package com.juejin.im.server.handler;

import com.juejin.im.protocol.Packet;
import com.juejin.im.protocol.request.LoginRequestPacket;
import com.juejin.im.protocol.response.LoginResponsePacket;
import com.juejin.im.util.LoginUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Date;

public class LoginRequestHandler extends SimpleChannelInboundHandler<LoginRequestPacket> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, LoginRequestPacket packet) throws Exception {
        ctx.channel().writeAndFlush(login(packet, ctx));
    }

    private Packet login(LoginRequestPacket packet, ChannelHandlerContext ctx){
        System.out.println(new Date() + ": 收到客户端登录请求……");

        LoginResponsePacket lrp = new LoginResponsePacket();
        lrp.setVersion(packet.getVersion());
        if (valid(packet)){
            //登录成功
            lrp.setSuccess(true);
            System.out.println(new Date() + "：" + packet.getUsername() + " 登录成功");
            LoginUtil.markAsLogin(ctx.channel());
        }else{
            //登录失败
            lrp.setSuccess(false);
            lrp.setReason("密码和用户名不正确");
            System.out.println(new Date() + "：" + packet.getUsername() + " 登录失败");
        }

        return lrp;
    }

    private boolean valid(LoginRequestPacket packet) {
        return true;
    }
}
