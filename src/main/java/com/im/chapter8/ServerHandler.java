package com.im.chapter8;

import com.sun.beans.editors.ByteEditor;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Date;

public class ServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf) msg;

        Packet packet = PacketCodeC.INSTANCE.decode(byteBuf);

        if (packet instanceof LoginRequestPacket){
            LoginRequestPacket loginRequestPacket = (LoginRequestPacket) packet;

            LoginResponsePacket loginResponsePacket = new LoginResponsePacket();
            loginResponsePacket.setVersion(packet.getVersion());
            if (valid(loginRequestPacket)){
                //登录成功
                loginResponsePacket.setSuccess(true);
                System.out.println(new Date() + "：" + loginRequestPacket.getUsername() + " 登录成功");
            }else{
                //登录失败
                loginResponsePacket.setSuccess(false);
                loginResponsePacket.setReason("密码和用户名不正确");
                System.out.println(new Date() + "：" + loginRequestPacket.getUsername() + " 登录失败");
            }

            //登录响应
            ByteBuf buf = PacketCodeC.INSTANCE.encode(ctx.alloc(), loginResponsePacket);
            ctx.channel().writeAndFlush(buf);
        }
    }

    private boolean valid(LoginRequestPacket loginRequestPacket) {
        return true;
    }
}
