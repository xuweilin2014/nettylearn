package com.juejin.im.client.handler;

import com.juejin.im.protocol.PacketCodeC;
import com.juejin.im.protocol.request.LoginRequestPacket;
import com.juejin.im.protocol.response.LoginResponsePacket;
import com.juejin.im.util.LoginUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Date;
import java.util.UUID;

public class LoginResponseHandler extends SimpleChannelInboundHandler<LoginResponsePacket> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, LoginResponsePacket packet) throws Exception {
        if (packet.isSuccess()){
            LoginUtil.markAsLogin(ctx.channel());
            System.out.println(new Date() + "：客户端登录成功");
        }else{
            System.out.println(new Date() + "：客户端登录失败，原因："
                    + packet.getReason());
        }
    }

    /**
     * 在客户端连接上服务器端后，会回调NioSocketChannel的pipeline中所有channeActive方法。
     * 我们在channelActive中实现的逻辑是：在客户端连接上服务器端后，立即登录
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println(new Date() + "：客户端开始登陆");

        //1.创建登陆对象
        LoginRequestPacket packet = new LoginRequestPacket();
        packet.setUserId(UUID.randomUUID().toString());
        packet.setUsername("zuko");
        packet.setPassword("123456");

        //2.写数据到服务器端，不过还需要经过pipeline上PacketDecoder处理将其
        //编码为二进制ByteBuf
        ctx.channel().writeAndFlush(packet);
    }
}
