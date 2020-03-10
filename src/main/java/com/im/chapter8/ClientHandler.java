package com.im.chapter8;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Date;
import java.util.UUID;

public class ClientHandler extends ChannelInboundHandlerAdapter {

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

        //2.对登录对象进行编码
        ByteBuf byteBuf = PacketCodeC.INSTANCE.encode(ctx.alloc(), packet);

        //3.写数据到服务器端
        ctx.channel().writeAndFlush(byteBuf);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf) msg;

        Packet packet = PacketCodeC.INSTANCE.decode(byteBuf);

        if (packet instanceof LoginResponsePacket){
            LoginResponsePacket loginResponsePacket = (LoginResponsePacket) packet;

            if (loginResponsePacket.isSuccess()){
                System.out.println(new Date() + "：客户端登录成功");
            }else{
                System.out.println(new Date() + "：客户端登录失败，原因："
                        + loginResponsePacket.getReason());
            }
        }
    }
}
