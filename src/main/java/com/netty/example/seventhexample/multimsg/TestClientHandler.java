package com.netty.example.seventhexample.multimsg;

import com.netty.example.seventhexample.multimsg.proto.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Random;

public class TestClientHandler extends SimpleChannelInboundHandler<MyMessage> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MyMessage msg) throws Exception {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        int type = new Random().nextInt(3) + 1;

        if (type == MyMessage.MessageType.App_VALUE){
            ctx.channel().writeAndFlush(MyMessage.newBuilder().setApp(App.newBuilder()
                    .setIp("192.168.218.2").setPhoneType("MI9 Pro"))
                    .setType(MyMessage.MessageType.App)
            );
        }else if (type == MyMessage.MessageType.Pad_VALUE){
            ctx.channel().writeAndFlush(MyMessage.newBuilder().setPad(Pad.newBuilder()
                    .setIp("192.168.218.5").setPadType("Apple"))
                    .setType(MyMessage.MessageType.Pad)
            );
        }else if (type == MyMessage.MessageType.Computer_VALUE){
            ctx.channel().writeAndFlush(MyMessage.newBuilder().setComputer(Computer.newBuilder()
                    .setIp("192.168.218.7").setComputerType("HP"))
                    .setType(MyMessage.MessageType.Computer)
            );
        }
    }
}
