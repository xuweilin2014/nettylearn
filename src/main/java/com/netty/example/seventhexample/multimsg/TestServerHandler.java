package com.netty.example.seventhexample.multimsg;

import com.netty.example.seventhexample.multimsg.proto.App;
import com.netty.example.seventhexample.multimsg.proto.Computer;
import com.netty.example.seventhexample.multimsg.proto.MyMessage;
import com.netty.example.seventhexample.multimsg.proto.Pad;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class TestServerHandler extends SimpleChannelInboundHandler<MyMessage> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MyMessage msg) throws Exception {
        MyMessage.MessageType type = msg.getType();

        if (type == MyMessage.MessageType.App){
            App app = msg.getApp();
            System.out.println(app.getIp());
            System.out.println(app.getPhoneType());
        }else if (type == MyMessage.MessageType.Pad){
            Pad pad = msg.getPad();
            System.out.println(pad.getIp());
            System.out.println(pad.getPadType());
        }else if (type == MyMessage.MessageType.Computer){
            Computer computer = msg.getComputer();
            System.out.println(computer.getIp());
            System.out.println(computer.getComputerType());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

    }
}
