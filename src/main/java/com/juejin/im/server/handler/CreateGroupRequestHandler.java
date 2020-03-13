package com.juejin.im.server.handler;

import com.juejin.im.protocol.request.CreateGroupRequestPacket;
import com.juejin.im.protocol.response.CreateGroupResponsePacket;
import com.juejin.im.session.Session;
import com.juejin.im.util.IDUtil;
import com.juejin.im.util.SessionUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;

import java.util.ArrayList;
import java.util.List;

public class CreateGroupRequestHandler extends SimpleChannelInboundHandler<CreateGroupRequestPacket> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CreateGroupRequestPacket packet) throws Exception {
        List<String> userIdList = packet.getUserIdList();
        List<String> userNameList = new ArrayList<>();

        // 1.创建一个用于群聊的channelGroup
        ChannelGroup channelGroup = new DefaultChannelGroup(ctx.executor());

        // 2.根据用户id，来将获取到的channel保存到channelGroup中，
        // 同时将和此channel相关的用户名userName保存到
        for (String id : userIdList) {
            Channel channel = SessionUtil.getChannel(id);
            if (channel != null){
                channelGroup.add(channel);
                userNameList.add(SessionUtil.getSession(channel).getUserName());
            }
        }

        // 3.创建群聊创建结果的响应
        CreateGroupResponsePacket crp = new CreateGroupResponsePacket();
        crp.setGroupId(IDUtil.randomId());
        crp.setSuccess(true);
        crp.setUserNameList(userNameList);

        // 4.给每个群聊客户端发送通知
        channelGroup.writeAndFlush(crp);

        System.out.print("群创建成功，id 为【" + crp.getGroupId() + "】， ");
        System.out.println("群里面有：" + crp.getUserNameList());
    }

}
