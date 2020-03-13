package com.juejin.im.client.console;

import com.juejin.im.protocol.request.ListGroupMembersRequestPacket;
import io.netty.channel.Channel;

import java.util.Scanner;

public class ListGroupMembersConsoleCommand implements ConsoleCommand {
    @Override
    public void exec(Scanner scanner, Channel channel) {
        ListGroupMembersRequestPacket lgmrp = new ListGroupMembersRequestPacket();

        System.out.println("输入 groupId，获取群成员列表：");
        String groupId = scanner.next();
        lgmrp.setGroupId(groupId);

        channel.writeAndFlush(lgmrp);
    }
}
