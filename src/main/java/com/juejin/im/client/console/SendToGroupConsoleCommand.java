package com.juejin.im.client.console;

import com.juejin.im.protocol.request.GroupMessageRequestPacket;
import io.netty.channel.Channel;

import java.util.Scanner;

public class SendToGroupConsoleCommand implements ConsoleCommand {

    @Override
    public void exec(Scanner scanner, Channel channel) {
        System.out.println("发送消息给群组：");

        GroupMessageRequestPacket gmrp = new GroupMessageRequestPacket();
        gmrp.setGroupId(scanner.next());
        gmrp.setMsg(scanner.next());

        channel.writeAndFlush(gmrp);
    }
}
