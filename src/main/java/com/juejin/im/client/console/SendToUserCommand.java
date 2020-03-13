package com.juejin.im.client.console;

import com.juejin.im.protocol.request.MessageRequestPacket;
import io.netty.channel.Channel;

import java.util.Scanner;

public class SendToUserCommand implements ConsoleCommand {

    @Override
    public void exec(Scanner scanner, Channel channel) {
        System.out.println("发送消息给用户：");
        String toUserId = scanner.next();
        String msg = scanner.next();

        channel.writeAndFlush(new MessageRequestPacket(toUserId, msg));
    }
}
