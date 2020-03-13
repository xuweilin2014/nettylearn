package com.juejin.im.client.console;

import com.juejin.im.protocol.request.LogoutRequestPacket;
import io.netty.channel.Channel;

import java.util.Scanner;

public class LogoutConsoleCommand implements ConsoleCommand {

    @Override
    public void exec(Scanner scanner, Channel channel) {
        LogoutRequestPacket lrp = new LogoutRequestPacket();
        channel.writeAndFlush(lrp);
    }
}
