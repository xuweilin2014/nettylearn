package com.juejin.im.client.console;

import com.juejin.im.protocol.request.LoginRequestPacket;
import io.netty.channel.Channel;

import java.util.Scanner;

public class LoginConsoleCommand implements ConsoleCommand{
    @Override
    public void exec(Scanner scanner, Channel channel) {
        LoginRequestPacket lrp = new LoginRequestPacket();

        System.out.println("请输入用户名登录：");
        lrp.setUsername(scanner.next());
        lrp.setPassword("pwd");

        channel.writeAndFlush(lrp);
        waitForLoginResponse();
    }

    private static void waitForLoginResponse() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
    }
}
