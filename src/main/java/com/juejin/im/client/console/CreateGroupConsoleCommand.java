package com.juejin.im.client.console;

import com.juejin.im.protocol.request.CreateGroupRequestPacket;
import com.juejin.im.server.handler.CreateGroupRequestHandler;
import io.netty.channel.Channel;

import java.util.Arrays;
import java.util.Scanner;

public class CreateGroupConsoleCommand implements ConsoleCommand {

    public static final String USER_ID_SPLITER = ",";

    @Override
    public void exec(Scanner scanner, Channel channel) {
        CreateGroupRequestPacket cgrp = new CreateGroupRequestPacket();

        System.out.println("【拉人群聊】输入 userId 列表，userId 之间英文逗号隔开：");
        String userIds = scanner.next();
        cgrp.setUserIdList(Arrays.asList(userIds.split(USER_ID_SPLITER)));
        channel.writeAndFlush(cgrp);
    }
}
