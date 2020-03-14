package com.juejin.im.client.console;

import com.juejin.im.util.SessionUtil;
import io.netty.channel.Channel;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class ConsoleCommandManager implements ConsoleCommand {

    private Map<String, ConsoleCommand> consoleCommandMap = new HashMap<>();

    public ConsoleCommandManager() {
        consoleCommandMap.put("sendToUser", new SendToUserCommand());
        consoleCommandMap.put("sendToGroup", new SendToGroupConsoleCommand());
        consoleCommandMap.put("createGroup", new CreateGroupConsoleCommand());
        consoleCommandMap.put("logout", new LogoutConsoleCommand());
        consoleCommandMap.put("joinGroup", new JoinGroupConsoleCommand());
        consoleCommandMap.put("quitGroup", new QuitGroupConsoleCommand());
        consoleCommandMap.put("listGroup", new ListGroupMembersConsoleCommand());
    }

    @Override
    public void exec(Scanner scanner, Channel channel) {
        String command = scanner.next();

        if (!SessionUtil.hasLogin(channel)) {
            return;
        }

        ConsoleCommand consoleCommand = consoleCommandMap.get(command);

        if (consoleCommand != null){
            consoleCommand.exec(scanner, channel);
        }else{
            System.err.println("无法识别【" + command + "】指令，请重新输入!");
        }
    }
}
