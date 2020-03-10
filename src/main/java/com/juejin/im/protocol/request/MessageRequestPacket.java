package com.juejin.im.protocol.request;

import com.juejin.im.protocol.Packet;
import com.juejin.im.protocol.command.Command;

/**
 * 客户端发送至服务器端的消息对象
 */
public class MessageRequestPacket extends Packet {

    private String message;

    @Override
    public Byte getCommand() {
        return Command.MESSAGE_REQUEST;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
