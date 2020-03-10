package com.juejin.im.protocol.response;

import com.juejin.im.protocol.Packet;
import com.juejin.im.protocol.command.Command;

/**
 * 服务端发送至客户端的消息对象
 */
public class MessageResponsePacket extends Packet {

    private String message;

    @Override
    public Byte getCommand() {
        return Command.MESSAGE_RESPONSE;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
