package com.juejin.im.protocol.request;

import com.juejin.im.protocol.Packet;
import com.juejin.im.protocol.command.Command;

public class GroupMessageRequestPacket extends Packet {

    private String groupId;

    private String msg;

    @Override
    public Byte getCommand() {
        return Command.GROUP_MESSAGE_REQUEST;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
