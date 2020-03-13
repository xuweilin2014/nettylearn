package com.juejin.im.protocol.request;

import com.juejin.im.protocol.Packet;
import com.juejin.im.protocol.command.Command;

public class JoinGroupRequestPacket extends Packet {

    private String groupId;

    @Override
    public Byte getCommand() {
        return Command.JOIN_GROUP_REQUEST;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }
}
