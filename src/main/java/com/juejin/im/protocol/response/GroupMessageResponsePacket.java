package com.juejin.im.protocol.response;

import com.juejin.im.protocol.Packet;
import com.juejin.im.protocol.command.Command;
import com.juejin.im.session.Session;

public class GroupMessageResponsePacket extends Packet {

    private String fromGroupId;

    private String msg;

    private Session fromUser;

    @Override
    public Byte getCommand() {
        return Command.GROUP_MESSAGE_RESPONSE;
    }

    public String getFromGroupId() {
        return fromGroupId;
    }

    public void setFromGroupId(String fromGroupId) {
        this.fromGroupId = fromGroupId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Session getFromUser() {
        return fromUser;
    }

    public void setFromUser(Session fromUser) {
        this.fromUser = fromUser;
    }
}
