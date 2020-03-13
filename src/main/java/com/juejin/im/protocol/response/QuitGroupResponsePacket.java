package com.juejin.im.protocol.response;

import com.juejin.im.protocol.Packet;
import com.juejin.im.protocol.command.Command;

public class QuitGroupResponsePacket extends Packet {

    private String grouId;

    private boolean success;

    private String reason;

    @Override
    public Byte getCommand() {
        return Command.QUIT_GROUP_RESPONSE;
    }

    public String getGrouId() {
        return grouId;
    }

    public void setGrouId(String grouId) {
        this.grouId = grouId;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }
}
