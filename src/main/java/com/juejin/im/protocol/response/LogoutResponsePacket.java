package com.juejin.im.protocol.response;

import com.juejin.im.protocol.Packet;
import com.juejin.im.protocol.command.Command;

public class LogoutResponsePacket extends Packet {

    private String reason;

    private boolean success;

    @Override
    public Byte getCommand() {
        return Command.LOGOUT_RESPONSE;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}
