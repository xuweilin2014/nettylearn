package com.juejin.im.protocol.request;

import com.juejin.im.protocol.Packet;
import com.juejin.im.protocol.command.Command;

public class HeartBeatRequestPacket extends Packet {
    @Override
    public Byte getCommand() {
        return Command.HEART_BEAT_REQUEST;
    }
}
