package com.juejin.im.protocol.response;

import com.juejin.im.protocol.Packet;
import com.juejin.im.protocol.command.Command;

public class HeartBeatResponsePacket extends Packet {
    @Override
    public Byte getCommand() {
        return Command.HEART_BEAT_RESPONSE;
    }
}
