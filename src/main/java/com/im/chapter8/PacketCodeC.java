package com.im.chapter8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.HashMap;
import java.util.Map;

public class PacketCodeC {

    private static final int MAGIC_NUMBER = 0x12345678;
    private static final Map<Byte, Class<? extends Packet>> PACKET_TYPE;
    private static final Map<Byte, Serializer> SERIALIZER_TYPE;

    static {
        PACKET_TYPE = new HashMap<>();
        PACKET_TYPE.put(Command.LOGIN_REQUEST, LoginRequestPacket.class);

        SERIALIZER_TYPE = new HashMap<>();
        SERIALIZER_TYPE.put(SerializerAlgorithm.JSON, new JSONSerializer());
    }

    public ByteBuf encode(Packet packet){
        //1.创建ByteBuf对象，用来作为二进制传递的数据包
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.ioBuffer();
        //2.序列化过程，将Packet序列化为字节数组
        byte[] bytes = Serializer.DEFAULT.serialize(packet);

        //3.实际的编码过程
        byteBuf.writeInt(MAGIC_NUMBER);
        byteBuf.writeByte(packet.getVersion());
        byteBuf.writeByte(Serializer.DEFAULT.getSerializerAlgorithm());
        byteBuf.writeByte(packet.getCommand());
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    public Packet decode(ByteBuf byteBuf){
        //1.跳过魔数
        byteBuf.skipBytes(4);

        //2.跳过版本号
        byteBuf.skipBytes(1);

        //3.得到序列化算法
        byte serializerAlgorithm = byteBuf.readByte();
        //4.得到指令类型
        byte command = byteBuf.readByte();

        byte[] bytes = new byte[byteBuf.readInt()];
        byteBuf.readBytes(bytes);

        Class<? extends Packet> packetType = getPacketType(command);
        Serializer serializer = getSerializerType(serializerAlgorithm);

        if (packetType != null && serializer != null){
            return serializer.deserialize(packetType, bytes);
        }
        
        return null;
    }

    private Serializer getSerializerType(byte serializerAlgorithm) {
        if (SERIALIZER_TYPE.containsKey(serializerAlgorithm)) {
            return SERIALIZER_TYPE.get(serializerAlgorithm);
        }
        return null;
    }

    private Class<? extends Packet> getPacketType(byte command) {
        if (PACKET_TYPE.containsKey(command)){
            return PACKET_TYPE.get(command);
        }
        return null;
    }


}
