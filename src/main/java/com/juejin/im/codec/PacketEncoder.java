package com.juejin.im.codec;

import com.juejin.im.protocol.Packet;
import com.juejin.im.protocol.PacketCodeC;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * PacketEncoder是一个OutBound类型的ChannelHandler
 */
public class PacketEncoder extends MessageToByteEncoder<Packet> {

    /**
     * 将java对象转化为二进制ByteBuf
     * @param packet 其它handler组装好的响应对象
     * @param out  编码后的二进制
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, Packet packet, ByteBuf out) throws Exception {
        PacketCodeC.INSTANCE.encode(out, packet);
    }
}
