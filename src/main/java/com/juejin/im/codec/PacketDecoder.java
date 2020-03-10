package com.juejin.im.codec;

import com.juejin.im.protocol.PacketCodeC;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * PacketDecoder是一个InBound类型的ChannelHandler
 */
public class PacketDecoder extends ByteToMessageDecoder {

    /**
     * 将二进制ByteBuf阶码为java对象
     * @param in 需要解码的二进制ByteBuf对象
     * @param out 往这个list添加解码后的对象就会自动将结果往下一个handler传递
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        out.add(PacketCodeC.INSTANCE.decode(in));
    }
}
