package com.juejin.im.server.handler;

import com.juejin.im.util.LoginUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class AuthHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!LoginUtil.hasLogin(ctx.channel())){
            // 如果客户端与服务器端的连接没有经过权限认证，则关闭此连接
            ctx.channel().close();
        }else{
            // 如果客户端的连接经过权限认证，则对此 Channel 而言，
            // 移除 AuthHandler，以后此 channel 上的消息就不用经过 AuthHandler
            ctx.pipeline().remove(this);
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        if (LoginUtil.hasLogin(ctx.channel())){
            System.out.println("当前连接登录验证完毕，无需再次验证, AuthHandler 被移除");
        }else{
            System.out.println("无登录验证，强制关闭连接!");
        }
    }

}
