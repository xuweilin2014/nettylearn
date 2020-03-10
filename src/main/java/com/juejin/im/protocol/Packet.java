package com.juejin.im.protocol;

public abstract class Packet {

    /**
     * 协议版本
     */
    private Byte version = 1;

    /**
     * 返回数据包指令
     * 获取指令的抽象方法，所有的指令数据包都必须实现这个方法，这样我们就可以知道某种指令的含义。
     */
    public abstract Byte getCommand();

    public Byte getVersion() {
        return version;
    }

    public void setVersion(Byte version) {
        this.version = version;
    }
}
