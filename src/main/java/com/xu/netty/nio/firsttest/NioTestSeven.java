package com.xu.netty.nio.firsttest;


import java.nio.ByteBuffer;

/**
 * 只读Buffer
 */
public class NioTestSeven {
    public static void main(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(10);

        System.out.println(buffer.getClass());

        for (int i = 0; i < buffer.capacity(); i++){
            buffer.put((byte)i);
        }

        /*
         * 创建一个新的、只读的Buffer，这个新的buffer和原来的buffer共享相同的内存
         * 对于原有buffer中内容的改变，都会影响到新的buffer，但是新的buffer是只读的
         * 无法对其中的内容进行修改。另外，新的buffer和旧的buffer中的position、limit和capacityd的值都是互相独立的
         */
        ByteBuffer rbuffer = buffer.asReadOnlyBuffer();
        System.out.println(rbuffer.getClass());
    }
}
