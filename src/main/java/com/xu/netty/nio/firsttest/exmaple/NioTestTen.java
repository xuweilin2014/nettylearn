package com.xu.netty.nio.firsttest.exmaple;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * Buffer中的scattering和gathering
 */
public class NioTestTen {
    public static void main(String[] args) throws IOException {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        InetSocketAddress address = new InetSocketAddress(8899);
        serverSocketChannel.socket().bind(address);

        int messageLength = 2 + 3 + 4;

        ByteBuffer[] buffers = new ByteBuffer[3];
        buffers[0] = ByteBuffer.allocate(2);
        buffers[1] = ByteBuffer.allocate(3);
        buffers[2] = ByteBuffer.allocate(4);

        SocketChannel socketChannel = serverSocketChannel.accept();

        while (true){
            int bytesRead = 0;

            while (bytesRead < messageLength){
                long r = socketChannel.read(buffers);
                bytesRead += r;

                System.out.println("bytesRead：" + bytesRead);

                for (int i = 0; i < buffers.length; i++){
                    System.out.println("buffer " + i + " position：" +
                            buffers[i].position() + " , limit：" + buffers[i].limit());
                }
            }

            for (ByteBuffer buffer : buffers) {
                buffer.flip();
            }

            int bytesWritten = 0;

            while (bytesWritten < messageLength){
                long r = socketChannel.write(buffers);

                bytesWritten += r;

            }

            System.out.println("bytesRead：" + bytesRead + " , bytesWritten：" + bytesWritten + "messageLength：" + messageLength);
            for (ByteBuffer buffer : buffers) {
                buffer.clear();
            }
        }
    }

}


