package com.xu.netty.zerocopy;

import java.io.*;
import java.net.Socket;

public class OldClient {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("localhost",8899);

        String filename = "ideaIU-2019.2.2.exe";
        InputStream is = new FileInputStream(filename);
        OutputStream os = new BufferedOutputStream(socket.getOutputStream());
        byte[] buffer = new byte[4096];
        int sendCount = 0;
        long total = 0;
        long startTime = System.currentTimeMillis();

        while ((sendCount = is.read(buffer)) != -1){
            total += sendCount;
            os.write(buffer);
        }

        System.out.println("总共发送字节：" + total + ", 耗时：" + (System.currentTimeMillis() - startTime));
    }
}
