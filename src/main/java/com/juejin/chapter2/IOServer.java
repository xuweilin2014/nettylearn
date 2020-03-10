package com.juejin.chapter2;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class IOServer {

    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(8000);

        new Thread(() -> {
            while (true){
                try {
                    Socket socket = serverSocket.accept();

                    System.out.println(socket.getInetAddress() + " 已连接");

                    new Thread(() -> {
                        try {
                            InputStream inputStream = socket.getInputStream();
                            byte[] data = new byte[1024];
                            int len = 0;
                            while ((len = inputStream.read(data)) != -1){
                                System.out.println(new String(data, 0, len));
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }).start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

}
