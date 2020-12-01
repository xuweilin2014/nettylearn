public class RocketmqRemotingAnalysis{

    /**
     * rocketmq 的 remoting 模块核心类的继承关系如下：
     * 
     *              --------------> RemotingService <-----------------
     *              |                                                |
     *       RemotingClient      NettyRemotingAbstract           RemotingServer
     *              |               |              |              |
     *             NettyRemotingClient            NettyRemotingServer
     * 
     * RemotingClient 和 RemotingServer 这两个接口继承了 RemotingService 接口。NettyRemotingAbstract 是一个抽象类，
     * 包含了很多公共数据处理，也包含了很多重要的数据结构。NettyRemotingClient 和 NettyRemotingServer 分别实现了 RemotingClient和RemotingServer, 
     * 并且都继承了 NettyRemotingAbstract 类。
     */

    public interface RemotingService {
        void start();
    
        void shutdown();
    
        void registerRPCHook(RPCHook rpcHook);
    }

    public interface RemotingServer extends RemotingService {
        // 注册 RequestCode 对应的 Processor，并且注册和这个 Processor 对应的 executor 线程池，用来执行这个 Processor 的请求
        void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);
    
        void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);
    
        int localListenPort();
    
        // 根据不同的 RequestCode 会调用不同的 Processor，并且把 Processor 执行具体的任务包装成一个 runnable task，
        // 放入到这个 Processor 一起注册的 executor 中去执行
        Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);
    
        // 同步通信
        RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis) 
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException;
    
        // 进行异步通信，无需返回 RemotingCommand
        void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback) 
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;
    
        // 进行单向通信，比如心跳包或者注册消息这样的消息类型
        void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;
    
    }

    /**
     * Rocketmq 通信协议分为 4 个部分：消息长度、序列化长度&头部长度、消息头数据、消息主体数据
     * 真正消息分为 4 个部分：
     * 1.消息长度（总长度，四个字节存储，int 类型）
     * 2.序列化长度&消息头长度（同样占用一个int类型, 第一个字节表示序列化类型, 后面三个字节表示消息头长度）
     * 3.消息头数据
     * 4.消息主体数据
     */
    public static class RemotingCommand {

        private transient byte[] body;

        public ByteBuffer encode() {
            // 1> header length size
            // length 代表的是消息的总长度，这里的 4 表示的是第二部分的长度，占用一个 int 类型
            int length = 4;
    
            // 2> header data length
            // length 加上了消息头具体数据的长度，也就是第 3 部分的长度
            byte[] headerData = this.headerEncode();
            length += headerData.length;
    
            // 3> body data length
            // length 加上了消息体中具体数据的长度，也就是第 4 部分的长度，到这里 length 等于第 2,3,4 部分的长度之和
            if (this.body != null) {
                length += this.body.length;
            }
    
            // length + 4 才等于消息的总长度
            // result 就是一个长度等于消息总长度的 ByteBuffer
            ByteBuffer result = ByteBuffer.allocate(4 + length);
    
            // length
            // 放入第 1 部分，4 个字节，表示消息总长度
            result.putInt(length);
    
            // header length
            // 放入第 2 部分，4 个字节，第 1 个字节表示序列化类型，后面 3 个字节表示消息头的长度
            result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));
    
            // header data
            // 放入具体的消息头数据
            result.put(headerData);
    
            // body data;
            // 放入具体的消息体数据
            if (this.body != null) {
                result.put(this.body);
            }
    
            result.flip();
    
            return result;
        }

        public static byte[] markProtocolType(int source, SerializeType type) {
            byte[] result = new byte[4];
    
            result[0] = type.getCode();
            result[1] = (byte) ((source >> 16) & 0xFF);
            result[2] = (byte) ((source >> 8) & 0xFF);
            result[3] = (byte) (source & 0xFF);
            return result;
        }

        private byte[] headerEncode() {
            this.makeCustomHeaderToNet();
            if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
                return RocketMQSerializable.rocketMQProtocolEncode(this);
            } else {
                return RemotingSerializable.encode(this);
            }
        }

        public static RemotingCommand decode(final byte[] array) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(array);
            return decode(byteBuffer);
        }

        // 对 byteBuffer 进行解码，这里的 byteBuffer 是前面所说的消息的第 2,3,4 部分，不包括第 1 部分，也就是消息的总长度
        public static RemotingCommand decode(final ByteBuffer byteBuffer) {
            int length = byteBuffer.limit();
            // 获取第 2 部分，4 个字节，表示消息头的长度（3 个字节）以及消息的序列化类型（1 个字节）
            int oriHeaderLen = byteBuffer.getInt();
            // 获取低 3 个字节，也就是消息头的长度
            int headerLength = getHeaderLength(oriHeaderLen);
    
            byte[] headerData = new byte[headerLength];
            byteBuffer.get(headerData);
    
            // getProtocolType 获取最高位的第 4 个字节，也就消息的序列化类型，然后进行消息的反序列化
            RemotingCommand cmd = headerDecode(headerData, getProtocolType(oriHeaderLen));
    
            int bodyLength = length - 4 - headerLength;
            byte[] bodyData = null;
            if (bodyLength > 0) {
                bodyData = new byte[bodyLength];
                // 将消息体的数据读入到 bodyData 字节数组中
                byteBuffer.get(bodyData);
            }
            cmd.body = bodyData;
    
            return cmd;
        }

        public static int getHeaderLength(int length) {
            return length & 0xFFFFFF;
        }

        public static SerializeType getProtocolType(int source) {
            return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
        }

        private static RemotingCommand headerDecode(byte[] headerData, SerializeType type) {
            switch (type) {
                case JSON:
                    RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
                    resultJson.setSerializeTypeCurrentRPC(type);
                    return resultJson;
                case ROCKETMQ:
                    RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(headerData);
                    resultRMQ.setSerializeTypeCurrentRPC(type);
                    return resultRMQ;
                default:
                    break;
            }
    
            return null;
        }

    }

}