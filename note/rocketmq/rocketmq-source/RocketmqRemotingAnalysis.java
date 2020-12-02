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

    /**
     * rocketmq 消息协议设计与编码
     * 
     * 消息分为 4 个部分：
     * 1.消息长度(总长度, 四个字节存储, 占用一个int类型)
     * 2.序列化类型&消息头长度(同样占用一个int类型, 第一个字节表示序列化类型, 后面三个字节表示消息头长度)
     * 3.消息头数据
     * 4.消息主体数据
     */

    /**
     * 通信方式和通信流程
     * 
     * RocketMQ支持三种方式的通信：同步、异步和单向（OneWay）
     * 
     * Rocketmq 同步通信的流程为：
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

        private static AtomicInteger requestId = new AtomicInteger(0);

        private int opaque = requestId.getAndIncrement();
        
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

        public ByteBuffer encodeHeader() {
            return encodeHeader(this.body != null ? this.body.length : 0);
        }
    
        public ByteBuffer encodeHeader(final int bodyLength) {
            // 1> header length size
            // length 代表的是消息的总长度，这里的 4 表示的是第二部分的长度，占用一个 int 类型
            int length = 4;
    
            // 2> header data length
            // length 加上了消息头具体数据的长度，也就是第 3 部分的长度
            byte[] headerData;
            headerData = this.headerEncode();
            length += headerData.length;
    
            // 3> body data length
            // length 加上了消息体中具体数据的长度，也就是第 4 部分的长度，到这里 length 等于第 2,3,4 部分的长度之和
            length += bodyLength;
            // 分配的长度目前包括: 总长度域(4) + 消息头长度域(4) + 消息头内容
            ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);
    
            // length
            // 保存总长度
            result.putInt(length);
    
            // header length
            // 放入第 2 部分，4 个字节，第 1 个字节表示序列化类型，后面 3 个字节表示消息头的长度
            result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));
    
            // header data
            // 保存消息头数据
            result.put(headerData);
    
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

        // 对 byteBuffer 进行解码，这里的 byteBuffer 是前面所说的消息的第 2,3,4 部分，不包括第 1 部分（也就是消息的总长度）
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

        // 组装 RemotingCommand 传输指令
        public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
            RemotingCommand cmd = new RemotingCommand();
            cmd.setCode(code);
            cmd.customHeader = customHeader;
            setCmdVersion(cmd);
            return cmd;
        }

    }

    public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {
        private static final Logger log = LoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    
        @Override
        public void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out)
            throws Exception {
            try {
                ByteBuffer header = remotingCommand.encodeHeader();
                out.writeBytes(header);
                byte[] body = remotingCommand.getBody();
                if (body != null) {
                    out.writeBytes(body);
                }
            } catch (Exception e) {
                log.error("encode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
                if (remotingCommand != null) {
                    log.error(remotingCommand.toString());
                }
                RemotingUtil.closeChannel(ctx.channel());
            }
        }
    }

    public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {

        public NettyRemotingServer(final NettyServerConfig nettyServerConfig) {
            this(nettyServerConfig, null);
        }

        public NettyRemotingServer(final NettyServerConfig nettyServerConfig, final ChannelEventListener channelEventListener) {
            
            super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());
            this.serverBootstrap = new ServerBootstrap();
            this.nettyServerConfig = nettyServerConfig;
            this.channelEventListener = channelEventListener;

            int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
            if (publicThreadNums <= 0) {
                publicThreadNums = 4;
            }

            this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyServerPublicExecutor_" + this.threadIndex.incrementAndGet());
                }
            });

            this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyBoss_%d", this.threadIndex.incrementAndGet()));
                }
            });

            if (useEpoll()) {
                // bossGroup
                this.eventLoopGroupSelector = new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(),
                        new ThreadFactory() {
                            private AtomicInteger threadIndex = new AtomicInteger(0);
                            private int threadTotal = nettyServerConfig.getServerSelectorThreads();
                            @Override
                            public Thread newThread(Runnable r) {
                                return new Thread(r, String.format("NettyServerEPOLLSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                            }});
            } else {
                // workerGroup
                this.eventLoopGroupSelector = new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(),
                        new ThreadFactory() {
                            private AtomicInteger threadIndex = new AtomicInteger(0);
                            private int threadTotal = nettyServerConfig.getServerSelectorThreads();

                            @Override
                            public Thread newThread(Runnable r) {
                                return new Thread(r, String.format("NettyServerNIOSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                            }
                        });
            }

            // 省略代码
        }

        @Override
        public void start() {

            this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyServerConfig.getServerWorkerThreads(),
                    new ThreadFactory() {
                        private AtomicInteger threadIndex = new AtomicInteger(0);
                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "NettyServerCodecThread_" + this.threadIndex.incrementAndGet());
                        }
                });

            ServerBootstrap childHandler = this.serverBootstrap
                    .group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                    .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024).option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.SO_KEEPALIVE, false).childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize())
                    .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize())
                    .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, new HandshakeHandler(TlsSystemConfig.tlsMode))
                                         .addLast(defaultEventExecutorGroup, new NettyEncoder(), new NettyDecoder(),
                                            new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                            new NettyConnectManageHandler(), new NettyServerHandler());
                        }
                    });

            if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
                childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            }

            try {
                ChannelFuture sync = this.serverBootstrap.bind().sync();
                InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
                this.port = addr.getPort();
            } catch (InterruptedException e1) {
                throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
            }

            if (this.channelEventListener != null) {
                this.nettyEventExecutor.start();
            }

            this.timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        NettyRemotingServer.this.scanResponseTable();
                    } catch (Throwable e) {
                        log.error("scanResponseTable exception", e);
                    }
                }
            }, 1000 * 3, 1000);
        }

        @Override
        public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis)
                throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
            // 调用 NettyRemotingAbstract 中的 invokeSyncImpl 方法
            return this.invokeSyncImpl(channel, request, timeoutMillis);
        }

        @Override
        public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis,
                InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException,
                RemotingTimeoutException, RemotingSendRequestException {
            this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
        }

    }

    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {

        public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
            this(nettyClientConfig, null);
        }

        public NettyRemotingClient(final NettyClientConfig nettyClientConfig, final ChannelEventListener channelEventListener) {
            // 调用父类的构造函数，主要设置单向调用和异步调用两种模式下的最大并发数
            super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
            this.nettyClientConfig = nettyClientConfig;
            this.channelEventListener = channelEventListener;

            // 执行用户回调函数的线程数
            int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
            if (publicThreadNums <= 0) {
                publicThreadNums = 4;
            }

            // 执行用户回调函数的线程池
            this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
                }
            });

            // netty eventLoopGroupWorker
            this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
                }
            });

            if (nettyClientConfig.isUseTLS()) {
                try {
                    sslContext = TlsHelper.buildSslContext(true);
                    log.info("SSL enabled for client");
                } catch (IOException e) {
                    log.error("Failed to create SSLContext", e);
                } catch (CertificateException e) {
                    log.error("Failed to create SSLContext", e);
                    throw new RuntimeException("Failed to create SSLContext", e);
                }
            }
        }

        public void start() {
            // 构建一个 DefaultEventExecutorGroup，使用里面的线程来处理我们注册的 ChannelHandler
            // 一个 ChannelHandler 都和一个 DefaultEventExecutorGroup 中的一个 EventExecutor 相联系，并且使用
            // 这个 EventExecutor 来处理 ChannelHandler 中自定义的业务逻辑
            this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                nettyClientConfig.getClientWorkerThreads(),
                new ThreadFactory() {
                    private AtomicInteger threadIndex = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                }
            });
    
            Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
                .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
                .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        if (nettyClientConfig.isUseTLS()) {
                            if (null != sslContext) {
                                pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
                                log.info("Prepend SSL handler");
                            } else {
                                log.warn("Connections are insecure as SSLContext is null!");
                            }
                        }
                        pipeline.addLast(
                            defaultEventExecutorGroup,
                            // 编码 handler
                            new NettyEncoder(),
                            // 解码 handler
                            new NettyDecoder(),
                            // 心跳检测
                            new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                            // 连接管理 handler，处理 connect、disconnect、close 等事件，每当对应的事件发生时，比如 CONNECT、CLOSE 等事件，
                            // 就会将其加入到 NettyEventExecutor 的阻塞队列中，让 ChannelEventListener 对象来具体进行处理
                            new NettyConnectManageHandler(),
                            // 处理接收到的 RemotingCommand 消息后的事件, 收到服务器端响应后的相关操作
                            new NettyClientHandler());
                    }
                });
    
            // 定时扫描 responseTable，获取返回结果，并且处理超时
            this.timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        NettyRemotingClient.this.scanResponseTable();
                    } catch (Throwable e) {
                        log.error("scanResponseTable exception", e);
                    }
                }
            }, 1000 * 3, 1000);
    
            if (this.channelEventListener != null) {
                // nettyConnectManageHandler 将发生的 channel 事件（CONNECT、CLOSE 等）加入到 NettyEventExecutor 的
                // 事件队列中，然后由 ChannelEventListener 对象不断从队列中读取出来进行对应处理
                this.nettyEventExecutor.start();
            }
        }

        @Override
        public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
                throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {

            final Channel channel = this.getAndCreateChannel(addr);
            // 如果 channel 不为 null，并且这个 channel 依然有效
            if (channel != null && channel.isActive()) {
                try {
                    if (this.rpcHook != null) {
                        this.rpcHook.doBeforeRequest(addr, request);
                    }
                    // 真正执行同步调用，并且阻塞直到返回结果，或者抛出异常
                    RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis);
                    if (this.rpcHook != null) {
                        this.rpcHook.doAfterResponse(RemotingHelper.parseChannelRemoteAddr(channel), request, response);
                    }
                    return response;
                } catch (RemotingSendRequestException e) {
                    // 省略代码
                } catch (RemotingTimeoutException e) {
                    // 省略代码
                }
            } else {
                this.closeChannel(addr, channel);
                throw new RemotingConnectException(addr);
            }
        }

        @Override
        public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
                throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
                RemotingTimeoutException, RemotingSendRequestException {
            final Channel channel = this.getAndCreateChannel(addr);
            if (channel != null && channel.isActive()) {
                try {
                    if (this.rpcHook != null) {
                        this.rpcHook.doBeforeRequest(addr, request);
                    }
                    this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
                } catch (RemotingSendRequestException e) {
                    log.warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                    this.closeChannel(addr, channel);
                    throw e;
                }
            } else {
                this.closeChannel(addr, channel);
                throw new RemotingConnectException(addr);
            }
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        // 将 CONNECT 事件加入到 NettyEventExecutor 的事件队列中
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            log.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.channel()));
            }
        }

        // 将 CLOSE 事件加入到 NettyEventExecutor 的事件队列中
        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        // 将 CLOSE 事件加入到 NettyEventExecutor 的事件队列中
        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        // 将 IDLE 事件加入到 NettyEventExecutor 的事件队列中
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        // 将 EXCEPTION 事件加入到 NettyEventExecutor 的事件队列中
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }
        }
    }

    public abstract class NettyRemotingAbstract {

        // This map caches all on-going requests.
        // responseTable 中保存的 ResponseFuture 代表依然没有收到回复的 request 对应的请求
        protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable = new ConcurrentHashMap<Integer, ResponseFuture>(256);

        public void putNettyEvent(final NettyEvent event) {
            this.nettyEventExecutor.putNettyEvent(event);
        }

        public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            final RemotingCommand cmd = msg;
            if (cmd != null) {
                switch (cmd.getType()) {
                    case REQUEST_COMMAND:
                        processRequestCommand(ctx, cmd);
                        break;
                    case RESPONSE_COMMAND:
                        processResponseCommand(ctx, cmd);
                        break;
                    default:
                        break;
                }
            }
        }

        public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
            // 获取到 response 中的 opaque，这个 opaque 其实就是 request 中的 requestId
            final int opaque = cmd.getOpaque();
            // 根据这个 opaque 从 responseTable 中取出 responseFuture
            final ResponseFuture responseFuture = responseTable.get(opaque);
            if (responseFuture != null) {
                responseFuture.setResponseCommand(cmd);
                responseTable.remove(opaque);
                // 如果 responseFuture 对象中有回调，那么就会执行回调函数，回调一般在异步调用中使用
                if (responseFuture.getInvokeCallback() != null) {
                    executeInvokeCallback(responseFuture);
                } else {
                    // 唤醒阻塞等待请求响应结果的线程
                    responseFuture.putResponse(cmd);
                    responseFuture.release();
                }
            } else {
                log.warn("receive response, but not matched any request, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
                log.warn(cmd.toString());
            }
        }

        public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
            // 根据 RemotingCommand 中的 code 获取到 processor 和 executorService
            final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
            final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessor : matched;
            final int opaque = cmd.getOpaque();
    
            if (pair != null) {
                Runnable run = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            RPCHook rpcHook = NettyRemotingAbstract.this.getRPCHook();
                            if (rpcHook != null) {
                                rpcHook.doBeforeRequest(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);
                            }
                            // 使用 processor 来处理这个 RemotingCommand 请求
                            final RemotingCommand response = pair.getObject1().processRequest(ctx, cmd);
                            if (rpcHook != null) {
                                rpcHook.doAfterResponse(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd, response);
                            }
    
                            if (!cmd.isOnewayRPC()) {
                                if (response != null) {
                                    response.setOpaque(opaque);
                                    response.markResponseType();
                                    try {
                                        // 将 response 对象发送回客户端
                                        ctx.writeAndFlush(response);
                                    } catch (Throwable e) {
                                        log.error("process request over, but response failed", e);
                                        log.error(cmd.toString());
                                        log.error(response.toString());
                                    }
                                } else {
                                }
                            }
                        } catch (Throwable e) {
                            // 省略代码
                        }
                    }
                };
    
                if (pair.getObject1().rejectRequest()) {
                    final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                        "[REJECTREQUEST]system busy, start flow control for a while");
                    response.setOpaque(opaque);
                    ctx.writeAndFlush(response);
                    return;
                }
    
                try {
                    // 将上面的 run 任务（实现了 Runnable 接口）封装成一个 task 提交到线程池中去执行，并且这个线程池和这个 processor 是成对的
                    final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
                    pair.getObject2().submit(requestTask);
                } catch (RejectedExecutionException e) {
                    // 省略代码
                }
            } else {
                String error = " request type " + cmd.getCode() + " not supported";
                final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
                response.setOpaque(opaque);
                ctx.writeAndFlush(response);
                log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
            }
        }

        // 同步转异步，同步调用模式依然是采用异步方式完成的
        public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
                throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {

            // opaque 可以看成是 requestId
            final int opaque = request.getOpaque();

            try {
                // 创建  ResponseFuture 对象
                final ResponseFuture responseFuture = new ResponseFuture(opaque, timeoutMillis, null, null);
                // 根据 opaque 保存 responseFuture，opaque 即 requestId，也就是在 responseTable 中，
                // 一个 requestId 和一个 responseFuture 一一对应
                this.responseTable.put(opaque, responseFuture);
                final SocketAddress addr = channel.remoteAddress();

                // 调用 netty 的 channel 发送请求，并且利用监听回写 response 结果
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        // 如果请求发送成功，那么就将 responseFuture 的 sendRequest 变量设置为 true，
                        // 表明发送成功，然后返回
                        if (f.isSuccess()) {
                            responseFuture.setSendRequestOK(true);
                            return;
                        // 如果请求发送失败，那么就将 responseFuture 的 sendRequest 变量设置为 false，
                        // 表明数据发送失败。然后继续执行下面的代码，将此 responseFuture 移除掉并且设置发送失败的原因
                        } else {
                            responseFuture.setSendRequestOK(false);
                        }

                        // 将此 responseFuture 从 responseTable 中移除掉
                        responseTable.remove(opaque);
                        // 设置发送失败的原因
                        responseFuture.setCause(f.cause());
                        // 将 responseFuture 中的 responseCommand 设置为 null，也就是表明没有返回结果
                        // 然后唤醒正在阻塞等待返回结果的线程
                        responseFuture.putResponse(null);
                        log.warn("send a request command to channel <" + addr + "> failed.");
                    }
                });

                // 等待 responseFuture 的返回结果
                RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
                // 如果 responseCommand 为 null，表明没有接收到服务器端的返回值
                if (null == responseCommand) {
                    // 为 true，表明请求发送成功，但是等待服务端响应超时
                    if (responseFuture.isSendRequestOK()) {
                        throw new RemotingTimeoutException();
                    // 为 false，表明是发送请求失败
                    } else {
                        throw new RemotingSendRequestException();
                    }
                }

                return responseCommand;
            } finally {
                // 根据 responseFuture 对应的 opaque，从 responseTable 中删除这个 responseFuture 对象
                this.responseTable.remove(opaque);
            }
        }

        public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis,
                final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException,
                RemotingTimeoutException, RemotingSendRequestException {
            final int opaque = request.getOpaque();
            boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
            if (acquired) {
                final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);

                final ResponseFuture responseFuture = new ResponseFuture(opaque, timeoutMillis, invokeCallback, once);
                this.responseTable.put(opaque, responseFuture);
                try {
                    channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture f) throws Exception {
                            if (f.isSuccess()) {
                                responseFuture.setSendRequestOK(true);
                                return;
                            } else {
                                responseFuture.setSendRequestOK(false);
                            }

                            responseFuture.putResponse(null);
                            responseTable.remove(opaque);
                            try {
                                executeInvokeCallback(responseFuture);
                            } catch (Throwable e) {
                                log.warn("excute callback in writeAndFlush addListener, and callback throw", e);
                            } finally {
                                responseFuture.release();
                            }

                            log.warn("send a request command to channel <{}> failed.",
                                    RemotingHelper.parseChannelRemoteAddr(channel));
                        }
                    });
                } catch (Exception e) {
                    responseFuture.release();
                    log.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel)
                            + "> Exception", e);
                    throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
                }
            } else {
                if (timeoutMillis <= 0) {
                    throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
                } else {
                    String info = String.format(
                            "invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                            timeoutMillis, this.semaphoreAsync.getQueueLength(),
                            this.semaphoreAsync.availablePermits());
                    log.warn(info);
                    throw new RemotingTimeoutException(info);
                }
            }
        }

        /**
         * Execute callback in callback executor. If callback executor is null, run directly in current thread
         */
        private void executeInvokeCallback(final ResponseFuture responseFuture) {
            boolean runInThisThread = false;
            ExecutorService executor = this.getCallbackExecutor();
            if (executor != null) {
                try {
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                responseFuture.executeInvokeCallback();
                            } catch (Throwable e) {
                                log.warn("execute callback in executor exception, and callback throw", e);
                            } finally {
                                responseFuture.release();
                            }
                        }
                    });
                } catch (Exception e) {
                    runInThisThread = true;
                    log.warn("execute callback in executor exception, maybe executor busy", e);
                }
            } else {
                runInThisThread = true;
            }
    
            if (runInThisThread) {
                try {
                    responseFuture.executeInvokeCallback();
                } catch (Throwable e) {
                    log.warn("executeInvokeCallback Exception", e);
                } finally {
                    responseFuture.release();
                }
            }
        }

    }

    class NettyEventExecutor extends ServiceThread {

        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        private final int maxSize = 10000;

        // 将 Channel 发生的事件加入到队列 eventQueue 中
        public void putNettyEvent(final NettyEvent event) {
            if (this.eventQueue.size() <= maxSize) {
                this.eventQueue.add(event);
            } else {
                log.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(), event.toString());
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            // 不断循环从 eventQueue 中获取事件，然后根据事件来进行处理
            while (!this.isStopped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
    }

    public class MQClientAPIImpl {

        public MQClientAPIImpl(final NettyClientConfig nettyClientConfig, final ClientRemotingProcessor clientRemotingProcessor, RPCHook rpcHook,
                final ClientConfig clientConfig) {

            this.clientConfig = clientConfig;
            topAddressing = new TopAddressing(MixAll.getWSAddr(), clientConfig.getUnitName());
            this.remotingClient = new NettyRemotingClient(nettyClientConfig, null);
            this.clientRemotingProcessor = clientRemotingProcessor;

            this.remotingClient.registerRPCHook(rpcHook);
            this.remotingClient.registerProcessor(RequestCode.CHECK_TRANSACTION_STATE, this.clientRemotingProcessor, null);
            this.remotingClient.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, this.clientRemotingProcessor, null);
            this.remotingClient.registerProcessor(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, this.clientRemotingProcessor, null);
            this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, this.clientRemotingProcessor, null);
            this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this.clientRemotingProcessor, null);
            this.remotingClient.registerProcessor(RequestCode.CONSUME_MESSAGE_DIRECTLY, this.clientRemotingProcessor, null);
        }

        public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis,
                boolean allowTopicNotExist) throws MQClientException, InterruptedException, RemotingTimeoutException,
                RemotingSendRequestException, RemotingConnectException {
            // 创建一个命令特有的消息头对象
            GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
            requestHeader.setTopic(topic);
            // 组装 RemotingCommand 传输指令，其实也就是将创建好的 customHeader（这里是 GetRouteInfoRequestHeader）
            // 以及 code（这里是 RequestCode.GET_ROUTEINTO_BY_TOPIC）设置到 RemotingCommand 中。
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC, requestHeader);

            // 调用 invokeSync
            RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
            assert response != null;
            switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST: {
                if (allowTopicNotExist && !topic.equals(MixAll.DEFAULT_TOPIC)) {
                    log.warn("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                }
                break;
            }
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            }
            default:
                break;
            }

            throw new MQClientException(response.getCode(), response.getRemark());
        }

    }

    public class GetRouteInfoRequestHeader implements CommandCustomHeader {
        @CFNotNull
        private String topic;
    
        @Override
        public void checkFields() throws RemotingCommandException {
        }
    
        public String getTopic() {
            return topic;
        }
    
        public void setTopic(String topic) {
            this.topic = topic;
        }
    }

    public class ResponseFuture {

        public ResponseFuture(int opaque, long timeoutMillis, InvokeCallback invokeCallback, SemaphoreReleaseOnlyOnce once) {
            this.opaque = opaque;
            this.timeoutMillis = timeoutMillis;
            this.invokeCallback = invokeCallback;
            this.once = once;
        }

        public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
            this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
            return this.responseCommand;
        }
    
        public void putResponse(final RemotingCommand responseCommand) {
            this.responseCommand = responseCommand;
            this.countDownLatch.countDown();
        }

        public void executeInvokeCallback() {
            if (invokeCallback != null) {
                if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                    invokeCallback.operationComplete(this);
                }
            }
        }
    }

    

}