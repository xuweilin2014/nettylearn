public class RocketmqHAAnalysis{

    /**
     * 我们知道，broker 有两种角色 master 和 slave，每种角色有两个关键配置：brokerId 和 brokerRole，brokerId 指定此 master 或 slave 在 Broker 中的序号，
     * 0 表示 Master，1及之后的表示 Slave，Broker 中 Slave 可以有多个，当然一般一个就够了，所以 brokerId 的值一般为1。brokerRole 表示其在 Broker 
     * 中的角色定位，有3个值可选：ASYNC_MASTER、SYNC_MASTERSLAVE。
     * 
     * ASYNC_MASTER：异步 Master，也就是新的消息存储时不需要等 Slave 同步完成；SYNC_MASTER：同步 Master，新消息存现时需要等 Slave 同步完成，
     * 也就是返回的 Ack Offset >= 当前消息的 CommitLog Offset； SLAVE：Slave 角色其 brokerRole；
     */

    /**
     * 为了提高消息消费的高可用性，避免 Broker 发生单点故障引起存储在 Broker 上的消息无法及时消费，Rocketmq 引入了 Broker 主备机制，
     * 即消息消费到达主服务器后需要将消息同步到消息从服务器，如果主服务器 Broker 宕机后，消息消费者可以从从服务器拉取消息。
     * 
     * HAService: Rocketmq 主从同步核心实现类
     * 
     * HAService$AcceptSocketService: Master 端监听客户端连接实现类
     * 
     * HAService$GroupTransferService: 主从同步通知实现类，用于判断主从同步复制是否完成
     * 
     * HAService$HAClient: HA Client 端实现类，SLAVE 使用此类对象来向 MASTER 发送心跳包来报告 slave offset，读取 MASTER 发送过来的数据
     * 
     * HAConnection: Master 与 SLAVE 连接的封装，包含 ReadSocketService 和 WriteSocketService 两个类，分别用来读取 SLAVE 的发送的请求
     * 以及往 SLAVE 发送消息数据
     * 
     * HAConnection$ReadSocketService: Master 网络读实现类，用来读取 SLAVE 上的 HA Client 发送的 slave offset，也就是 SLAVE 已经下次希望获取到
     * 的消息偏移量（确切地说是消息在 CommitLog 文件中的偏移量）
     * 
     * HAConnection$WriteSocketService: HA Master 网络写实现类，根据 SLAVE 发送的 slave offset，将 CommitLog 中的消息发送给 SLAVE 的 HA Client，
     * 同时每过 5s 会发送一个心跳包给 SLAVE
     */

    /**
     * 最开始，在 DefaultMessageStore 的构造函数中，创建 HAService 对象，然后在 DefaultMessageStore#start 方法中开启 haService。在 HAService
     * 类的构造方法中，会分别创建一个 HAClient 对象、一个 AcceptSocketService 对象以及一个 GroupTransferService 对象。这三个类都继承于 ServiceThread，
     * 所以都是服务线程。它们在 HAService#start 方法中开启。
     * 
     * MASTER 在 HAService#start 方法中会开启 AcceptSocketService 线程，用来获取到 SLAVE 端建立连接的请求。一旦接收到连接，就创建一个 HAConnection 
     * 对象将和此 SLAVE 的连接包装起来，并且在这个 HAConnection 对象中，开启两个服务线程，ReadSocketService 和 WriteSocketService 分别用来读取 SLAVE 的发送的请求
     * 以及往 SLAVE 发送消息数据。
     * 
     * HAClient 主要用于 SLAVE 端。在 HAClient 开启以后，它会和 MASTER 创建一个连接，然后会每隔一定时间向 MASTER 发送一个心跳包，在这个心跳包中只有一个 8 字节
     * 的数据 slave offset，【就是 SLAVE 端下次希望获取到的消息偏移量】（其实就是消息在 CommitLog 中的偏移量）。
     * 
     * MASTER 端的 ReadSocketService 在获取到 SLAVE 端发送过来的 slave offset 之后会判断其和 push2SlaveMaxOffset 的大小，如果大于的话
     * 就会唤醒阻塞的消息发送者线程（如果有的话）。
     * 
     * MASTER 端的 WriteSocketService 会将 CommitLog 中的消息发送给 SLAVE 进行同步
     */

    public class HAService {

        /**
         * Rocketmq 的 HA 实现原理如下：
         * 1.主服务器启动，并且在特定端口上监听从服务器的连接
         * 2.从服务器主动连接主服务器，主服务器接收客户端的连接，并且建立相关 TCP 连接
         * 3.从服务器主动向主服务器发送待拉取的消息偏移量，主服务器解析请求并且返回消息给从服务器
         * 4.从服务器保存消息并且继续发送新的消息同步请求
         */
        public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
            this.defaultMessageStore = defaultMessageStore;
            this.acceptSocketService = new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
            this.groupTransferService = new GroupTransferService();
            this.haClient = new HAClient();
        }

        public void start() throws Exception {
            this.acceptSocketService.beginAccept();
            this.acceptSocketService.start();
            this.groupTransferService.start();
            this.haClient.start();
        }

    }

    public class CommitLog{

        // GroupTransferService 主从同步阻塞实现，如果是同步主从模式，消息发送者将消息刷写到磁盘后，需要继续等待新数据被传输到从服务器，
        // 从服务器数据的复制是在另外一个线程 HAConnection 中去拉取，所以消息发送者在这里需要等待数据传输的结果
        // 下面的 SYNC_MASTER 表明 Master-Slave 之间使用的是同步复制，ASYNC_MASTER 表明的是异步复制
        public void handleHA(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
            if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
                HAService service = this.defaultMessageStore.getHaService();
                if (messageExt.isWaitStoreMsgOK()) {
                    // Determine whether to wait
                    if (service.isSlaveOK(result.getWroteOffset() + result.getWroteBytes())) {
                        // result.getWroteOffset + result.getWroteBytes 等于消息生产者发送消息后服务端返回的下一条消息的起始偏移量
                        GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                        service.putRequest(request);
                        service.getWaitNotifyObject().wakeupAll();
                        // 阻塞直到从服务器的消息同步完毕
                        boolean flushOK = request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                        if (!flushOK) {
                            log.error("do sync transfer other node, wait return, but failed");
                            putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                        }
                    }
                    // Slave problem
                    else {
                        // Tell the producer, slave not available
                        putMessageResult.setPutMessageStatus(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                    }
                }
            }
    
        }

    }

    // HAClient 是主从同步 Slave 端的核心实现类
    class HAClient extends ServiceThread{

        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;

        private SocketChannel socketChannel;
        // 读缓存区，大小为 4MB
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        private long lastWriteTimestamp = System.currentTimeMillis();
        // Slave 向 Master 发起主从同步的拉取偏移量
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        // 表示 byteBufferRead 中已经转发的指针
        private int dispatchPostion = 0;

        /**
         * Slave 端尝试连接 Master 服务器，在 Broker 启动的时候，如果 Broker 角色为 SLAVE 时将读取 Broker 配置文件中的
         * haMasterAddress 属性并且更新 HAClient 的 masterAddress，如果角色为 SLAVE 并且 haMasterAddress 为空，启动
         * 并不会报错，但不会执行主从同步复制，该方法最终返回是否成功连接上 Master
         * 
         * Master 在接收到 Slave 的连接请求后，创建 SocketChannel，封装成一个 HAConnection，同时启动 writeSocketService 
         * 和 readSocketService 服务。但 Master 启动时是不会主动传输数据的，因为其不知道 Slave 的 CommitLog的maxPhyOffset，
         * 也就是不知道从哪个位置开始同步，需要 Slave 先上报当前 CommitLog的maxPhyOffset。
         * @return
         * @throws ClosedChannelException
         */
        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                String addr = this.masterAddress.get();
                if (addr != null) {
                    // 当自身角色是 Slave 时,会将配置中的 MessageStoreConfig 中的 haMasterAddress 赋值给 masterAddress, 
                    // 或者在 registerBroker 时的返回值赋值
                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            // 注册网络读事件 OP_READ
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }
                // 初始化 currentReportedOffset 为 commitlog 文件的最大偏移量
                // 更新最近上报 offset, 也就是当前 SLAVE 端的 CommitLog 的 maxOffset
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
                this.lastWriteTimestamp = System.currentTimeMillis();
            }
            return this.socketChannel != null;
        }

        // 判断是否需要向 Master 反馈当前待拉取的偏移量，Master 与 Slave 的 HA 心跳发送间隔默认为 5s，
        // 可以通过配置 haSendHeartbeatInterval 来改变心跳间隔
        private boolean isTimeToReportOffset() {
            long interval = HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
            return needHeart;
        }

        /**
         * 向 Master 服务器反馈拉取偏移量，这里有两重意义，对于 Slave 端来说，是发送下次待拉取消息偏移量，而对于 Master 服务端来说，
         * 既可以认为是 Slave 本次请求拉取的消息偏移量，也可以理解为 Slave 的消息同步 ACK 确认消息
         * 
         * 上报进度,传递进度的时候仅传递一个 Long 类型的 Offset, 8 个字节,没有其他数据
         * @param maxOffset
         * @return
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            // 只保存一个 8 字节的 Long 型数据
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            // 特别需要留意的是，调用网络通道的 write 方法是在一个 while 循环中反复判断 byteBuffer 是否全部写入到通道中，
            // 这是由于 NIO 是一个非阻塞 IO，调用一次 write 方法不一定会将 ByteBuffer 可读字节全部写入
            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error("reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            return !this.reportOffset.hasRemaining();
        }

        /**
         * 处理网络读请求，即处理从 Master 服务器传回的消息数据。同样 RocketMQ 给出了一个处理网络读的 NIO 示例，循环判断 readByteBuffer 是否还有剩余空间，
         * 如果存在剩余空间，则调用 SocketChannel#read(ByteBuffer eadByteBuffer)，将通道中的数据读入到读缓存区中
         * 
         * 1.如果读取到的字节数大于 0，重置读取到 0 字节的次数，并更新最后一次写入时间戳（ lastWriteTimestamp ），然后调用 dispatchReadRequest 
         * 方法将读取到的所有消息全部追加到消息内存映射文件中，然后再次反馈拉取进度给服务器
         * 2.如果连续 3 次从网络通道读取到 0 个字节，则结束本次读取，返回 true
         * 3.如果读取到的字节数小于 0 或发生 IO 异常，则返回 false，
         * 
         * HA Client 线程反复执行上述 3 个步骤完成主从同步复制功能
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
                        readSizeZeroTimes = 0;
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        // 该方法主要从byteBufferRead中解析一条一条的消息，然后存储到commitlog文件并转发到消息消费队列与索引文件中
        // 该方法需要解决以下两个问题：
        // 1.如果判断 byteBufferRead 中是否包含一条完整的消息
        // 2.如果不包含一条完整的消息，应该如何处理
        private boolean dispatchReadRequest() {
            // msgHeaderSize 头部长度，大小为12个字节，8 个字节的物理偏移量，4 个字节的消息长度，包括消息的物理偏移量与消息的长度，
            // 长度字节必须首先探测，否则无法判断byteBufferRead缓存区中是否包含一条完整的消息
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            // readSocketPos 记录当前 byteBufferRead 的当前指针，后面可能会使用其恢复 byteBufferRead 的位置
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                // 先探测 byteBufferRead 缓冲区中是否包含一条消息的头部，如果包含头部，则读取物理偏移量与消息长度，然后再探测是否包含一条完整的消息，
                // 如果不包含，则需要将 byteBufferRead 中的数据备份，以便更多数据到达再处理
                // byteBufferRead.position - dispatchPosition 的值为 byteBufferRead 从网络连接中获取到的字节数
                int diff = this.byteBufferRead.position() - this.dispatchPostion;
                if (diff >= msgHeaderSize) {
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPostion);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPostion + 8);

                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    // 如果 byteBufferRead 中包含一则消息头部，则读取物理偏移量与消息的长度，然后获取 Slave 当前消息文件的最大物理偏移量，
                    // 如果 slave 的最大物理偏移量与 master 给的偏移量不相等，则返回false
                    // 这里也就是保证了 M-S 消息的一致性，Master 发送过来的消息在 Master 的 CommitLog 文件中的地址和现在 SLAVE 的 CommitLog 
                    // 的最大偏移量必须是一样的
                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave");
                            return false;
                        }
                    }

                    if (diff >= (msgHeaderSize + bodySize)) {
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPostion + msgHeaderSize);
                        // 读取 bodySize 个字节内容到 byte[] 字节数组中
                        this.byteBufferRead.get(bodyData);
                        // 将消息内容追加到消息内存映射文件中
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        this.byteBufferRead.position(readSocketPos);
                        // 更新 dispatchPosition，dispatchPosition 表示 byteBufferRead 中已转发的指针
                        // 更确切地说，dispatchPosition 表示 byteBufferRead 中已经被 SLAVE 读取并添加到 CommitLog 文件的字节数
                        this.dispatchPostion += msgHeaderSize + bodySize;

                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                // 只要 byteBufferRead 中还有剩余的空间，就不会调用 reallocateByteBuffer，否则会发生错误
                // 如果 byteBufferRead 中还有剩余空间，则会继续在 run 方法中，从 socketChannel 中读取数据到 byteBufferRead 中
                if (!this.byteBufferRead.hasRemaining()) {
                    // reallocateByteBuffer 方法的核心思想是：byteBufferRead 已经被写满了，所以将 dispatchPosition 到 limit 之间的数据
                    // 复制到另外一个 byteBufferBackup 中，然后交换 byteBufferBackup 与 byteBufferRead
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 当存在 masterAddress != null && 连接 Master 成功
                    if (this.connectMaster()) {
                        // 若距离上次上报时间超过5S，上报到 Master 进度
                        if (this.isTimeToReportOffset()) {
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        // 最多阻塞 1s,直到 Master 有数据同步过来。若 1s 满了还是没有接受到数据,中断阻塞, 执行 processReadEvent()
                        this.selector.select(1000);
                        // 处理读取事件
                        boolean ok = this.processReadEvent();

                        if (!ok) {
                            this.closeMaster();
                        }
                        // 处理读事件之后，若进度有变化，上报到Master进度
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }
                        long interval = HAService.this.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;
                        // Master 超过 20s 未返回数据，关闭连接，MASTER 每过 5s 就会向 SLAVE 发送一次心跳包
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection");
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {
        // Broker 服务监听的套接字（本地 IP + 端口号）
        private final SocketAddress socketAddressListen;
        private ServerSocketChannel serverSocketChannel;
        // 事件选择器
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        // 开始监听从节点的连接请求
        public void beginAccept() throws Exception {
            // 创建 ServerSocketChannel
            this.serverSocketChannel = ServerSocketChannel.open();
            // 创建 Selector
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            // 绑定监听端口
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            // 设置为非阻塞模式
            this.serverSocketChannel.configureBlocking(false);
            // 并且注册 OP_ACCEPT 事件（连接事件）
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            // 下面的逻辑是标准的基于 NIO 的服务端实例，选择器每 1s 处理一次连接就绪事件，连接事件就绪后
            // 调用 ServerSocketChannel 的 accept 方法创建 SocketChannel。然后为每一个连接创建一个 HAConnection 
            // 对象，该 HAConnection 将负责 M-S 数据同步逻辑
            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection");
                                    try {
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }
                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }


    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        // GroupTransferService 的职责是负责当主从同步复制结束后通知由于等待 HA 同步结果而阻塞的消息发送者线程。 
        // 判断主从同步是否完成的依据是 Slave 中已成功复制的最大偏移量是否大于等于消息生产者发送消息后消息服务端返回
        // 下一条消息的起始偏移，如果是则表示主从同步复制已经完成，唤醒消息发送线程，否则等待 ls 再次判断，每一个
        // 任务在一批任务中循环判断 5 次。消息发送者返回有两种情况： 等待超过 5s或者  GroupTransferService 通知主从复制完成，
        // 可以通过 syncFlushTimeout 来设置发送线程等待超时时间。
        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        // 判断 Slave 中已成功复制的最大偏移量是否大于等于消息生产者发送消息后消息服务端返回下一条消息的起始偏移
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        // 最多等待 5s
                        for (int i = 0; !transferOK && i < 5; i++) {
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }
                        req.wakeupCustomer(transferOK);
                    }
                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }
            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    public class HAConnection {

        private volatile long slaveRequestOffset = -1;
        
        private volatile long slaveAckOffset = -1;

        public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
            this.haService = haService;
            this.socketChannel = socketChannel;
            this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
            this.socketChannel.configureBlocking(false);
            this.socketChannel.socket().setSoLinger(false, -1);
            this.socketChannel.socket().setTcpNoDelay(true);
            this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
            this.socketChannel.socket().setSendBufferSize(1024 * 64);
            this.writeSocketService = new WriteSocketService(this.socketChannel);
            this.readSocketService = new ReadSocketService(this.socketChannel);
            this.haService.getConnectionCount().incrementAndGet();
        }

        public void start() {
            // 启动 readSocketService 读取 Slave 传过来的数据
            this.readSocketService.start();
            // 启动 writeSocketService 向 Channel 写入消息, 同步给 Slave
            this.writeSocketService.start();
        }

    }

    // 服务端解析从服务器的拉取请求实现类为 HAConnection 的内部类 ReadSocketService
    class ReadSocketService extends ServiceThread {

        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;

        private final Selector selector;

        private final SocketChannel socketChannel;

        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        private int processPostion = 0;

        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.thread.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 每隔 1s 处理一次读就绪事件，每次读请求调用其 processReadEvent 来解析从服务器拉取的请求
                    this.selector.select(1000);
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    
                    // 如果超过 20s 没有收到 SLAVE 的心跳包，则关闭掉这个连接
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            this.makeStop();
            writeSocketService.makeStop();
            haService.removeConnection(HAConnection.this);
            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        // 解析 SLAVE 服务器拉取的请求
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            // 如果该 byteBufferRead 没有剩余空间，那么说明 position == limit，调用 byteBufferRead 的 flip 方法
            // 产生的效果为 limit = position, position = 0，表示从头开始处理
            if (!this.byteBufferRead.hasRemaining()) {
                this.byteBufferRead.flip();
                this.processPostion = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();

                        // 超过 8 个字节处理，因为 slave 的 broker 发送的就是 8 个字节的 slave 的 offset 的心跳
                        // 如果读取的字节大于 0 并且本次读取到的内容大于等于 8，表明收到了从服务器一条拉取消息的请求。
                        // 由于有新的从服务器反馈拉取偏移量，服务端会通知由于同步等待 HA 复制结果而阻塞的消息发送者线程
                        if ((this.byteBufferRead.position() - this.processPostion) >= 8) {
                            // 获取离 byteBufferRead.position() 最近的 8 的整除数（获取最后一个完整的包）
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            // 读取能读取到的最后一个有效的 8 个字节的心跳包，比如 position = 571, 则 571 - (571%8) = 571-3 = 568
                            // 也就是读取 560-568 位置的字节, 因为Slave可能发送了多个进度过来, Master只读最末尾也就是最大的那个
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            this.processPostion = pos;

                            // 更新 slave broker 反馈的已经拉取完的 offset 偏移量，也就是下一次希望获取到的消息起始偏移量
                            HAConnection.this.slaveAckOffset = readOffset;
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                // 将 slave broker 请求的拉取消息的偏移量也更新为该值
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            // 读取从服务器已拉取偏移量，因为有新的从服务器反馈拉取进度，需要通知某些生产者以便返回，因为如果消息发送使用同步方式，
                            // 需要等待将消息复制到从服务器，然后才返回，故这里需要唤醒相关线程去判断自己关注的消息是否已经传输完成
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    // 该类主要负责将消息内容传输给从服务器
    class WriteSocketService extends ServiceThread {
        private final Selector selector;
        private final SocketChannel socketChannel;

        private final int headerSize = 8 + 4;
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);
        private long nextTransferFromWhere = -1;
        private SelectMappedBufferResult selectMappedBufferResult;
        private boolean lastWriteOver = true;
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.thread.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);

                    // 如果 slaveRequestOffset 等于 -1，说明 Master 还未收到 SLAVE 服务器的拉取请求，因为不知道 Slave 需要同步的开始位置，
                    // 会休眠，然后继续，只有 Slave 把 maxPhyOffset 传给 Master 后才能往下
                    // slaveRequestOffset 在收到从服务器拉取请求时更新（HAConnection$ReadSocketService#processReadEvent）
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }

                    // 如果 nextTransferFromWhere 为 -1 表示初次进行数据传输，需要计算需要传输的物理偏移量
                    if (-1 == this.nextTransferFromWhere) {
                        // 如果 Slave 传的 maxPhyOffset 为 0，那么 Master 只会同步最后一个 MappedFile 的数据到 Slave
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            masterOffset = masterOffset - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getMapedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        // 否则根据从服务器的拉取请求偏移量开始传输
                        } else {
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }
                    }

                    // 如果已全部写入，判断当前系统是与上次最后写入的时间间隔是否大于 HA 心跳检测时间，则需要发送一个心跳包，心跳包的长度为 12 个字节
                    // (从服务器待拉取偏移量(8) + size(4))，消息长度默认存 0，表示本次数据包为心跳包，避免长连接由于空闲被关闭。HA 心跳包发送间隔通过设置
                    // haSendHeartbeatInterval，默认值为 5s
                    if (this.lastWriteOver) {
                        long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaSendHeartbeatInterval()) {
                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            // size = 0
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();
                            this.lastWriteOver = this.transferData();

                            if (!this.lastWriteOver)
                                continue;
                        }
                    // 如果上次数据未写完，则继续传输上一次的数据，然后再次判断是否传输完成，如果消息还是未全部传输，
                    // 则结束此次事件处理，待下次写事件到底后，继续将未传输完的数据先写入消息从服务器。
                    } else {
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    // 根据 SLAVE 服务器请求的待拉取偏移量，RocketMQ 首先获取该偏移量之后所有的可读消息，如果未查到匹配的消息，通知所有等待线程继续等待100ms
                    SelectMappedBufferResult selectResult = HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    
                    // 如果匹配到消息，判断返回消息总长度是否大于配置的 HA 传输一次同步任务最大传输的字节数，则通过设置 ByteBuffer 的 limit 来设置只传输
                    // 指定长度的字节，这就意味着 HA 客户端收到的信息会包含不完整的消息。HA 一批次传输消息最大字节通过 haTransferBatchSize 来设置，默认值为 32K。
                    // HA 服务端消息的传输一直以上述步骤在循环运行，每次事件处理完成后等待 1s
                    if (selectResult != null) {
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        this.nextTransferFromWhere += size;

                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        // headerSize 为 8 + 4 = 12
                        this.byteBufferHeader.limit(headerSize);
                        // 发送到 SLAVE 端的消息在 CommitLog 文件中的偏移量
                        this.byteBufferHeader.putLong(thisOffset);
                        // 发送的消息的大小
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        this.lastWriteOver = this.transferData();
                    } else {
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            this.makeStop();
            readSocketService.makeStop();
            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;

            // Write Header(8 + 4)，8 个字节为消息在 CommitLog 文件的偏移量，4 个字节为消息的大小
            // 如果请求头有数据，先传输请求头，同时更新最后写入时间
            while (this.byteBufferHeader.hasRemaining()) {
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            if (null == this.selectMappedBufferResult) {
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            // 请求头没有数据，就发送消息体，也就是消息本身
            if (!this.byteBufferHeader.hasRemaining()) {
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }

    public class PullAPIWrapper{

        // 首先从 pullFromWhichNodeTable 缓存表中获取该消息消费队列的 brokerId，如果找到，则返回，否则返回 brokerName 的主节点。
        // 由此可以看出 pullFromWhichNodeTable 中存放的是消息队列建议从从哪个 Broker 服务器拉取消息的缓存表，
        // 其存储结构：MessageQueue：AtomicLong，那该信息从何而来呢？
        //
        // 原来消息消费拉取线程 PullMessageService 根据 PullRequest 请求从主服务器拉取消息后会返回下一次建议拉取的 brokerId，
        // 消息消费者线程在收到消息后，会根据主服务器的建议拉取 brokerId 来更新 pullFromWhichNodeTable
        public long recalculatePullFromWhichNode(final MessageQueue mq) {
            if (this.isConnectBrokerByUser()) {
                return this.defaultBrokerId;
            }
    
            AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
            if (suggest != null) {
                return suggest.get();
            }
    
            return MixAll.MASTER_ID;
        }

        public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
            AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
            if (null == suggest) {
                this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
            } else {
                suggest.set(brokerId);
            }
        }

        // RocketMQ 根据 MessageQueue 查找 Broker 地址的唯一依据便是 brokerName，从 RocketMQ 的 Broker 组织实现来看，同一组 Broker(M-S) 服务器，
        // 其 brokerName 相同，主服务器的 brokerId 为0，从服务器的 brokerId 大于 0，RocketMQ 提供了 MQClientFactory.findBrokerAddressInSubscribe 
        // 来实现根据 brokerName、brokerId 查找 Broker 地址
        public PullResult pullKernelImpl(final MessageQueue mq, final String subExpression, final String expressionType,
                final long subVersion, final long offset, final int maxNums, final int sysFlag, final long commitOffset,
                final long brokerSuspendMaxTimeMillis, final long timeoutMillis,
                final CommunicationMode communicationMode, final PullCallback pullCallback)
                throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                    this.recalculatePullFromWhichNode(mq), false);
            if (null == findBrokerResult) {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
                findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                        this.recalculatePullFromWhichNode(mq), false);
            }

            if (findBrokerResult != null) {
                {
                    // check version
                    if (!ExpressionType.isTagType(expressionType)
                            && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                        throw new MQClientException(
                                "The broker[" + mq.getBrokerName() + ", " + findBrokerResult.getBrokerVersion()
                                        + "] does not upgrade to support for filter message by " + expressionType,
                                null);
                    }
                }
                int sysFlagInner = sysFlag;

                if (findBrokerResult.isSlave()) {
                    sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
                }

                PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
                requestHeader.setConsumerGroup(this.consumerGroup);
                requestHeader.setTopic(mq.getTopic());
                requestHeader.setQueueId(mq.getQueueId());
                requestHeader.setQueueOffset(offset);
                requestHeader.setMaxMsgNums(maxNums);
                requestHeader.setSysFlag(sysFlagInner);
                requestHeader.setCommitOffset(commitOffset);
                requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
                requestHeader.setSubscription(subExpression);
                requestHeader.setSubVersion(subVersion);
                requestHeader.setExpressionType(expressionType);

                String brokerAddr = findBrokerResult.getBrokerAddr();
                if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                    brokerAddr = computPullFromWhichFilterServer(mq.getTopic(), brokerAddr);
                }

                PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(brokerAddr, requestHeader,
                        timeoutMillis, communicationMode, pullCallback);

                return pullResult;
            }

            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }

    }

    public class MQClientInstance{

        public FindBrokerResult findBrokerAddressInSubscribe(final String brokerName, final long brokerId,
                final boolean onlyThisBroker) {

            String brokerAddr = null;
            boolean slave = false;
            boolean found = false;

            // brokerAddrTable 地址缓存表中根据 brokerName 获取所有的 broke r信息。brokerAddrTable 的存储格式如：
            // brokerName -> { brokerId -> brokerAddress }
            HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
            if (map != null && !map.isEmpty()) {
                brokerAddr = map.get(brokerId);
                // 如果 brokerId=0 表示返回的 broker 是主节点，否则返回的是从节点
                slave = brokerId != MixAll.MASTER_ID;
                found = brokerAddr != null;

                // onlyThisBroker 表明是否必须返回此 BrokerId 的 Broker，如果不是则返回任意一个 SLAVE 节点
                if (!found && !onlyThisBroker) {
                    Entry<Long, String> entry = map.entrySet().iterator().next();
                    brokerAddr = entry.getValue();
                    slave = entry.getKey() != MixAll.MASTER_ID;
                    found = true;
                }
            }

            if (found) {
                return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
            }

            return null;
        }

    }

    public class DefaultMessageStore implements MessageStore {

        public GetMessageResult getMessage(final String group, final String topic, final int queueId, 
            final long offset, final int maxMsgNums, final MessageFilter messageFilter) {

            // maxOffsetPy：代表当前主服务器消息存储文件最大偏移量
            // maxPhyOffsetPulling：此次拉取消息最大偏移量
            // diff：对于 PullMessageService 线程来说，当前未被拉取到消息消费端的消息长度。
            // TOTAL_PHYSICAL_MEMORY_SIZE：RocketMQ 所在服务器总内存大小; 
            // accessMessageInMemoryMaxRatio：表示 RocketMQ 所能使用的最大内存比例，超过该内存，消息将被置换出内存；
            // memory 表示RocketMQ消息常驻内存的大小，超过该大小，RocketMQ会将旧的消息置换会磁盘。
            // 如果 diff 大于 memory,表示当前需要拉取的消息已经超出了常驻内存的大小，表示主服务器繁忙，此时才建议从从服务器拉取。
            long diff = maxOffsetPy - maxPhyOffsetPulling;
            long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
            getResult.setSuggestPullingFromSlave(diff > memory);

        }

        @Override
        public boolean appendToCommitLog(long startOffset, byte[] data) {
            if (this.shutdown) {
                log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
                return false;
            }

            boolean result = this.commitLog.appendData(startOffset, data);
            if (result) {
                // 唤醒线程根据 CommitLog 文件构建 ConsumeQueue、IndexFile 文件
                this.reputMessageService.wakeup();
            } else {
                log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
            }

            return result;
        }

    }

}