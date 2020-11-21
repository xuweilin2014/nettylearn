public class RocketmqMessageStoreAnalysis{

    /**
     * Rocketmq 消息文件存储目录存为 ${ROCKETMQ_HOME}/store/commitlog 目录，每一个文件默认大小为 1 G，一个文件写满之后再创建另外一个，
     * 并且以该文件中的第一个偏移量为文件名，偏移量小于 20 位用 0 补齐。这样就可以根据偏移量快速定位到消息的位置。
     * 
     * MappedFileQueue 可以看成是 ${ROCKET_HOME}/store/commitlog/ 文件夹，而 MappedFile 则可以看成是 commitlog 文件夹下的一个文件
     */

    /**
     * Rocketmq 使用内存映射文件来提高 IO 访问性能，无论是 CommitLog、ConsumeQueue 还是 IndexFile，单个文件都被设计为固定长度，
     * 如果一个文件写满以后再创建一个新文件，文件名就为该文件第一条消息对应的全局物理偏移量。
     * 
     * MappedFileQueue 是 MappedFile 的管理容器，MappedFileQueue 是对存储目录的封装，例如 CommitLog 文件的存储路径 ${ROCKET_HOME}/store/commitlog/，
     * 该目录下会存在多个内存映射文件（MappedFile）
     */

    /**
     * IndexFile 属性，IndexFile 主要分为 3 部分，也就是 IndexHeader，hash 槽，index 条目，因此其属性也主要和这三个部分有关：
     * hashSlotNum：hash 槽的个数
     * indexNum：index 条目的个数
     * indexHeader：IndexHeader 对象
     * 
     * ConsumeQueue 属性：
     * queueId：这个 ConsumeQueue 文件属于的消息队列的 id
     * topic：这个 ConsumeQueue 文件属于的消息主题 topic
     * mappedFileSize：这个 ConsumeQueue 文件的大小，30000 * 20 byte
     * CQ_STORE_UNIT_SIZE：ConsumeQueue 文件中每一个条目的大小，20 byte
     * 
     * MappedFile 属性：
     * OS_PAGE_SIZE：操作系统中一页的大小，也就是 4kb
     * wrotePosition：消息已经写入的位置
     * flushedPosition：消息已经刷盘的位置
     * commitedPosition：已经提交的消息的位置，这里我们要注意分两种情况：当 transientPoolEnable 为 true 的话，消息的写入分为 3 个阶段，第一个阶段是
     * 写入到 writeBuffer 中，然后在 CommitLog 类中开启的线程 CommitRealTimeService，会将 writeBuffer 中的数据写入到 fileChannel 中，然后再同步刷盘
     * 或者异步刷盘的时候写入到磁盘里面。当 transientPoolEnable 为 false 的时候，消息写入只有两个阶段，一个是将消息写入到 mappedByteBuffer 中，
     * 然后再进行刷盘。所以 committedPosition 只有在 transientPool 开启的时候才有用
     * 
     * MappedFileQueue 属性：
     * flushedWhere：消息已经刷新到哪儿了
     * committedWhere：消息已经提交到哪儿了
     * allocateMappedFileService：用来真正的分配 MappedFile
     * 
     */

    public class DefaultMessageStore implements MessageStore {
        // 消息存储配置属性
        private final MessageStoreConfig messageStoreConfig;
        // CommitLog 文件的存储实现类
        private final CommitLog commitLog;
        // 消息队列缓存
        private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;
        // 消息队列文件 ConsumeQueue 刷盘服务，或者叫线程
        private final FlushConsumeQueueService flushConsumeQueueService;
        // 清除 CommitLog 文件服务
        private final CleanCommitLogService cleanCommitLogService;
        // 清除 ConsumeQueue 文件服务
        private final CleanConsumeQueueService cleanConsumeQueueService;
        // 索引文件实现类
        private final IndexService indexService;
        // 真正分配 MappedFile 文件内存的服务类
        private final AllocateMappedFileService allocateMappedFileService;
        // 这个服务用于根据 CommitLog 文件中的消息构建 ConsumeQueue 文件和 IndexFile 索引文件
        private final ReputMessageService reputMessageService;

        private final MessageArrivingListener messageArrivingListener;

        public DefaultMessageStore(final MessageStoreConfig messageStoreConfig,
                final BrokerStatsManager brokerStatsManager, final MessageArrivingListener messageArrivingListener,
                final BrokerConfig brokerConfig) throws IOException {

            this.messageArrivingListener = messageArrivingListener;
            this.brokerConfig = brokerConfig;
            this.messageStoreConfig = messageStoreConfig;
            this.brokerStatsManager = brokerStatsManager;
            this.allocateMappedFileService = new AllocateMappedFileService(this);
            this.commitLog = new CommitLog(this);
            this.consumeQueueTable = new ConcurrentHashMap<>(32);

            this.flushConsumeQueueService = new FlushConsumeQueueService();
            this.cleanCommitLogService = new CleanCommitLogService();
            this.cleanConsumeQueueService = new CleanConsumeQueueService();
            this.storeStatsService = new StoreStatsService();
            this.indexService = new IndexService(this);
            this.haService = new HAService(this);

            this.reputMessageService = new ReputMessageService();

            this.scheduleMessageService = new ScheduleMessageService(this);

            this.transientStorePool = new TransientStorePool(messageStoreConfig);

            if (messageStoreConfig.isTransientStorePoolEnable()) {
                this.transientStorePool.init();
            }

            this.allocateMappedFileService.start();

            this.indexService.start();

            this.dispatcherList = new LinkedList<>();
            this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
            this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());

            File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
            MappedFile.ensureDirOK(file.getParent());
            lockFile = new RandomAccessFile(file, "rw");
        }
        
        // DefaultMessageStore#putMessage
        public PutMessageResult putMessage(MessageExtBrokerInner msg) {
            // 判断当前 Broker 是否能够进行消息写入
            
            // 1.如果当前 Broker 停止工作，不支持写入
            if (this.shutdown) {
                log.warn("message store has shutdown, so putMessage is forbidden");
                return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
            }
            // 2.如果当前 Broker 是 SLAVE 的话，那么也不支持写入
            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                long value = this.printTimes.getAndIncrement();
                if ((value % 50000) == 0) {
                    log.warn("message store is slave mode, so putMessage is forbidden ");
                }
                return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
            }
            // 3.如果当前 Broker 不支持消息写入，则拒绝消息写入
            if (!this.runningFlags.isWriteable()) {
                long value = this.printTimes.getAndIncrement();
                if ((value % 50000) == 0) {
                    log.warn("message store is not writeable, so putMessage is forbidden " + this.runningFlags.getFlagBits());
                }
                return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
            } else {
                this.printTimes.set(0);
            }
            // 4.如果消息主题的长度大于 256 个字符，则拒绝消息写入
            if (msg.getTopic().length() > Byte.MAX_VALUE) {
                log.warn("putMessage message topic length too long " + msg.getTopic().length());
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
            }
            // 5.如果消息属性的长度超过 65536 个字符，则拒绝消息写入
            if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
                return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
            }

            if (this.isOSPageCacheBusy()) {
                return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null);
            }

            long beginTime = this.getSystemClock().now();
            PutMessageResult result = this.commitLog.putMessage(msg);

            long eclipseTime = this.getSystemClock().now() - beginTime;
            if (eclipseTime > 500) {
                log.warn("putMessage not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, msg.getBody().length);
            }
            this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
            }

            return result;
        }

        public void start() throws Exception {

            lock = lockFile.getChannel().tryLock(0, 1, false);
            if (lock == null || lock.isShared() || !lock.isValid()) {
                throw new RuntimeException("Lock failed,MQ already started");
            }
    
            lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
            lockFile.getChannel().force(true);
    
            this.flushConsumeQueueService.start();
            this.commitLog.start();
            this.storeStatsService.start();
    
            if (this.scheduleMessageService != null && SLAVE != messageStoreConfig.getBrokerRole()) {
                this.scheduleMessageService.start();
            }
    
            // Broker 服务器在启动的时候会启动 ReputMessageService 线程，并且初始化一个非常关键的参数 reputFromOffset，
            // 该参数的含义是 ReputMessageService 线程从哪个偏移量开始转发消息给 ConsumeQueue 和 IndexFile。如果允许重复转发，
            // reputFromOffset 设置为 CommitLog 的提交指针。如果不允许重复转发，reputFromOffset 设置为 CommitLog 的内存中的最大偏移量
            if (this.getMessageStoreConfig().isDuplicationEnable()) {
                this.reputMessageService.setReputFromOffset(this.commitLog.getConfirmOffset());
            } else {
                this.reputMessageService.setReputFromOffset(this.commitLog.getMaxOffset());
            }
            this.reputMessageService.start();
    
            this.haService.start();
    
            this.createTempFile();
            this.addScheduleTask();
            this.shutdown = false;
        }

        public void doDispatch(DispatchRequest req) {
            // CommitLogDispatcherBuildIndex 和 CommitLogDispatcherBuildConsumeQueue 这两个类对象在 DefaultMessageStore 的构造方法中被添加到
            // dispatcherList 中去，在这里会被依次调用，将从 CommitLog 中读取到的消息添加到 ConsumeQueue 和 IndexFile 中去。
            for (CommitLogDispatcher dispatcher : this.dispatcherList) {
                dispatcher.dispatch(req);
            }
        }

        public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
            // 根据消息主题 topic 和消息队列 id 从 consumeQueueTable 中获取到对应的 ConsumeQueue 
            ConsumeQueue cq = this.findConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
            cq.putMessagePositionInfoWrapper(dispatchRequest);
        }

        private void addScheduleTask() {
            // rocketmq 会每隔 10s 调度一次 cleanFilesPeriodically 方法，检测是否需要清除过期文件，执行的
            // 频率可以通过设置 cleanResourceInterval，默认为 10s
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    DefaultMessageStore.this.cleanFilesPeriodically();
                }
            }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

            // ignore code
        }

        // 分别执行清除消息存储文件（CommitLog 文件）与消息消费队列文件（ConsumeQueue 文件）
        private void cleanFilesPeriodically() {
            this.cleanCommitLogService.run();
            this.cleanConsumeQueueService.run();
        }
        

    }

    class CleanCommitLogService {

        public void run() {
            try {
                this.deleteExpiredFiles();
                this.redeleteHangedFile();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            int deleteCount = 0;
            // 文件保留时间，也就是从最后一次更新到现在的时间，如果超过了该时间，就说明是过期文件，可以被删除。
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            // 删除物理文件的时间间隔，因为在一次清除过程中，可能需要被删除的文件不止一个，该值指定了两次删除文件之间的时间间隔
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

            // 指定删除文件的时间点， RocketMQ 通过 deleteWhen 设置一天的固定时间执行一次删除过期文件操作，默认为凌晨 4 点
            boolean timeup = this.isTimeToDelete();
            // 磁盘空间是否充足，如果磁盘空间不充足，则返回 true，表示应该触发过期文件删除机制，
            boolean spacefull = this.isSpaceToDelete();
            // 预留，手工触发，可以通过调用 excuteDeleteFilesManualy 方法手工触发过期文件删除，目前 RocketMQ 暂未封装手工触发文件删除的命令
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            if (timeup || spacefull || manualDelete) {

                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",
                    fileReservedTime,
                    timeup,
                    spacefull,
                    manualDeleteFileSeveralTimes,
                    cleanAtOnce);

                fileReservedTime *= 60 * 60 * 1000;

                deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                    destroyMapedFileIntervalForcibly, cleanAtOnce);
                if (deleteCount > 0) {
                } else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }

    }

    class CommitRealTimeService extends FlushCommitLogService {

        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                // CommitRealTimeService 线程间隔时间，默认 200ms
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();
                // commitDataLeastPages 一次提交任务至少包含页数， 如果待提交数据不足，小于该参数配置的值，将忽略本次提交任务，默认为 4 页
                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();
                // commitDataThoroughInterval 两次真实提交的最大间隔，默认为 200ms
                int commitDataThoroughInterval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                // 如果距上次提交间隔超过 commitDataThoroughInterval，则本次提交忽略 commitDataLeastPages 参数，也就是如果待提交数据小于指定页数，
                // 也执行提交操作
                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }

                try {
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    if (!result) {
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        // commit 操作执行完成后，CommitRealTimeService 唤醒 flushCommitLogService 线程执行 flush 操作
                        flushCommitLogService.wakeup();
                    }

                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    class FlushRealTimeService extends FlushCommitLogService {
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                // 默认为 false，表示 await 方法等待，如果为 true，表示使用 sleep 进行等待
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();
                // FlushRealTimeService 线程任务运行时间间隔
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                // 一次刷写任务至少包含的页数，如果待刷写的数据不足，小于该参数配置的值，将忽略本次刷写任务，默认为 4 页
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();
                // 两次真实刷写任务最大间隔，默认为 10s
                int flushPhysicQueueThoroughInterval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                // 如果距上次提交间隔超过 flushPhysicQueueThoroughinterval，则本次刷盘任务将忽略 flushPhysicQueueLeastPages。
                // 也就是如果待刷写数据小于指定页数也执行刷写磁盘操作
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    // 执行一次刷盘任务前先等待指定的时间间隔，然后再执行刷盘任务
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }


    class ReputMessageService extends ServiceThread {

        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        // ReputMessageService#doReput
        private void doReput() {
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

                if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
                    && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
                    break;
                }

                // 返回 reputFromOffset 偏移量开始的全部有效数据(CommitLog 文件)，然后循环读取每一条消息
                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {
                        this.reputFromOffset = result.getStartOffset();

                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {
                            // 从 result 中返回的 ByteBuffer 中循环读取消息，一次读取一条，并且创建 DispatchResult 对象
                            DispatchRequest dispatchRequest = DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                            int size = dispatchRequest.getMsgSize();

                            if (dispatchRequest.isSuccess()) {
                                // 如果消息的长度大于 0，则调用 doDisptach 方法，最终会分别调用 CommitLogDispatcherBuildConsumeQueue（构建消息消费队列文件）
                                // 和 CommitLogDispatcherBuildIndex（构建消息索引文件）
                                if (size > 0) {
                                    DefaultMessageStore.this.doDispatch(dispatchRequest);

                                    // 如果开启了长轮询机制，那么就会通知阻塞的线程有新的消息到达
                                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                        && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()) {
                                        DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                            dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                                            dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                                            dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
                                    }

                                    this.reputFromOffset += size;
                                    readSize += size;
                                    if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        DefaultMessageStore.this.storeStatsService
                                            .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).incrementAndGet();
                                        DefaultMessageStore.this.storeStatsService
                                            .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                            .addAndGet(dispatchRequest.getMsgSize());
                                    }
                                } else if (size == 0) {
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    readSize = result.getSize();
                                }
                            } else if (!dispatchRequest.isSuccess()) {

                                if (size > 0) {
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                    this.reputFromOffset += size;
                                } else {
                                    doNext = false;
                                    if (DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]the master dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                            this.reputFromOffset);

                                        this.reputFromOffset += result.getSize() - readSize;
                                    }
                                }
                            }
                        }
                    } finally {
                        result.release();
                    }
                } else {
                    doNext = false;
                }
            }
        }

        // ReputMessageService#run
        @Override
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // ReputMessageService 线程每执行一次任务推送就休息 1 毫秒就继续尝试推送消息到消息消费队列 ConsumeQueue 文件
                    // 和索引文件 IndexFile，消息消费转发的核心实现在 doReput 方法中实现
                    Thread.sleep(1);
                    this.doReput();
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }

    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // 将消息保存到消息队列 ConsumeQueue 中
                    DefaultMessageStore.this.putMessagePositionInfo(request);
                    break;
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
            }
        }
    }

    class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {
        @Override
        public void dispatch(DispatchRequest request) {
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                // 根据消息，更新消息索引文件
                DefaultMessageStore.this.indexService.buildIndex(request);
            }
        }
    }

    public static class CommitLog {
        
        // topic-queueId -> offset
        private HashMap<String, Long> topicQueueTable = new HashMap<String, Long>(1024);

        // CommitLog#CommitLog
        public CommitLog(final DefaultMessageStore defaultMessageStore) {
            this.mappedFileQueue = new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog(),
                defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog(), defaultMessageStore.getAllocateMappedFileService());
            this.defaultMessageStore = defaultMessageStore;

            // 如果是同步刷盘策略，那么 flushCommitLogService 就初始化为 GroupCommitService
            if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                this.flushCommitLogService = new GroupCommitService();
            // 如果是异步刷盘策略，flushCommitLogService 初始化为 FlushRealTimeService
            } else {
                this.flushCommitLogService = new FlushRealTimeService();
            }

            this.commitLogService = new CommitRealTimeService();

            this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            batchEncoderThreadLocal = new ThreadLocal<MessageExtBatchEncoder>() {
                @Override
                protected MessageExtBatchEncoder initialValue() {
                    return new MessageExtBatchEncoder(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
                }
            };
            this.putMessageLock = defaultMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? 
                new PutMessageReentrantLock() : new PutMessageSpinLock();
        }

        // CommitLog#start
        public void start() {
            this.flushCommitLogService.start();
            if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                this.commitLogService.start();
            }
        }

        public boolean load() {
            boolean result = this.mappedFileQueue.load();
            log.info("load commit log " + (result ? "OK" : "Failed"));
            return result;
        }

        // CommitLog#doDispatch
        public void doDispatch(DispatchRequest req) {
            for (CommitLogDispatcher dispatcher : this.dispatcherList) {
                dispatcher.dispatch(req);
            }
        }

        class DefaultAppendMessageCallback implements AppendMessageCallback {

            private final StringBuilder keyBuilder = new StringBuilder();

            private final StringBuilder msgIdBuilder = new StringBuilder();
            // 用来保存 Broker 的 host 信息，也就是 ip + port
            private final ByteBuffer hostHolder = ByteBuffer.allocate(8);
            // 用来保存消息的 id
            private final ByteBuffer msgIdMemory;
            // Store the message content
            // 用来保存消息的实际内容
            private final ByteBuffer msgStoreItemMemory;

            // DefaultAppendMessageCallback#doAppend
            public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank, 
                        final MessageExtBrokerInner msgInner) {
                // 写入消息的偏移量 wroteOffset 为：这个 MappedFile 最开始的偏移量 + position
                long wroteOffset = fileFromOffset + byteBuffer.position();

                this.resetByteBuffer(hostHolder, 8);
                // 创建一个全局唯一的消息 id，这个消息 id 有 16 个字节。这 16 个字节的组成如下：
                // 4 字节的 IP + 4 字节的端口号地址 + 8 字节的消息偏移量
                String msgId = MessageDecoder.createMessageId(this.msgIdMemory, msgInner.getStoreHostBytes(hostHolder), wroteOffset);

                // Record ConsumeQueue information
                keyBuilder.setLength(0);
                keyBuilder.append(msgInner.getTopic());
                keyBuilder.append('-');
                keyBuilder.append(msgInner.getQueueId());
                String key = keyBuilder.toString();

                // 获取该主题的消息在消息队列的偏移量，CommitLog 的 topicQueueTable 中保存了当前所有消息队列的当前写入偏移量
                // 这个 key 是 topic-queueId
                Long queueOffset = CommitLog.this.topicQueueTable.get(key);
                if (null == queueOffset) {
                    queueOffset = 0L;
                    // 如果没找到对应主题的消息在消息队列的偏移量，就说明是一个新的消息队列，所以 queueOffset 为 0
                    // 这里的偏移量 queueOffset 是一个逻辑偏移量，表示 ConsumeQueue 中消息的个数
                    CommitLog.this.topicQueueTable.put(key, queueOffset);
                }

                /**
                 *  接下来对消息 msgInner 进行序列化操作，也就是转变为字节数组 
                 */

                // 将 msg 中的 propertiesString 序列化为字节数组
                final byte[] propertiesData = msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);
                final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

                if (propertiesLength > Short.MAX_VALUE) {
                    log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                    return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
                }

                // 将 msg 中的主题 topic 序列化为字节数组
                final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
                final int topicLength = topicData.length;
                // 将 msg 的消息主体 body 序列化为字节数组
                final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

                final int msgLen = calMsgLength(bodyLength, topicLength, propertiesLength);

                // Determines whether there is sufficient free space
                // 判断 commitlog 剩下的空间是否可以容纳 消息的长度 + 8 个字节的系统信息
                // 如果大于则返回 AppendMessageStatus.END_OF_FILE，Broker 会重新创建一个新的 CommitLog 文件来存储该消息，
                // 从这里也可以看出来 CommitLog 最少会空闲 8 个字节，高 4 个字节会存储当前文件的剩余空间，低 4 个字节会存储魔数
                if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                    // 1 TOTALSIZE 剩余空间
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE 魔数
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId,
                            msgInner.getStoreTimestamp(), queueOffset,
                            CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }

                // Initialization of storage space
                this.resetByteBuffer(msgStoreItemMemory, msgLen);
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(msgLen);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
                // 4 QUEUEID
                this.msgStoreItemMemory.putInt(msgInner.getQueueId());
                // 5 FLAG
                this.msgStoreItemMemory.putInt(msgInner.getFlag());
                // 6 QUEUEOFFSET
                this.msgStoreItemMemory.putLong(queueOffset);
                // 7 PHYSICALOFFSET
                this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
                // 8 SYSFLAG
                this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
                // 9 BORNTIMESTAMP
                this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
                // 10 BORNHOST
                this.resetByteBuffer(hostHolder, 8);
                this.msgStoreItemMemory.put(msgInner.getBornHostBytes(hostHolder));
                // 11 STORETIMESTAMP
                this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                this.resetByteBuffer(hostHolder, 8);
                this.msgStoreItemMemory.put(msgInner.getStoreHostBytes(hostHolder));
                // this.msgBatchMemory.put(msgInner.getStoreHostBytes());
                // 13 RECONSUMETIMES
                this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
                // 14 Prepared Transaction Offset
                this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
                // 15 BODY
                this.msgStoreItemMemory.putInt(bodyLength);
                if (bodyLength > 0)
                    this.msgStoreItemMemory.put(msgInner.getBody());
                // 16 TOPIC
                this.msgStoreItemMemory.put((byte) topicLength);
                this.msgStoreItemMemory.put(topicData);
                // 17 PROPERTIES
                this.msgStoreItemMemory.putShort((short) propertiesLength);
                if (propertiesLength > 0)
                    this.msgStoreItemMemory.put(propertiesData);

                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                // Write messages to the queue buffer
                // 将消息写入到 ByteBuffer 中，然后创建 AppendMessageResult。这里只是将消息存储到 MappedFile 对应的磁盘映射中，并没有刷到磁盘中
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);
                
                AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId, msgInner.getStoreTimestamp(), 
                        queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);

                switch (tranType) {
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    // 将对应 topic 和 queueId 的消息队列的 queueOfffset 的数目加 1
                    CommitLog.this.topicQueueTable.put(key, ++queueOffset);
                    break;
                default:
                    break;
                }
                
                return result;
            }

        }

        // 这也就表明 CommitLog 中每一个条目长度是不固定的，每一个条目的长度存储在前 4 个字节中
        private static int calMsgLength(int bodyLength, int topicLength, int propertiesLength) {
            final int msgLen = 4 //TOTALSIZE
                + 4 //MAGICCODE
                + 4 //BODYCRC
                + 4 //QUEUEID
                + 4 //FLAG
                + 8 //QUEUEOFFSET
                + 8 //PHYSICALOFFSET
                + 4 //SYSFLAG
                + 8 //BORNTIMESTAMP
                + 8 //BORNHOST
                + 8 //STORETIMESTAMP
                + 8 //STOREHOSTADDRESS
                + 4 //RECONSUMETIMES
                + 8 //Prepared Transaction Offset
                + 4 // 消息体的长度
                + (bodyLength > 0 ? bodyLength : 0) // 消息体的实际内容
                + 1 // 消息主题的长度 
                + topicLength // 消息主题实际内容
                + 2 // 消息的属性的长度
                + (propertiesLength > 0 ? propertiesLength : 0) // 消息的附属属性的长度
                + 0;
            return msgLen;
        }

        public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
            // Set the storage time
            msg.setStoreTimestamp(System.currentTimeMillis());
            // Set the message body BODY CRC (consider the most appropriate setting on the client)
            msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
            // Back to Results
            AppendMessageResult result = null;

            StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();
            String topic = msg.getTopic();
            int queueId = msg.getQueueId();

            if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
                // Delay Delivery
                if (msg.getDelayTimeLevel() > 0) {
                    // 如果消息的延迟级别超过最大的延迟级别的话（默认为 32），就将其设置为 32
                    if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                        msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                    }

                    // 定时消息的主题统一设置为 SCHEDULE_TOPIC_XXX
                    topic = ScheduleMessageService.SCHEDULE_TOPIC;
                    // 定时消息的消息队列为: delayLevel - 1
                    queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                    // 将这个消息的原主题和原来的消息队列 id 存入到消息的属性中，进行备份
                    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID,String.valueOf(msg.getQueueId()));
                    msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
                    
                    msg.setTopic(topic);
                    msg.setQueueId(queueId);
                }
            }

            long eclipseTimeInLock = 0;
            MappedFile unlockMappedFile = null;

            // 获取到现在可以写入的 CommitLog 文件，其实也就是获取到 mappedFileQueue 中的最后一个 MappedFile
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

            // 在往 CommitLog 中写入消息之前，先申请一个写锁 putMessageLock，这也就说明将消息写入到 CommitLog 是串行的
            putMessageLock.lock(); 
            try {
                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                this.beginTimeInLock = beginLockTimestamp;

                // 设置消息的存储时间
                msg.setStoreTimestamp(beginLockTimestamp);

                // 1.如果 mappedFile 为空，说明本次消息是第一次消息发送，用偏移量 0 创建第一个 commitlog 文件，文件名为 0000 0000 0000 0000 0000，
                // 如果创建文件失败，抛出 CREATE_MAPEDFILE_FAILED 异常，可能是权限不够或者磁盘空间不足
                // 2.如果 mappedFile 已经满了的话，创建一个新的 mappedFile，这个新 mappedFile 的 createOffset 也就是初始偏移量是 
                // mappedFileLast.getFileFromOffset() + this.mappedFileSize
                if (null == mappedFile || mappedFile.isFull()) {
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
                }

                if (null == mappedFile) {
                    log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
                }

                result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    // 上一个 MappedFile 已经满了，创建一个新的 MappedFile，并且将消息重新 append 到新的 MappedFile
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                }

                eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                beginTimeInLock = 0;
            } finally{
                putMessageLock.unlock();
            }

            if (eclipseTimeInLock > 500) {
                log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, msg.getBody().length, result);
            }
    
            if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
                this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
            }
    
            // result 的类型为 AppendMessageResult
            PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);
    
            // Statistics
            storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
            storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());
    
            // DefaultAppendMessageCallback#doAppend 是将消息追加在内存中， 需要根据是同步刷盘还是异步刷盘方式，
            // 将内存中的数据持久化到磁盘
            handleDiskFlush(result, putMessageResult, msg);
            // 然后执行 HA 主从同步复制
            handleHA(result, putMessageResult, msg);
    
            return putMessageResult;
        }

        // 同步刷盘和异步刷盘
        public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
            // Synchronization flush
            // 同步刷盘，是指在消息追加到内存映射文件的内存之后，立即将数据刷写到磁盘
            // 同步刷盘的实现方式类似于 MappedFile 创建，即构造刷盘请求 GroupCommitRequest 写入请求队列，由异步线程 GroupCommitService 消费请求
            if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
                if (messageExt.isWaitStoreMsgOK()) {
                    // 1.构建 GroupCommitRequest 同步任务并提交到 GroupCommitService
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                    service.putRequest(request);
                    // 2.waitForFlush 会阻塞直到刷盘任务完成，如果超时则返回刷盘错误， 刷盘成功后正常返回给调用方
                    boolean flushOK = request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    if (!flushOK) {
                        log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags()
                            + " client address: " + messageExt.getBornHostString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }
                } else {
                    service.wakeup();
                }
            }
            // Asynchronous flush
            // 异步刷盘
            else {
                // 如果 isTransientStorePoolEnable 为 false1，唤醒 FlushCommitLogService 线程直接将 mappedByteBuffer 中的内容提交
                if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    flushCommitLogService.wakeup();
                // 如果 isTransientStorePoolEnable 为 true，唤醒 CommitRealTimeService 线程将 writeBuffer 中的数据 commit 至 fileChannel
                // 虽然这里没有唤醒 flushCommitLogService，但是在 commitLogService 线程中，将 writeBuffer 中的数据提交后会自行唤醒 flushCommitLogService
                } else {
                    commitLogService.wakeup();
                }
            }
        }

        // 获取当前 commitlog 目录的最小偏移量，首先获取目录下的第一个文件，如果该文件可用，那么返回该文件的起始偏移量，
        // 否则返回下一个文件的起始偏移量
        public long getMinOffset() {
            MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
            if (mappedFile != null) {
                if (mappedFile.isAvailable()) {
                    return mappedFile.getFileFromOffset();
                } else {
                    return this.rollNextFile(mappedFile.getFileFromOffset());
                }
            }
            return -1;
        }

        // 根据偏移量与消息长度查找消息
        public SelectMappedBufferResult getMessage(final long offset, final int size) {
            // 获取一个 commitlog 文件的大小
            int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
            // 根据 offset 找到这个偏移量所在的 mappedFile 或者说 commitlog
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
            if (mappedFile != null) {
                // 取余得到文件内部的偏移量 pos，然后从该偏移量 pos 开始读取 size 个字节长度的内容就可以返回
                int pos = (int) (offset % mappedFileSize);
                return mappedFile.selectMappedBuffer(pos, size);
            }
            return null;
        }

        public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC, final boolean readBody) {
            try {
                // 1 TOTAL SIZE 先从 ByteBuffer 中获取到这条消息的总长度
                int totalSize = byteBuffer.getInt();
                // 2 MAGIC CODE
                int magicCode = byteBuffer.getInt();
                switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
                }

                byte[] bytesContent = new byte[totalSize];

                // 从 byteBuffer 中读出存储的 msg 的各个属性
                int bodyCRC = byteBuffer.getInt();
                int queueId = byteBuffer.getInt();
                int flag = byteBuffer.getInt();
                long queueOffset = byteBuffer.getLong();
                long physicOffset = byteBuffer.getLong();
                int sysFlag = byteBuffer.getInt();
                long bornTimeStamp = byteBuffer.getLong();
                ByteBuffer byteBuffer1 = byteBuffer.get(bytesContent, 0, 8);
                long storeTimestamp = byteBuffer.getLong();
                ByteBuffer byteBuffer2 = byteBuffer.get(bytesContent, 0, 8);
                int reconsumeTimes = byteBuffer.getInt();
                long preparedTransactionOffset = byteBuffer.getLong();
                int bodyLen = byteBuffer.getInt();

                if (bodyLen > 0) {
                    if (readBody) {
                        byteBuffer.get(bytesContent, 0, bodyLen);
                        if (checkCRC) {
                            int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                            if (crc != bodyCRC) {
                                log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                                return new DispatchRequest(-1, false/* success */);
                            }
                        }
                    } else {
                        byteBuffer.position(byteBuffer.position() + bodyLen);
                    }
                }

                byte topicLen = byteBuffer.get();
                byteBuffer.get(bytesContent, 0, topicLen);
                String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

                long tagsCode = 0;
                String keys = "";
                String uniqKey = null;

                short propertiesLength = byteBuffer.getShort();
                Map<String, String> propertiesMap = null;
                if (propertiesLength > 0) {
                    byteBuffer.get(bytesContent, 0, propertiesLength);
                    String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                    propertiesMap = MessageDecoder.string2messageProperties(properties);

                    keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);
                    uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

                    String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                    if (tags != null && tags.length() > 0) {
                        // 从消息的 properties 属性中获取到消息的标签 tags 的 hashcode，其实就是返回 tags.hashcode() 的值
                        tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                    }

                    // Timing message processing
                    {
                        String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                        if (ScheduleMessageService.SCHEDULE_TOPIC.equals(topic) && t != null) {
                            int delayLevel = Integer.parseInt(t);

                            if (delayLevel > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                                delayLevel = this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel();
                            }

                            if (delayLevel > 0) {
                                tagsCode = this.defaultMessageStore.getScheduleMessageService().computeDeliverTimestamp(delayLevel, storeTimestamp);
                            }
                        }
                    }
                }

                int readLength = calMsgLength(bodyLen, topicLen, propertiesLength);
                if (totalSize != readLength) {
                    doNothingForDeadCode(reconsumeTimes);
                    doNothingForDeadCode(flag);
                    doNothingForDeadCode(bornTimeStamp);
                    doNothingForDeadCode(byteBuffer1);
                    doNothingForDeadCode(byteBuffer2);
                    log.error("[BUG]read total count not equals msg total size");
                    return new DispatchRequest(totalSize, false/* success */);
                }

                return new DispatchRequest(topic, queueId, physicOffset, totalSize, tagsCode, storeTimestamp,
                        queueOffset, keys, uniqKey, sysFlag, preparedTransactionOffset, propertiesMap);
            } catch (Exception e) {
            }

            return new DispatchRequest(-1, false /* success */);
        }

    }

    public static class GroupCommitRequest {
        // 刷盘点偏移量
        private final long nextOffset;
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        // 刷盘结果，初始状态为 false
        private volatile boolean flushOK = false;

        // GroupCommitService 线程处理 GroupCommitRequest 对象后将调用 wakeupCustomer 法将消费发送线程唤醒，并将刷盘的结果告知 GroupCommitRequest
        // 也就是将 flushOK 的结果保存到 GroupCommitRequest 中去
        public void wakeupCustomer(final boolean flushOK) {
            this.flushOK = flushOK;
            this.countDownLatch.countDown();
        }

        // 消费发送线程将消息追加到内存映射文件后，将同步任务 GroupCommitRequest 提交到 GroupCommitService 线程，然后调用阻塞等待刷盘结果，
        // 超时时间默认 5s
        public boolean waitForFlush(long timeout) {
            try {
                this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                return this.flushOK;
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
                return false;
            }
        }
    }

    // 为了避免刷盘请求 GroupCommitRequest 的锁竞争，GroupCommitService 线程维护了 GroupCommitRequest 
    // 读队列 requestsRead 和写队列 requestsWrite，GroupCommitRequest 的提交和消费互不阻塞。当 GroupCommitService 
    // 线程消费完 requestsRead 队列后，清空 requestsRead，交换 requestsRead 和 requestsWrite
    class GroupCommitService extends FlushCommitLogService {
        // 同步刷盘任务暂存器
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        // GroupCommitService 线程每次处理的 request 容器，这是一个设计亮点，避免了任务提交与任务执行的锁冲突
        // 在 putRequest 将创建的任务提交到 GroupCommitService 中时，是存入到 requestsWrite 列表中，但是在 doCommit 中
        // 真正是从 requestsRead 列表中读取任务进行刷盘操作，这样就避免了刷盘任务提交与刷盘任务具体执行的冲突
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();

        public synchronized void putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                // 如果线程处于等待状态，则将其唤醒
                waitPoint.countDown(); // notify
            }
        }

        // 由于避免同步刷盘消费任务与其他消息生产者提交任务直接的锁竞争，GroupCommitrvice 提供读容器与写容器，这两个容器每执行完一次任务后，
        // 交互，继续消费
        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doCommit() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    // 1.遍历同步刷盘任务列表，根据加入顺序逐一执行刷盘逻辑
                    for (GroupCommitRequest req : this.requestsRead) {
                        // There may be a message in the next file, so a maximum of two times the flush
                        boolean flushOK = false;
                        // 2.调用 mappedFileQueue#flush 方法执行刷盘操作，最终会调用 MappedByteBuffer#force 方法。
                        // 如果已刷盘指针大于等于提交的刷盘点，表示刷盘成功，每执行一次刷盘操作后，立即调用 GroupCommitRequest#wakeupCustomer
                        // 唤醒消息发送线程并通知刷盘结果
                        for (int i = 0; i < 2 && !flushOK; i++) {
                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                            if (!flushOK) {
                                CommitLog.this.mappedFileQueue.flush(0);
                            }
                        }
                        req.wakeupCustomer(flushOK);
                    }

                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    // 3.处理完所有的同步刷盘任务之后，更新刷盘检测点 StoreCheckpoint 中的 physicMsgTimestamp，
                    // 但并没有执行检测点的刷盘操作，刷盘检测点的刷盘操作将在写消息队列文件时触发
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    this.requestsRead.clear();
                } else {
                    // Because of individual messages is set to not sync flush, it will come to this process
                    CommitLog.this.mappedFileQueue.flush(0);
                }
            }
        }

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            // GroupCommitService 每处理一批同步刷盘请求（也就是 requestsRead 容器中的刷盘请求）后"休息" 0ms，然后继续处理下一批，
            // 其任务的核心实现为 doCommit 方法
            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    public class MappedFile extends ReferenceResource{
        // 操作系统每页的大小，默认为 4KB
        public static final int OS_PAGE_SIZE = 1024 * 4;
        private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);
        // 当前 JVM 实例中 MappedFile 对象的个数
        private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
        // 当前文件的写指针（内存映射文件中的写指针），从 0 开始
        protected final AtomicInteger wrotePosition = new AtomicInteger(0);
        // ADD BY ChenYang
        protected final AtomicInteger committedPosition = new AtomicInteger(0);
        // 刷写到磁盘指针，该指针之前的数据已经被刷写到磁盘中
        private final AtomicInteger flushedPosition = new AtomicInteger(0);
        // 文件大小
        protected int fileSize;
        // 文件通道
        protected FileChannel fileChannel;
        /**
         * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
         * 如果开启了 transientStorePool 的话，数据首先应该存储在该 writeBuffer 中，然后提交到 MappedFile 对应的内存映射文件
         */
        protected ByteBuffer writeBuffer = null;
        protected TransientStorePool transientStorePool = null;
        // 文件名
        private String fileName;
        // 该文件的初始偏移量
        private long fileFromOffset;
        // 物理文件
        private File file;
        private MappedByteBuffer mappedByteBuffer;
        // 文件上一次写入内容的时间
        private volatile long storeTimestamp = 0;
        // 是否是 MappedFileQueue 队列的第一个文件
        private boolean firstCreateInQueue = false;

        // MappedFile#appendMessage
        public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
            return appendMessagesInner(msg, cb);
        }

        // 将消息追加到 MappedFile 中
        // MappedFile#appendMessageInner
        public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {  
            // 先获取 MappedFile 当前的写指针  
            int currentPos = this.wrotePosition.get();

            // 如果 currentPos 大于或者等于文件大小，则表明文件已经写满，会抛出 AppendMessageStatus.UNKNOWN_ERROR 异常
            if (currentPos < this.fileSize) {
                // 如果 currentPos 小于文件大小，通过 slice() 方法创建一个与 MappedFile 的共享共存区，并设置 position 为当前指针
                // 如果 writeBuffer 为 null，则表明 transientStorePoolEnable 为 false，数据直接写入到 mappedByteBuffer 中
                // 否则的话。消息写入到 writeBuffer 中
                ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
                byteBuffer.position(currentPos);
                AppendMessageResult result = null;

                if (messageExt instanceof MessageExtBrokerInner) {
                    result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
                } else if (messageExt instanceof MessageExtBatch) {
                    result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
                } else {
                    return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                }

                this.wrotePosition.addAndGet(result.getWroteBytes());
                this.storeTimestamp = result.getStoreTimestamp();
                return result;
            }

            log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
            return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
        }

        public MappedFile(final String fileName, final int fileSize) throws IOException {
            init(fileName, fileSize);
        }

        public MappedFile(final String fileName, final int fileSize, final TransientStorePool transientStorePool)throws IOException {
            init(fileName, fileSize, transientStorePool);
        }

        public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool)throws IOException {
            init(fileName, fileSize);
            this.writeBuffer = transientStorePool.borrowBuffer();
            this.transientStorePool = transientStorePool;
        }

        /**
         * 根据是否开启 transientStorePoolEnable 存在两种初始化情况。
         * 
         * transientStorePoolEnable 为 true 表明先将内容存储到对外内存，也就是 MappedFile 对象的 writeBuffer 中（writeBuffer 就是从 transientStorePool
         * 对象中得到的）。然后在 commit 的时候将 writeBuffer 中的内容写入到 fileChannel 中去（这个时候由于内核的缓存机制，真正写入到 fileChannel 中的数据
         * 可能已经写入到磁盘中，也可能还存在于内核缓冲区中），最后在 flush 的时候，强制将其刷到磁盘中。
         * 
         * transientStorePoolEnable 为 false 的时候，则把内容直接写入到 mappedByteBuffer 中，而 mappedByteBuffer 是 fileChannel 在内存中的映射，可以看成是
         * 直接写入到磁盘上的文件里面，所以不需要提交（这一点可以从 MappedFile#commit 方法看出来），然后在 flush 的时候，将所有内容强制刷入到磁盘中。
         */
        private void init(final String fileName, final int fileSize) throws IOException {
            this.fileName = fileName;
            this.fileSize = fileSize;
            this.file = new File(fileName);
            // fileFromOffset 是文件的初始偏移量，和文件名相同
            this.fileFromOffset = Long.parseLong(this.file.getName());
            boolean ok = false;

            ensureDirOK(this.file.getParent());

            try {
                // Java NIO中的FileChannel是一个连接到文件的通道。可以通过文件通道读写文件，fileChannel 无法设置为非阻塞模式，它总是运行在阻塞模式下
                // 这里获取到指定文件 file 的 fileChannel
                this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
                // 将文件内容使用 NIO 的内存映射 Buffer 映射到内存中
                // MappedByteBuffer 是Java NIO中引入的一种硬盘物理文件和内存映射方式，当磁盘上的物理文件较大时，采用MappedByteBuffer，读写性能较高，
                // 内部的核心实现是DirectByteBuffer(JVM 堆外直接物理内存)。由于 commitlog 文件的大小默认为 1G，比较大，所以使用 mappedByteBuffer 加快
                // 消息写入磁盘上的 commitlog 文件的速度
                this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
                TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
                TOTAL_MAPPED_FILES.incrementAndGet();
                ok = true;
            } catch (FileNotFoundException e) {
                log.error("create file channel " + this.fileName + " Failed. ", e);
                throw e;
            } catch (IOException e) {
                log.error("map file " + this.fileName + " Failed. ", e);
                throw e;
            } finally {
                if (!ok && this.fileChannel != null) {
                    this.fileChannel.close();
                }
            }
        }

        public void mlock() {
            final long beginTime = System.currentTimeMillis();
            final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
            // pointer 指向 mappedByteBuffer 所代表的内存
            Pointer pointer = new Pointer(address);

            {
                // 调用 mlock 方法将 mappedByteBuffer 所代表的内存区域进行锁定，防止 OS 将内存交换到 swap 空间上去
                // 内存的 page input/page output 可能会耗费很多时间
                int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
                log.info("mlock {} {} {} ret = {} time consuming = {}");
            }

            {
                // 调用 madvise 方法，madvise会向内核提供一个针对于于地址区间的I/O的建议，内核可能会采纳这个建议，
                // 会做一些预读的操作。RocketMQ采用的是MADV_WILLNEED模式，它的效果是，对所有当前不在内存中的数据进行页面调度
                int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
                log.info("madvise {} {} {} ret = {} time consuming = {}");
            }
        }

        /**
         * 1.对当前映射文件进行预热
         * 第一步：对当前映射文件的每个内存页写入一个字节0.当刷盘策略为同步刷盘时，执行强制刷盘，并且是每修改pages(默认是16MB)个分页刷一次盘
         * 第二步：将当前全部的地址空间锁定在物理存储中，防止其被交换到swap空间。再调用，传入 MADV_WILLNEED 策略，将刚刚锁住的内存预热，
         * 其实就是告诉内核，我马上就要用（MADV_WILLNEED）这块内存，先做虚拟内存到物理内存的映射，防止正式使用时产生缺页中断。
         * 
         * 2.使用mmap()内存分配时，只是建立了进程虚拟地址空间，并没有分配虚拟内存对应的物理内存。当进程访问这些没有建立映射关系的虚拟内存时，
         * 处理器自动触发一个缺页异常，进而进入内核空间分配物理内存、更新进程缓存表，最后返回用户空间，恢复进程运行。
         * 写入假值0的意义在于实际分配物理内存，在消息写入时防止缺页异常。
         */
        public void warmMappedFile(FlushDiskType type, int pages) {
            long beginTime = System.currentTimeMillis();
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            int flush = 0;
            long time = System.currentTimeMillis();
            for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
                byteBuffer.put(i, (byte) 0);
                // force flush when flush disk type is sync
                if (type == FlushDiskType.SYNC_FLUSH) {
                    if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                        flush = i;
                        mappedByteBuffer.force();
                    }
                }
    
                // prevent gc
                if (j % 1000 == 0) {
                    log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                    time = System.currentTimeMillis();
                    try {
                        Thread.sleep(0);
                    } catch (InterruptedException e) {
                        log.error("Interrupted", e);
                    }
                }
            }
    
            // force flush when prepare load finished
            if (type == FlushDiskType.SYNC_FLUSH) {
                log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                    this.getFileName(), System.currentTimeMillis() - beginTime);
                mappedByteBuffer.force();
            }
            log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);
    
            this.mlock();
        }

        // MappedFile#commit
        public int commit(final int commitLeastPages) {
            // 如果 writeBuffer 为空，说明是 transientStorePoolEnable 为 false，也就是说消息内容是直接写入到 mappedByteBuffer 中，
            // 所以不需要进行提交
            if (writeBuffer == null) {
                // no need to commit data to file channel, so just regard wrotePosition as committedPosition.
                return this.wrotePosition.get();
            }
            // commitLeastPages 为本次提交最小的页数，如果待提交数据不满 commitLeastPages ，则不执行本次提交操作，待下次提交
            if (this.isAbleToCommit(commitLeastPages)) {
                if (this.hold()) {
                    commit0(commitLeastPages);
                    this.release();
                } else {
                    log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
                }
            }
            // All dirty data has been committed to FileChannel.
            if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
                this.transientStorePool.returnBuffer(writeBuffer);
                this.writeBuffer = null;
            }
            return this.committedPosition.get();
        }

        // MappedFile#commit0
        protected void commit0(final int commitLeastPages) {
            int writePos = this.wrotePosition.get();
            int lastCommittedPosition = this.committedPosition.get();

            // 把 commitedPosition 到 wrotePosition 之间的数据写入到 FileChannel 中， 然后更新 committedPosition 指针为
            // wrotePosition。commit 的作用就是将 MappedFile#writeBuffer 中的数据提交到文件通道 FileChannel 中
            if (writePos - this.committedPosition.get() > 0) {
                try {
                    // 创建 writeBuffer 的共享缓存区。slice 方法创建一个共享缓冲区，与原先的 ByteBuffer 共享内存，但是维护一套独立的指针
                    ByteBuffer byteBuffer = writeBuffer.slice();
                    byteBuffer.position(lastCommittedPosition);
                    byteBuffer.limit(writePos);

                    this.fileChannel.position(lastCommittedPosition);
                    this.fileChannel.write(byteBuffer);
                    this.committedPosition.set(writePos);
                } catch (Throwable e) {
                    log.error("Error occurred when commit data to FileChannel.", e);
                }
            }
        }

        protected boolean isAbleToCommit(final int commitLeastPages) {
            // 得到 mappedFile 的 flush 指针和 write 指针，一般来说 flush <= write
            int flush = this.committedPosition.get();
            int write = this.wrotePosition.get();
    
            if (this.isFull()) {
                return true;
            }
    
            // commitLeastPages 小于 0 的话，就表示只要存在脏页就提交
            if (commitLeastPages > 0) {
                // write 指针减去 flush 指针的值，除以 OS_PAGE_SIZE 的值，就是当前脏页的数量
                // commitLeastPages 为本次提交最小的页数，如果待提交数据不满 commitLeastPages ，则不执行本次提交操作，待下次提交
                return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
            }
    
            return write > flush;
        }

        /**
         * @return The max position which have valid data
         * 
         * 返回有效数据的指针。在 MappedFile 设计中，只有提交了的数据（写入到 MappedByteBuffer 或者 FileChannel 中的数据）才是安全有效的数据;
         * 
         * 如果 writeBuffer 为空，则直接返回当前的写指针;如果 writeBuffer 是空，就说明 transientStorePoolEnable 为 false，消息内容是直接写入到 mappedByteBuffer 中，
         * 也就是说可以看成是直接写入到磁盘文件中，所以写入的数据都可以看成是合法的数据
         * 如果 writeBuffer 不为空，则返回上一次提交的指针。
         */
        public int getReadPosition() {
            return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
        }

        public int flush(final int flushLeastPages) {
            if (this.isAbleToFlush(flushLeastPages)) {
                if (this.hold()) {
                    int value = getReadPosition();
    
                    try {
                        // We only append data to fileChannel or mappedByteBuffer, never both.
                        // 刷写磁盘，直接调用 mappedByteBuffer 或者 fileChannel 的 force 方法将内存中数据持久化到磁盘
                        // 这里的刷盘分为两种情况，一种就是 trasientPoolEnable 为 true 的话，这种情况下，消息是先写入到 writeBuffer 中，随后再提交到
                        // fileChannel 中，所以 if 判断这两个地方是否有数据。第二种情况是 trasientPoolEnable 为 false 的情况，在这种情况下，会直接使用
                        // mappedByteBuffer，而不会使用 fileChannel
                        if (writeBuffer != null || this.fileChannel.position() != 0) {
                            this.fileChannel.force(false);
                        } else {
                            this.mappedByteBuffer.force();
                        }
                    } catch (Throwable e) {
                        log.error("Error occurred when force data to disk.", e);
                    }
    
                    this.flushedPosition.set(value);
                    this.release();
                } else {
                    log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                    this.flushedPosition.set(getReadPosition());
                }
            }
            return this.getFlushedPosition();
        }

        /**
         * 先介绍一下，NIO 中 slice 方法的作用。java.nio.ByteBuffer类的slice()方法用于创建一个新的字节缓冲区，其内容是给定缓冲区内容的共享子序列。
         * 新缓冲区的内容将从该缓冲区的当前位置（也就是 position）开始。对该缓冲区内容的更改将在新缓冲区中可见，反之亦然。这两个缓冲区的位置，限制和标记值将是独立的。
         * 新缓冲区的位置（position）将为零，其容量（capacity）和限制（limit）将为该缓冲区中剩余的空间。当且仅当该缓冲区是直接缓冲区时，新缓冲区才是直接缓冲区；
         * 当且仅当该缓冲区是只读缓冲区时，新缓冲区才是只读缓冲区。
         * 
         * 查找从 pos 开始，size 个大小的数据，由于在整个写入期间都未曾改变 MappedByteBuffer 的指针（注意，在 appendMessagesInner 方法中，
         * 获取到的 byteBuffer 是 writeBuffer 或者 mappedByteBuffer 的 slice 片段，也就是说 slice 方法新得到的 ByteBuffer 区域的 position
         * 和 limit 指针和原始的 writeBuffer 以及 mappedByteBuffer 区域是相互独立的）。
         * 
         * 所以 mappedByteBuffer.slice() 法返回的共享缓存区空间为整个 MappedFile ，然后通过设置 byteBuffer 的 position 为待查找的值，再次通过 slice 
         * 方法得到的 byteBufferNew 的就是 byteBuffer 从 pos 开始往后的区域，byteBufferNew 的 position 为  0，而 limit 为 size，也就说明
         * byteBufferNew 中可以读取到 size 个字节的数据，也就是 byteBuffer 中从 pos 开始往后 size 个字节的数据。
         */
        public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
            int readPosition = getReadPosition();
            if ((pos + size) <= readPosition) {
    
                if (this.hold()) {
                    ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                    byteBuffer.position(pos);
                    ByteBuffer byteBufferNew = byteBuffer.slice();
                    byteBufferNew.limit(size);
                    return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
                } else {
                    log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
                }
            } else {
                log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
            }
    
            return null;
        }
    

    }

    /**
     * TransientStorePool 短暂的存储池。RocketMQ 在 MappedFile 中从 TransientStorePool 对象中获取（borrow）一个内存缓存池，
     * 其实也就是一个 ByteBuffer 对象，用来临时存储数据，数据先写入该 ByteBuffer 中，然后由 commit 线程定时将数据从该内存复制到与目的物理文件对应的内存映射中，
     * 也就是从 ByteBuffer 写入到 fileChannel 中。然后再在 flush 线程中，将 fileChannel 中的数据写入到磁盘上
     */
    public class TransientStorePool {
        // availableBuffers 中 ByteBuffer 的个数
        private final int poolSize;
        // 每个 ByteBuffer 的大小
        private final int fileSize;
        // 双端队列
        private final Deque<ByteBuffer> availableBuffers;

        private final MessageStoreConfig storeConfig;

        public TransientStorePool(final MessageStoreConfig storeConfig) {
            this.storeConfig = storeConfig;
            this.poolSize = storeConfig.getTransientStorePoolSize();
            this.fileSize = storeConfig.getMapedFileSizeCommitLog();
            this.availableBuffers = new ConcurrentLinkedDeque<>();
        }

        public void init() {
            for (int i = 0; i < poolSize; i++) {
                // 创建 poolSize 个堆外内存
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);
                final long address = ((DirectBuffer) byteBuffer).address();
                Pointer pointer = new Pointer(address);

                // 并利用 com.sun.jna.Library 类库将该批内存锁定，避免被置换到交换区，提高存储性能
                LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));
    
                availableBuffers.offer(byteBuffer);
            }
        }

        public void returnBuffer(ByteBuffer byteBuffer) {
            byteBuffer.position(0);
            byteBuffer.limit(fileSize);
            this.availableBuffers.offerFirst(byteBuffer);
        }
    
        public ByteBuffer borrowBuffer() {
            ByteBuffer buffer = availableBuffers.pollFirst();
            if (availableBuffers.size() < poolSize * 0.4) {
                log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
            }
            return buffer;
        }

    }

    public class MappedFileQueue {
        // commitlog 文件的存储目录
        private final String storePath;
        // 单个 mappedFile 文件的大小
        private final int mappedFileSize;
        // mappedFile 文件的集合
        private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();
        // 一个服务类，专门用来创建 mappedFile 文件
        private final AllocateMappedFileService allocateMappedFileService;
        // 当前刷盘指针，表示该指针之前的数据全部被持久化到磁盘
        private long flushedWhere = 0;
        // 当前数据提交指针，内存中的 ByteBuffer 当前写指针， 该值大于等于 flushedWhere 指针
        private long committedWhere = 0;

        private volatile long storeTimestamp = 0;

        // 根据消息存储时间戳来查找 MappdFile。从 MappedFile 列表中第一个文件开始查找，找到第一个最后一次更新时间大于待查找时间戳的文件，
        // 其实也就是查找在 timestamp 时间戳之后进行过修改的文件，如果有多个则返回找到的第一个。如果不存在，则返回最后一个MappedFile 文件
        public MappedFile getMappedFileByTime(final long timestamp) {
            Object[] mfs = this.copyMappedFiles(0);
    
            if (null == mfs)
                return null;
    
            for (int i = 0; i < mfs.length; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                    return mappedFile;
                }
            }
    
            return (MappedFile) mfs[mfs.length - 1];
        }

        /**
         * 根据消息偏移量来查找 MappedFile
         * 
         * 根据 offset 查找 MappedFile 直接使用 offset/mappedFileSize 是否可行？答案是否定的，
         * 只要存在于存储目录下的文件，都需要对应创建内存映射文件，如果不定时将已消费的消息从存储文件中删除，会造成极大的内存压力与
         * 资源浪费，所有 RocketMQ 采取定时删除存储文件的策略，也就是说在存储文件中， 第一个文件不一定是 00000000000000000000 ，
         * 因为该文件在某一时刻会被删除，故根据 offset 定位 MappedFile 算法为：
         * 
         * ((offset / this.mappedFileSize) - (mappedFile.getFileFromOffset() / this.mappedFileSize))
         * 
         * 这里 mappedFile#getFileFromOffset 获取到的是这个 mappedFile 中第一个消息的偏移量
         */
        public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
            try {
                MappedFile mappedFile = this.getFirstMappedFile();
                if (mappedFile != null) {
                    // 计算出这个 offset 所在的 MappedFile 文件的索引
                    int index = (int) ((offset / this.mappedFileSize) - (mappedFile.getFileFromOffset() / this.mappedFileSize));
                    if (index < 0 || index >= this.mappedFiles.size()) {
                        LOG_ERROR.warn("Offset for {} not matched. Request offset: {}, index: {}, ");
                    }
    
                    try {
                        return this.mappedFiles.get(index);
                    } catch (Exception e) {
                        if (returnFirstOnNotFound) {
                            return mappedFile;
                        }
                        LOG_ERROR.warn("findMappedFileByOffset failure. ", e);
                    }
                }
            } catch (Exception e) {
                log.error("findMappedFileByOffset Exception", e);
            }
    
            return null;
        }

        // 获取存储文件的最小偏移量。mappedFile 的 fileFromOffset 属性是这个 mappedFile 中第一个消息的偏移量
        public long getMinOffset() {
            if (!this.mappedFiles.isEmpty()) {
                try {
                    return this.mappedFiles.get(0).getFileFromOffset();
                } catch (IndexOutOfBoundsException e) {
                    //continue;
                } catch (Exception e) {
                    log.error("getMinOffset has exception.", e);
                }
            }
            return -1;
        }

        // 获取存储文件的最大偏移量，返回最后一个 MappedFile 文件的 fileFromOffset 加上 MappedFile 文件当前的写指针
        public long getMaxOffset() {
            MappedFile mappedFile = getLastMappedFile();
            if (mappedFile != null) {
                return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
            }
            return 0;
        }

        // MappedFileQueue#getLastMappedFile
        public MappedFile getLastMappedFile(final long startOffset) {
            return getLastMappedFile(startOffset, true);
        }

        // MappedFileQueue#getLastMappedFile
        public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
            // 创建映射问价的起始偏移量
            long createOffset = -1;
            // 获取最后一个映射文件，如果为null或者写满则会执行创建逻辑
            MappedFile mappedFileLast = getLastMappedFile();

            // 1.mappedFileLast == null 则说明此时不存在任何 mappedFile 文件或者历史的 mappedFile 文件已经被清理了
            // 此时的 createOffset 按照如下的方式进行计算
            if (mappedFileLast == null) {
                // 计算将要创建的映射文件的起始偏移量
                // 如果 startOffset < mappedFileSize 则起始偏移量为0
                // 如果 startOffset >= mappedFileSize 则起始偏移量为是 mappedFileSize 的倍数
                createOffset = startOffset - (startOffset % this.mappedFileSize);
            }

            // 2.mappedFileLast != null && mappedFileLast.isFull() 说明最后一个 mappedFile 的空间已满，此时 createOffset
            // 应该按照下面的方式进行计算，createOffset 也就是新的 mappedFile 的偏移量以及文件名（这两个是相等的）
            if (mappedFileLast != null && mappedFileLast.isFull()) {
                // 创建的映射文件的偏移量等于最后一个映射文件的起始偏移量 + 映射文件的大小（commitlog文件大小）
                createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
            }

            if (createOffset != -1 && needCreate) {
                String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
                String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
                MappedFile mappedFile = null;

                if (this.allocateMappedFileService != null) {
                    // 预分配内存
                    // 基于 createOffset 构建两个连续的 AllocateRequest 并插入 AllocateMappedFileService 线程维护的 requestQueue
                    // 这两个 AllocateRequest 也就是创建两个 mappedFile 文件，一个文件的初始偏移量为 createOffset，另外一个为 createOffset + mappedFileSize
                    // AllocateMappedFileService 线程读取 requestQueue 中的 AllocateRequest 异步创建对应的 MappedFile。在创建过程中，
                    // 消息处理线程通过 CountDownLatch 同步等待 MappedFile 完成创建
                    mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath, nextNextFilePath, this.mappedFileSize);
                } else {
                    try {
                        // 直接 new 一个 MappedFile 对象，并且不使用 TransientStorePool
                        mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                    } catch (IOException e) {
                        log.error("create mappedFile exception", e);
                    }
                }

                if (mappedFile != null) {
                    if (this.mappedFiles.isEmpty()) {
                        mappedFile.setFirstCreateInQueue(true);
                    }
                    this.mappedFiles.add(mappedFile);
                }

                return mappedFile;
            }

            return mappedFileLast;
        }
    }

    public class AllocateMappedFileService extends ServiceThread {

        // AllocateMappedFileService#putRequestAndReturnMappedFile
        public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
            // 默认提交两个请求
            int canSubmitRequests = 2;
            if (this.messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                        && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) { 
                    canSubmitRequests = this.messageStore.getTransientStorePool().remainBufferNumbs()
                            - this.requestQueue.size();
                }
            }

            // 创建一个 AllocateRequest 请求，创建初始偏移量为 nextFilePath 的 mappedFile 
            AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
            // 判断 requestTable 中是否存在该路径的分配请求，如果存在则说明该请求已经在排队中
            boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;

            // 该路径没有在排队
            if (nextPutOK) {
                if (canSubmitRequests <= 0) {
                    log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, ");
                    this.requestTable.remove(nextFilePath);
                    return null;
                }
                // 将创建的 AllocateRequest 放入到 requestQueue 中
                boolean offerOK = this.requestQueue.offer(nextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
                canSubmitRequests--;
            }

            // 创建一个 AllocateRequest 请求，创建初始偏移量为 nextNextFilePath（等于 nextFilePath + mappedFileSize） 的 mappedFile 
            AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
            boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
            if (nextNextPutOK) {
                if (canSubmitRequests <= 0) {
                    log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, ";
                    this.requestTable.remove(nextNextFilePath);
                } else {
                    boolean offerOK = this.requestQueue.offer(nextNextReq);
                    if (!offerOK) {
                        log.warn("never expected here, add a request to preallocate queue failed");
                    }
                }
            }

            if (hasException) {
                log.warn(this.getServiceName() + " service has exception. so return null");
                return null;
            }

            // 获取创建第一个 mappedFile 的请求
            AllocateRequest result = this.requestTable.get(nextFilePath);
            try {
                if (result != null) {
                    // 阻塞等待创建第一个 mappedFile 完成
                    // AllocateMappedFileService 线程循环从 requestQueue 获取 AllocateRequest，AllocateRequest 实现了 Comparable 接口，
                    // 依据文件名从小到大排序。当需要创建 MappedFile 时，同时构建两个 AllocateRequest，消息处理线程通过下面的 CountDownLatch 将 
                    // AllocateMappedFileService 线程异步创建第一个 MappedFile 文件转化为同步操作，而第二个 MappedFile 文件的仍然创建交由 
                    // AllocateMappedFileService 线程异步创建。当消息处理线程需要再次创建 MappedFile 时，此时可以直接获取已创建的 MappedFile。
                    boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                    if (!waitOK) {
                        log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                        return null;
                    } else {
                        this.requestTable.remove(nextFilePath);
                        // 返回创建好的第一个 mappedFile 对象
                        return result.getMappedFile();
                    }
                } else {
                    log.error("find preallocate mmap failed, this never happen");
                }
            } catch (InterruptedException e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }

            return null;
        }

        // AllocateMappedFileService#run
        public void run() {
            log.info(this.getServiceName() + " service started");
            while (!this.isStopped() && this.mmapOperation()) {
            }
            log.info(this.getServiceName() + " service end");
        }

        // AllocateMappedFileService#mmapOperation
        private boolean mmapOperation() {
            boolean isSuccess = false;
            AllocateRequest req = null;
            try {
                req = this.requestQueue.take();
                AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
                if (null == expectedRequest) {
                    log.warn("this mmap request expired, maybe cause timeout");
                    return true;
                }
                if (expectedRequest != req) {
                    log.warn("never expected here,  maybe cause timeout");
                    return true;
                }

                if (req.getMappedFile() == null) {
                    long beginTime = System.currentTimeMillis();

                    // 在下面开始正式进行 mappedFile 的创建工作
                    // 如果 isTransientStorePoolEnable 为 true，MappedFile 会将 TransientStorePool 申请的堆外内存（Direct Byte Buffer）空间作为 
                    // writeBuffer，写入消息时先将消息写入 writeBuffer，然后将消息提交至 fileChannel 再 flush；否则，直接创建 MappedFile 内存映射
                    // 文件字节缓冲区 mappedByteBuffer，将消息写入 mappedByteBuffer 再 flush。完成消息写入后，更新 wrotePosition（此时还未 flush 至磁盘）
                    MappedFile mappedFile;
                    if (messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                        try {
                            mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
                            mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                        } catch (RuntimeException e) {
                            log.warn("Use default implementation.");
                            mappedFile = new MappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                        }
                    } else {
                        mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                    }

                    // 省略代码

                    // pre write mappedFile
                    // 对 MappedFile 进行预热
                    if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig().getMapedFileSizeCommitLog() &&
                        this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
                        mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
                            this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
                    }

                    req.setMappedFile(mappedFile);
                    this.hasException = false;
                    isSuccess = true;
                }
            } catch (InterruptedException e) {
                // ignore code
            } catch (IOException e) {
                // ignore code
            } finally {
                if (req != null && isSuccess)
                    req.getCountDownLatch().countDown();
            }
            return true;
        }

    }

    /**
     * When write a message to the commit log, returns results
     */
    public class AppendMessageResult {
        // Return code
        private AppendMessageStatus status;
        // Where to start writing
        private long wroteOffset;
        // Write Bytes
        private int wroteBytes;
        // Message ID
        private String msgId;
        // Message storage timestamp
        private long storeTimestamp;
        // Consume queue's offset(step by one)
        private long logicsOffset;
        private long pagecacheRT = 0;
        private int msgNum = 1;

        public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, String msgId, long storeTimestamp, 
                    long logicsOffset, long pagecacheRT) {
            this.status = status;
            this.wroteOffset = wroteOffset;
            this.wroteBytes = wroteBytes;
            this.msgId = msgId;
            this.storeTimestamp = storeTimestamp;
            this.logicsOffset = logicsOffset;
            this.pagecacheRT = pagecacheRT;
        }
    }

}