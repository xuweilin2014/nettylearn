public class RocketmqMessageStoreAnalysis{

    /**
     * Rocketmq 文件存储目录存为 ${ROCKETMQ_HOME}/store/commitlog 目录，每一个文件默认大小为 1 G，一个文件写满之后再创建另外一个，
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
    public class DefaultMessageStore implements MessageStore {
        // 消息存储配置属性
        private final MessageStoreConfig messageStoreConfig;
        // CommitLog 文件的存储实现类
        private final CommitLog commitLog;
        // 消息队列缓存，按照消息主题进行分组
        private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;
        // 消息队列文件 ConsumeQueue 刷盘服务，或者叫线程
        private final FlushConsumeQueueService flushConsumeQueueService;
        // 清除 CommitLog 文件服务
        private final CleanCommitLogService cleanCommitLogService;
        // 清除 ConsumeQueue 文件服务
        private final CleanConsumeQueueService cleanConsumeQueueService;
        // 索引文件实现类
        private final IndexService indexService;
        
        public PutMessageResult putMessage(MessageExtBrokerInner msg) {
            // 判断当前 Broker 是否能够进行消息写入
            // 1.如果当前 Broker 停止工作，不支持写入
            // 2.如果当前 Broker 是 SLAVE 的话，那么也不支持写入
            // 3.如果当前 Broker 不支持消息写入，则拒绝消息写入
            // 4.如果消息主题的长度大于 256 个字符，则拒绝消息写入
            // 5.如果消息属性的长度超过 65536 个字符，则拒绝消息写入
    
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

    }

    public static class CommitLog {
        
        // topic-queueId -> offset
        private HashMap<String, Long> topicQueueTable = new HashMap<String, Long>(1024);

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

                // 省略代码...
                
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

                // 如果 mappedFile 为空，表明 ${ROCKET_HOME}/store/commitlog 目录下不存在任何文件
                // 也就是说明本次消息是第一次消息发送，用偏移量 0 创建第一个 commitlog 文件，文件名为 0000 0000 0000 0000 0000，
                // 如果创建文件失败，抛出 CREATE_MAPEDFILE_FAILED 异常，可能是权限不够或者磁盘空间不足
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
                    // 上一个 commitlog 已经满了，所以会去创建一个新的 commitlog
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

    }

    public class MappedFile extends ReferenceResource{

        public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
            return appendMessagesInner(msg, cb);
        }

        // 将消息追加到 MappedFile 中
        public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {  
            // 先获取 MappedFile 当前的写指针  
            int currentPos = this.wrotePosition.get();
    
            // 如果 currentPos 大于或者等于文件大小，则表明文件已经写满，会抛出 AppendMessageStatus.UNKNOWN_ERROR 异常
            if (currentPos < this.fileSize) {
                // 如果 currentPos 小于文件大小，通过 slice() 方法创建一个与 MappedFile 的共享共存区，并设置 position 为当前指针
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