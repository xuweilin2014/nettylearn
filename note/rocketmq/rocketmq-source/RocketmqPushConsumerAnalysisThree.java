public class RocketmqPushConsumerAnalysisThree{

    /**
     * 定时消息是指消息发送到 Broker 后，并不立即被消费者消费而是要等到特定的时间后才能被消费， RocketMQ 并不支持任意的时间精度， 如果要支持任意时间精度的定时调度，不可避免地需要在 Broker 层做消息排序，
     * 再加上持久化方面的考量，将不可避免地带来具大的性能消耗，所以 RocketMQ 只支持特定级别的延迟消息。
     * 
     * 说到定时任务，上文提到的消息重试正是借助定时任务实现的，在将消息存入 commitlog 文件之前需要判断消息的重试次数 ，如果大于 0，则会将消息的主题设置 SCHEDULE_TOPIC_XXXX，
     * RocketMQ 定时消息 实现类为 org.apache.rocketmq.store.schedule.ScheduleMessageService。该类的实例在 DefaultMessageStore 中创建，通过在 DefaultMessageStore 中调用 load 方法
     * 加载并调用 start 方法进行启动。
     */

    public class ScheduleMessageService extends ConfigManager {
        // 定时消息统一主题
        public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
        // 第一次调度延迟的时间，默认为 1s
        private static final long FIRST_DELAY_TIME = 1000L;
        // 每一延时级别调度一次后延迟该时间间隔后再放入调度池
        private static final long DELAY_FOR_A_WHILE = 100L;
        // 发送异常后延迟该时间后再继续参与调度
        private static final long DELAY_FOR_A_PERIOD = 10000L;

        private final DefaultMessageStore defaultMessageStore;
        // 最大消息延迟级别
        private int maxDelayLevel;
        // 延迟级别，将 "1s 5s 10s 30s 1m 2m 3m 4m Sm 6m 7m 8m 9m 10m 20m 30m lh 2h" 字符串解析成 delayLevelTable，转换后的数据结构类似 {1: 1000 ,2 :5000 30000, ...}
        private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable = new ConcurrentHashMap<Integer, Long>(32);

        private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable = new ConcurrentHashMap<Integer, Long>(32);

        // 该方法主要完成延迟消息消费队列消息进度的加载与 delayLevelTable 数据的构造，延迟队列消息消费进度默认存储路径为 ${ROCKET HOME}/store/config/delayOffset.json
        public boolean load() {
            boolean result = super.load();
            result = result && this.parseDelayLevel();
            return result;
        }

        public void start() {
            // 根据延迟队列创建定时任务，遍历延迟级别，根据延迟级别 level 从 offsetTable 中获取到消费队列的消费进度，如果不存在，则使用 0，也就是说，每一个延迟级别对应一个消息消费队列，
            // 然后创建定时任务，每一个时任务第一次启动时默认延迟 ls 先执行一次定时任务，第二次调度开始才使用相应的延迟时间。延迟级别与消息消费队列的映射关系为：消息队列 ID = 延迟级别 - 1
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }
    
                if (timeDelay != null) {
                    this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
                }
            }
    
            this.timer.scheduleAtFixedRate(new TimerTask() {
    
                @Override
                public void run() {
                    try {
                        // 创建定时任务，每隔 10s 持久化一次延迟队列的消息消费进度（延迟消息调进度），持久化频率可以通过 flushDelayOffsetInterval 配置属性进行设置
                        ScheduleMessageService.this.persist();
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
        }

    }

    // ScheduleMessageService#start 方法启动后，会为每一个延迟级别创建一个调度任务，每一个延迟级别其实对应 SCHEDULE_TOPIC_XXXX 主题下的一个消息消费队列。
    // 定时调度任务的实现类为 DeliverDelayedMessageTimerTask，其核心实现为 executeOnTime
    class DeliverDelayedMessageTimerTask extends TimerTask {

        @Override
        public void run() {
            try {
                this.executeOnTimeup();
            } catch (Exception e) {
                // ignore code
            }
        }

        public void executeOnTimeup() {
            // 根据队列 ID 与延迟主题查找消息消费队列，如果未找到，说明目前并不存在该延时级别的消息，忽略本次任务，根据延时级别创建下一次调度任务即可
            ConsumeQueue cq = ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(SCHEDULE_TOPIC, delayLevel2QueueId(delayLevel));

            long failScheduleOffset = offset;

            if (cq != null) {
                // 根据 offset 从消息消费队列中获取当前队列中所有有效的消息。如果未找到，更新一下延迟队列定时拉取进度并创建定时任务待下一次继续尝试
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);

                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        // 遍历 ConsumeQueue，每一个标准 ConsumeQueue 条目为 20 个字节。解析出消息的物理偏移量、
                        // 消息长度、消息 tag hashcode，为从 commitlog 加载具体的消息做准备
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}", tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }

                            long now = System.currentTimeMillis();
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            long countdown = deliverTimestamp - now;

                            if (countdown <= 0) {
                                // 根据消息物理偏移量与消息大小从 commitlog 文件中查找消息。如果未找到消息，打印错误日志，根据延迟时间创建下一个定时器
                                MessageExt msgExt = ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);

                                if (msgExt != null) {
                                    try {
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                        // 将消息再次存入到 commitlog，并转发到主题对应的消息队列上，供消费者再次消费
                                        PutMessageResult putMessageResult = ScheduleMessageService.this.defaultMessageStore.putMessage(msgInner);
                                        
                                        if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            continue;
                                        } else {
                                            log.error("ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}", msgExt.getTopic(), msgExt.getMsgId());
                                            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset), DELAY_FOR_A_PERIOD);
                                            ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        // ignore code
                                    }
                                }
                            } else {
                                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset), countdown);
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for
                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(his.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {
                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {

                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset=" + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        // 根据消息重新构建新的消息对象，清除消息的延迟级别属性(delayLevel)、并恢复消息原先的消息主题与消息消费队列，消息的消费次数 reconsumeTimes 并不会丢失
        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }

    }

    /**
     * RocketMQ 支持局部消息顺序消费，可以确保同一个消息消费队列中的消息被顺序消费，如果需要做到全局顺序消费则可以将主题配置成一个队列。
     * 消息消费包含如下 4 个步骤：消息队列负载、消息拉取、消息消费、消息消费进度存储。
     */

    public abstract class RebalanceImpl{

        /**
         * RocketMQ 首先需要通过 RebalanceService 线程实现消息队列的负载， 集群模式下同一个消费组内的消费者共同承担其订阅主题下消息队列的消费，同一个消息消费队列在同一
         * 时刻只会被消费组内一个消费者消费，一个消费者同一时刻可以分配多个消费队列
         */
        private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet, final boolean isOrder) {

            // 省略代码

            List<PullRequest> pullRequestList = new ArrayList<PullRequest>();

            /**
             * 如果经过消息队列重新负载（分配）后，分配到新的消息队列时，首先需要尝试向 Broker 发起锁定该消息队列的请求，如果返回加锁成功则创建该消息队列的拉取任务，
             * 否则将跳过，等待其他消费者释放该消息队列的锁，然后在下一次队列重新负载时再尝试加锁。
             * 
             * 顺序消息消费与并发消息消费的第一个关键区别：顺序消息在创建消息队列拉取任务时需要在 Broker 服务器锁定该消息队列。
             */
            for (MessageQueue mq : mqSet) {
                if (!this.processQueueTable.containsKey(mq)) {
                    if (isOrder && !this.lock(mq)) {
                        log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                        continue;
                    }

                    this.removeDirtyOffset(mq);
                    ProcessQueue pq = new ProcessQueue();
                    long nextOffset = this.computePullFromWhere(mq);
                    if (nextOffset >= 0) {
                        ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                        if (pre != null) {
                            log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                        } else {
                            log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                            PullRequest pullRequest = new PullRequest();
                            pullRequest.setConsumerGroup(consumerGroup);
                            pullRequest.setNextOffset(nextOffset);
                            pullRequest.setMessageQueue(mq);
                            pullRequest.setProcessQueue(pq);
                            pullRequestList.add(pullRequest);
                            changed = true;
                        }
                    } else {
                        log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                    }
                }
            }

            this.dispatchPullRequest(pullRequestList);

            return changed;
        }

        // ConcurrentMap< MessageQueue, Process Queue> processQueueTable，将消息队列按照 Broker 组织成 Map<String /*brokerName */, Set<MessageQueue>>，
        // 方便下一步向 Broker 发送锁定消息队列的请求。
        private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
            HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();
            for (MessageQueue mq : this.processQueueTable.keySet()) {
                Set<MessageQueue> mqs = result.get(mq.getBrokerName());
                if (null == mqs) {
                    mqs = new HashSet<MessageQueue>();
                    result.put(mq.getBrokerName(), mqs);
                }
    
                mqs.add(mq);
            }
    
            return result;
        }

        public void lockAll() {
            HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();
    
            Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, Set<MessageQueue>> entry = it.next();
                final String brokerName = entry.getKey();
                final Set<MessageQueue> mqs = entry.getValue();
    
                if (mqs.isEmpty())
                    continue;
    
                FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
                if (findBrokerResult != null) {
                    LockBatchRequestBody requestBody = new LockBatchRequestBody();
                    requestBody.setConsumerGroup(this.consumerGroup);
                    requestBody.setClientId(this.mQClientFactory.getClientId());
                    requestBody.setMqSet(mqs);
                    try {
                        // 向 Broker ( Master 主节点) 发送锁定消息队列，该方法返回成功被当前消费者锁定的消息消费队列
                        Set<MessageQueue> lockOKMQSet = this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
    
                        // 将成功锁定的消息消费队列相对应的处理队列 ProcessQueue 设置为锁定状态，同时更新加锁时间
                        for (MessageQueue mq : lockOKMQSet) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                if (!processQueue.isLocked()) {
                                    log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                                }
    
                                processQueue.setLocked(true);
                                processQueue.setLastLockTimestamp(System.currentTimeMillis());
                            }
                        }

                        // 遍历当前处理队列中的消息消费队列，如果当前消费者不持有该消息队列的锁将处理队列锁状态设置为 false ，暂停该消息消费队列的消息拉取与消息消费
                        for (MessageQueue mq : mqs) {
                            if (!lockOKMQSet.contains(mq)) {
                                ProcessQueue processQueue = this.processQueueTable.get(mq);
                                if (processQueue != null) {
                                    processQueue.setLocked(false);
                                    log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("lockBatchMQ exception, " + mqs, e);
                    }
                }
            }
        }

        // 有两种情况下，要重新进行负载均衡（doRebalance）：
        // 1.消费者 Consumer 的上线和下线
        // 2.定时每隔 20s 来重新进行一次负载均衡
        public void doRebalance(final boolean isOrder) {
            // 获取到该 DefaultMQPushConsumerImpl 中所有的订阅信息
            Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
            if (subTable != null) {
                for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                    final String topic = entry.getKey();
                    try {
                        // 循环针对所有订阅的 topic，做 rebalance
                        this.rebalanceByTopic(topic, isOrder);
                    } catch (Throwable e) {
                        if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                            log.warn("rebalanceByTopic Exception", e);
                        }
                    }
                }
            }
            
            // 做完 rebalance 后，检查是否有的 queue 已经不归自己负责消费，是的话就释放缓存 message 的 queue
            this.truncateMessageQueueNotMyTopic();
        }

        private void rebalanceByTopic(final String topic, final boolean isOrder) {
            switch (messageModel) {
                case BROADCASTING: {
                    Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                    if (mqSet != null) {
                        boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                        if (changed) {
                            this.messageQueueChanged(topic, mqSet, mqSet);
                        }
                    } else {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                    break;
                }

                case CLUSTERING: {
                    // 从路由信息中获取 topic 对应所有的 Queue
                    Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                    // 从 Broker 获取所有同一个 Group 的所有 Consumer ID
                    List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);

                    if (null == mqSet) {
                        if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                            log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                        }
                    }
    
                    if (null == cidAll) {
                        log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                    }
    
                    if (mqSet != null && cidAll != null) {
                        List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                        mqAll.addAll(mqSet);
                        // 将 MQ 和 cid 都排好序
                        Collections.sort(mqAll);
                        Collections.sort(cidAll);
    
                        AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;
                        List<MessageQueue> allocateResult = null;
                        try {
                            // 按照初始化是指定的分配策略，获取分配的 MQ 列表
                            // 同一个 topic 的消息会分布于集群内的多个 broker 的不同 queue 上。同一个 group 下面会有多个 consumer，
                            // 分配策略 AllocateMessageQueueStrategy 的作用就是计算当前 consumer 应该消费哪几个 queue 的消息
                            allocateResult = strategy.allocate(this.consumerGroup, this.mQClientFactory.getClientId(), mqAll,cidAll);
                        } catch (Throwable e) {
                            log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(), e);
                            return;
                        }
    
                        Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                        if (allocateResult != null) {
                            allocateResultSet.addAll(allocateResult);
                        }
                        
                        // 更新 RebalanceImpl 中的 processQueue 用来缓存收到的消息，对于新加入的 Queue，提交一次 PullRequest
                        // 根据前面分配策略分配到 queue 之后，会查看是否是新增的 queue，如果是则提交一次 PullRequest 去 broker 拉取消息
                        // 不过对于新启动的 consumer 来说，所有的 queue 都是新添加的，所以所有 queue 都会触发 PullRequest
                        // 
                        // 1、为什么会有新的 queue，broker 和 consumer 的上下线，造成集群中每个 consumer 重新做一次负载均衡，这样就会有本来不属于这个 consumer 的 queue 被分到当前 consumer 来负责消费
                        // 2、对于 RebalanceImpl 来说，启动的时候会对每个 queue 发送一次 pullRequest，之后就由处理线程负责发起 pullRequest。所有要有个定时任务定期检查是否有 queue 是新进来的，第一次的 pullRequest 没做
                        boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                        if (changed) {
                            // 同步数据到 Broker，通过发送一次心跳实现
                            this.messageQueueChanged(topic, mqSet, allocateResultSet);
                        }
                    }
                    break;
                }
                default:
                    break;
            }
        }

        /**
         * 从以上的代码可以看出，rebalanceImpl 每次都会检查分配到的 queue 列表，如果发现有新的 queue 加入，就会给这个 queue 初始化一个缓存队列，然后新发起一个 PullRequest 给 PullMessageService 执行。
         * 由此可见，新增的 queue 只有第一次 Pull 请求时 RebalanceImpl 发起的，后续请求是在 broker 返回数据后，处理线程发起的。
         */
        private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet, final boolean isOrder) {
            boolean changed = false;

            Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, ProcessQueue> next = it.next();
                MessageQueue mq = next.getKey();
                ProcessQueue pq = next.getValue();

                if (mq.getTopic().equals(topic)) {
                    // 不再消费这个Queue的消息
                    if (!mqSet.contains(mq)) {
                        pq.setDropped(true);
                        if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                            it.remove();
                            changed = true;
                            log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                        }
                    // 超过max idle时间    
                    } else if (pq.isPullExpired()) {
                        // ignore code
                    }
                }
            }

            List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
            for (MessageQueue mq : mqSet) {
                // 如果是新加入的 Queue
                if (!this.processQueueTable.containsKey(mq)) {
                    if (isOrder && !this.lock(mq)) {
                        log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                        continue;
                    }

                    // 从 OffsetStore 中移除过时数据
                    this.removeDirtyOffset(mq);
                    ProcessQueue pq = new ProcessQueue();
                    // 获取起始消费的 Offset
                    long nextOffset = this.computePullFromWhere(mq);
                    if (nextOffset >= 0) {
                        // 为新的 MessageQueue 初始化一个 ProcessQueue，用来缓存收到的消息
                        ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                        if (pre != null) {
                            log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                        } else {
                            // 对于新加的 MessageQueue，初始化一个 PullRequest
                            log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                            PullRequest pullRequest = new PullRequest();
                            pullRequest.setConsumerGroup(consumerGroup);
                            pullRequest.setNextOffset(nextOffset);
                            pullRequest.setMessageQueue(mq);
                            pullRequest.setProcessQueue(pq);
                            pullRequestList.add(pullRequest);
                            changed = true;
                        }
                    } else {
                        log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                    }
                }
            }

            // 分发 Pull Request 到 PullMessageService 中的 pullRequestQueue 中
            this.dispatchPullRequest(pullRequestList);

            return changed;
        }

    }


    public class ConsumeMessageOrderlyService implements ConsumeMessageService {
        // 每次消费任务最大的持续时间，默认为 60s
        private final static long MAX_TIME_CONSUME_CONTINUOUSLY = Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));
        // 消息消费者实现类
        private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
        // 消息消费者
        private final DefaultMQPushConsumer defaultMQPushConsumer;
        // 顺序消息消费监听器
        private final MessageListenerOrderly messageListener;
        // 消息消费任务队列
        private final BlockingQueue<Runnable> consumeRequestQueue;
        // 消息消费线程池
        private final ThreadPoolExecutor consumeExecutor;
        // 消费者组名
        private final String consumerGroup;
        // 消息消费端消息、消费队列锁容器，内部持有 ConcurrentMap<MessageQueue, Object> mqLockTable = new ConcurrentHashMap<MessageQueue, Object>();
        private final MessageQueueLock messageQueueLock = new MessageQueueLock();
        // 调度任务线程池
        private final ScheduledExecutorService scheduledExecutorService;
        private volatile boolean stopped = false;

        public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl, MessageListenerOrderly messageListener) {

            this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
            this.messageListener = messageListener;
            this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
            this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();

            // 消息消费任务队列
            this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

            this.consumeExecutor = new ThreadPoolExecutor(this.defaultMQPushConsumer.getConsumeThreadMin(),
                    this.defaultMQPushConsumer.getConsumeThreadMax(), 1000 * 60, TimeUnit.MILLISECONDS,
                    this.consumeRequestQueue, new ThreadFactoryImpl("ConsumeMessageThread_"));

            this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
        }

        /**
         * 如果消费模式为集群模式，启动定时任务，默认每隔 20s 执行一次锁定分配给自己的消息消费队列。通过 -Drocketmq.client.rebalance.locklnterval = 20000 设置间隔，该值建议与一次消息负载频率设置相同。
         * 从上文可知，集群模式下顺序消息消费在创建拉取任务时并未将 ProcessQueue 的 locked 状态设置为 true，在未锁定消息队列之前无法执行消息拉取任务， ConsumeMessageOrderlyService 以每秒 20s 频率对
         * 分配给自己的消息队列进行自动锁操作，从而消费加锁成功的消息消费队列
         */
        public void start() {
            if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        ConsumeMessageOrderlyService.this.lockMQPeriodically();
                    }
                }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
            }
        }

        public synchronized void lockMQPeriodically() {
            if (!this.stopped) {
                this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
            }
        }

        public void submitConsumeRequest(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue, final boolean dispathToConsume) {
            // 构建消费任务，并且提交到消费线程池中
            if (dispathToConsume) {
                ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
                this.consumeExecutor.submit(consumeRequest);
            }

        }
    }


    class ConsumeRequest implements Runnable {
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.warn("run, the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                return;
            }

            // 根据消息队列获取一个对象 objLock，然后消息消费时先独占 objLock。顺序消息消费者的并发度为消息队列，也就是一个消息队列在同一时间
            // 只会被一个消费线程池中的线程消费
            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
            synchronized (objLock) {

                // 如果是广播模式的话，直接进入消费，无须锁定处理队列，因为相互直接无竞争; 
                // 如果是集群模式，进入消息消费逻辑的前提条件 proceessQueue 已被锁定并且锁未超时
                if (MessageModel.BROADCASTING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                    || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {

                    final long beginTime = System.currentTimeMillis();
                    for (boolean continueConsume = true; continueConsume; ) {
                        // ignore code

                        long interval = System.currentTimeMillis() - beginTime;
                        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                            break;
                        }

                        // 每次从处理队列中按顺序取出 consumeBatchSize 消息，如果未取到消息，也就是 msgs 为空，则设置 continueConsume 为 false ，本次消费任务结束。
                        // 顺序消息消费时，从 ProceessQueue 取出的消息，会临时存储在 ProceeQueue 的 consumingMsgOrderlyTreeMap 属性中
                        final int consumeBatchSize = ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
                        List<MessageExt> msgs = this.processQueue.takeMessags(consumeBatchSize);

                        if (!msgs.isEmpty()) {
                            final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);
                            ConsumeOrderlyStatus status = null;
                            ConsumeMessageContext consumeMessageContext = null;

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                // ignore code
                                // 执行消息消费钩子函数（消息消费之前 before 方法），通过 DefaultMQPushConsumerimpl#registerConsumeMessageHook
                                // 注册消息消费钩子函数并可以注册多个
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                            }

                            long beginTimestamp = System.currentTimeMillis();
                            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                            boolean hasException = false;
                            try {
                                // 申请消息消费锁，然后执行消息消费监听器，调用业务方具体消息监听器执行真正的消息消费处理逻辑，并通知 RocketMQ 消息消费结果
                                this.processQueue.getLockConsume().lock();
                                if (this.processQueue.isDropped()) {
                                    log.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                                        this.messageQueue);
                                    break;
                                }

                                status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                            } catch (Throwable e) {
                                hasException = true;
                            } finally {
                                this.processQueue.getLockConsume().unlock();
                            }

                            if (null == status || ConsumeOrderlyStatus.ROLLBACK == status
                                || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",ConsumeMessageOrderlyService.this.consumerGroup,msgs, messageQueue);
                            }

                            long consumeRT = System.currentTimeMillis() - beginTimestamp;
                            if (null == status) {
                                if (hasException) {
                                    returnType = ConsumeReturnType.EXCEPTION;
                                } else {
                                    returnType = ConsumeReturnType.RETURNNULL;
                                }
                            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                                returnType = ConsumeReturnType.TIME_OUT;
                            } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                returnType = ConsumeReturnType.FAILED;
                            } else if (ConsumeOrderlyStatus.SUCCESS == status) {
                                returnType = ConsumeReturnType.SUCCESS;
                            }

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                            }

                            if (null == status) {
                                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                            }

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.setStatus(status.toString());
                                consumeMessageContext.setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                            }

                            ConsumeMessageOrderlyService.this.getConsumerStatsManager().incConsumeRT(ConsumeMessageOrderlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);
                            // 如果消息消费结果为 ConsumeOrderlyStatus.SUCCESS，执行 ProceeQueue 的 commit 方法，并返回待更新的消息消费进度
                            continueConsume = ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status, context, this);
                        } else {
                            continueConsume = false;
                        }
                    }
                
                // 集群模式下，如果未锁定处理队列，则延迟该队列的消息消费    
                } else {
                    if (this.processQueue.isDropped()) {
                        log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                        return;
                    }

                    ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
                }
            }
        }

    }


    public class ProcessQueue{

        /**
         * 提交，就是将该批消息从 ProceeQueue 中移除，维护 msgCount (消息处理队列中消息条数) 并获取消息消费的偏移量 offset，然后将该批消息从 msgTreeMapTemp 中移除，
         * 并返回待保存的消息消费进度 (offset+ 1)，从中可以看出 offset 表示消息消费队列的逻辑偏移似于数组的下标，代表第 n 个 ConsumeQueue 条目
         */
        public long commit() {
            try {
                this.lockTreeMap.writeLock().lockInterruptibly();
                try {
                    Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
                    msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size());
                    for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                        msgSize.addAndGet(0 - msg.getBody().length);
                    }
                    this.consumingMsgOrderlyTreeMap.clear();
                    if (offset != null) {
                        return offset + 1;
                    }
                } finally {
                    this.lockTreeMap.writeLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("commit exception", e);
            }
    
            return -1;
        }

    }
    


}