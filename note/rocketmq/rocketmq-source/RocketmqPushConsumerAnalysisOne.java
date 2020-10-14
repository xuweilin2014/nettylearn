import jdk.javadoc.internal.doclets.formats.html.resources.standard;

public class RocketmqPushConsumerAnalysis{

    public class DefaultMQPushConsumerImpl implements MQConsumerInner {

        private static final long PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION = 3000;

        private final DefaultMQPushConsumer defaultMQPushConsumer;

        private final RPCHook rpcHook;

        private MessageListener messageListenerInner;

        private MQClientInstance mQClientFactory;

        private ConsumeMessageService consumeMessageService;

        public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
            this.defaultMQPushConsumer = defaultMQPushConsumer;
            this.rpcHook = rpcHook;
        }

        public void registerMessageListener(MessageListener messageListener) {
            this.messageListenerInner = messageListener;
        }

        public MessageListener getMessageListenerInner() {
            return messageListenerInner;
        }

        // 构建主题订阅信息 SubscriptionData 并加入到 Rebalancelmpl 的订阅消息中
        // 这里的订阅关系主要来自于 DefaultMQPushConsumer#subscribe(String topic, String subExpression) 方法
        public void subscribe(String topic, String subExpression) throws MQClientException {
            try {
                // 创建一个 SubscriptionData 类型的对象，保存了 topic、subString、tagsSet 属性，其中 tagsSet 保存了 subString 中的各个 tag
                SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), topic, subExpression);
                this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                if (this.mQClientFactory != null) {
                    this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                }
            } catch (Exception e) {
                throw new MQClientException("subscription exception", e);
            }
        }

        private void copySubscription() throws MQClientException {
            try {
                Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
                if (sub != null) {
                    for (final Map.Entry<String, String> entry : sub.entrySet()) {
                        final String topic = entry.getKey();
                        final String subString = entry.getValue();
                        SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), topic, subString);
                        this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                    }
                }
    
                if (null == this.messageListenerInner) {
                    this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
                }
    
                /**
                 * 订阅重试主题消息，从这里可以看出，RocketMQ 消息重试是以消费组为单位，而不是主题，消息重试主题名为 %RETRY%＋消费组名，
                 * 消费者在启动的时候会自动订阅该主题，参与该主题的消息队列负载
                 */
                switch (this.defaultMQPushConsumer.getMessageModel()) {
                    case BROADCASTING:
                        break;
                    case CLUSTERING:
                        // 重试主题为：%RETRY% + ConsumerGroup
                        final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                        SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), retryTopic, SubscriptionData.SUB_ALL);
                        this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                throw new MQClientException("subscription exception", e);
            }
        }

        public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName) throws Exception{
            try {
                String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName) : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
                this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg, this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());
            } catch (Exception e) {
                // ignore code

                this.mQClientFactory.getDefaultMQProducer().send(newMsg);
            }
        }

        public synchronized void start() throws MQClientException {
            switch (this.serviceState) {
                case CREATE_JUST:
                    log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}", this.defaultMQPushConsumer.getConsumerGroup(),
                        this.defaultMQPushConsumer.getMessageModel(), this.defaultMQPushConsumer.isUnitMode());
                    this.serviceState = ServiceState.START_FAILED;
                    // 基本的参数检查，group name 不能是 DEFAULT_CONSUMER
                    this.checkConfig();
                    // 将 DefaultMQPushConsumer 的订阅信息 copy 到 RebalanceService 中如果是 cluster 模式，如果订阅了 topic,则自动订阅 %RETRY%topic
                    this.copySubscription();
    
                    // 修改 InstanceName 参数值为 pid
                    // 当消息模式为 CLUSTERING 时，且此 Consumer 没有指定 InstanceName 时，就会修改 InstanceName 为 pid
                    if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                        this.defaultMQPushConsumer.changeInstanceNameToPID();
                    }
                    
                    /**
                     * 初始化 MQCientInstance Rebalancelmple （消息重新负载实现类）等
                     * 新建一个 MQClientInstance，客户端管理类，所有的i/o类操作由它管理，一个进程只有一个实例
                     */
                    this.mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);
    
                    this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                    this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
                    this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
                    this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);
                    // PullRequest 封装实现类，封装了和 broker 的通信接口
                    this.pullAPIWrapper = new PullAPIWrapper(mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
                    this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);
    
                    /* 
                     * 初始化消息进度，如果消息消费是集群模式（负载均衡），那么消息进度保存在 Broker 上; 如果是广播模式，那么消息消进度存储在消费端
                     */
                    if (this.defaultMQPushConsumer.getOffsetStore() != null) {
                        this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
                    } else {
                        switch (this.defaultMQPushConsumer.getMessageModel()) {
                            case BROADCASTING:
                                this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                                break;
                            case CLUSTERING:
                                this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                                break;
                            default:
                                break;
                        }
                        this.defaultMQPushConsumer.setOffsetStore(this.offsetStore);
                    }
                    // 如果是本地持久化会从文件中进行加载
                    this.offsetStore.load();
    
                    /**
                     * 根据是否是顺序消费，创建消费端消费线程服务。ConsumeMessageService 主要负责消息消费，内部维护一个线程池
                     */
                    if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                        this.consumeOrderly = true;
                        this.consumeMessageService = new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                    } else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                        this.consumeOrderly = false;
                        this.consumeMessageService = new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                    }
                    // 启动了清理等待处理消息服务
                    this.consumeMessageService.start();
    
                    /**
                     * 向 MQClientInstance 注册消费者，并启动 MQClientlnstance，在一个 JVM 中的所有消费者、生产者持有同一个 MQClientInstance, MQClientInstance 只会启动一次
                     */
                    boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                    if (!registerOK) {
                        this.serviceState = ServiceState.CREATE_JUST;
                        this.consumeMessageService.shutdown();
                        throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                    }
    
                    // 启动MQClientInstance，会启动PullMessageService和RebalanceService
                    mQClientFactory.start();
                    log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case RUNNING:
                case START_FAILED:
                case SHUTDOWN_ALREADY:
                    throw new MQClientException("The PushConsumer service state not OK, maybe started once, "
                        + this.serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
                default:
                    break;
            }
    
            this.updateTopicSubscribeInfoWhenSubscriptionChanged();
            this.mQClientFactory.checkClientInBroker();
            this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            this.mQClientFactory.rebalanceImmediately();
        }

        /**
         * 消息拉取分为 3 个部分：
         * 1.客户端封装消息拉取请求
         * 2.消息服务器查找并返回消息
         * 3.消息拉取客户端处理返回的消息
         * @param pullRequest
         */
        public void pullMessage(final PullRequest pullRequest) {
            // 从 pullRequest 中获取到 ProcessQueue
            final ProcessQueue processQueue = pullRequest.getProcessQueue();
            // 检查处理队列 ProcessQueue 是否被丢弃
            if (processQueue.isDropped()) {
                log.info("the pull request[{}] is dropped.", pullRequest.toString());
                return;
            }
            // 如果处理队列当前状态未被丢弃，则更新 ProcessQueue 的 lastPullTimestamp 为当前时间戳
            pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());
    
            try {
                // 检查当前 Consumer 的状态是否为 RUNNING，如果不是，则当前消费者被挂起，将拉取任务延迟 3000 ms 再次放入到 PullMessageService 的拉取任务队列中，进行拉取操作
                // 结束本次消息的拉取
                this.makeSureStateOK();
            } catch (MQClientException e) {
                log.warn("pullMessage exception, consumer state not ok", e);
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);
                return;
            }
    
            // 检查当前消费者是否暂停，否则，也将拉取任务延迟 1s 放入到 PullMessageService 的拉取任务队列中
            if (this.isPause()) {
                log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
                return;
            }

            /**
             * 接下来进行消息拉取的流控
             */
    
            // 消息的总数
            long cachedMessageCount = processQueue.getMsgCount().get();
            // ProcessQueue 中消息的大小，cachedMessageSizeInMiB 单位为 MB
            long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);
    
            // 如果 ProcessQueue 当前处理的消息条数超过了 pullThresholdForQueue = 1000 将触发流控，放弃本次拉取任务
            // 将拉取任务延迟 50 ms 之后再次加入到拉取任务队列中，进行拉取操作
            if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                if ((queueFlowControlTimes++ % 1000) == 0) {
                   // 打印警告日志
                }
                return;
            }
    
            // 如果 ProcessQueue 当前处理的消息总大小超过了 pullThresholdSizeForQueue = 1000 MB 将触发流控
            if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()) {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                if ((queueFlowControlTimes++ % 1000) == 0) {
                    // 打印警告日志
                }
                return;
            }
    
            if (!this.consumeOrderly) {
                // ProcessQueue 中队列最大偏移量与最小偏离量的间距，不能超 consumeConcurrentlyMaxSpan = 2000，否则触发流控，
                if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                    this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                    if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                        // 打印警告日志
                    }
                    return;
                }
            } else {
                if (processQueue.isLocked()) {
                    if (!pullRequest.isLockedFirst()) {
                        final long offset = this.rebalanceImpl.computePullFromWhere(pullRequest.getMessageQueue());
                        boolean brokerBusy = offset < pullRequest.getNextOffset();
                        log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
                            pullRequest, offset, brokerBusy);
                        if (brokerBusy) {
                            log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
                                pullRequest, offset);
                        }
    
                        pullRequest.setLockedFirst(true);
                        pullRequest.setNextOffset(offset);
                    }
                } else {
                    this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);
                    log.info("pull message later because not locked in broker, {}", pullRequest);
                    return;
                }
            }
    
            // 拉取该主题订阅信息，如果为空，结束本次消息拉取，关于该队列的下一次拉取任务延迟 3 s
            final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
            if (null == subscriptionData) {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);
                log.warn("find the consumer's subscription failed, {}", pullRequest);
                return;
            }
    
            final long beginTimestamp = System.currentTimeMillis();
    
            PullCallback pullCallback = new PullCallback() {
                @Override
                public void onSuccess(PullResult pullResult) {
                    if (pullResult != null) {
                        // 调用 pullAPIWrapper 中的 processPullResult 方法将消息字节数组解码成消息列表填充 msgFoundList，并且对消息进行消息过滤（TAG）模式
                        pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult, subscriptionData);
    
                        switch (pullResult.getPullStatus()) {
                            case FOUND:
                                // 更新 PullRequest 的下一次拉取偏移量
                                long prevRequestOffset = pullRequest.getNextOffset();
                                pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                                long pullRT = System.currentTimeMillis() - beginTimestamp;
                                DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(),
                                    pullRequest.getMessageQueue().getTopic(), pullRT);
    
                                long firstMsgOffset = Long.MAX_VALUE;
                                // 如果 msgFoundList 为空，则立即将 PullRequest 放入到 PullMessageService 的 pullRequestQueue，以便  PullMessageService 能够及时唤醒，
                                // 并进行消息拉取操作，为什么 PullStatus.FOUND, msgFoundList 会为空呢？因为 RocketMQ 根据 TAG 消息过滤，在服务端只是验证了 TAG hashcode ，在客户端再次
                                // 对消息进行过滤，故可能会出现 msgFoundList 为空的情况
                                if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
                                    DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                } else {
                                    firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();
    
                                    DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(),
                                        pullRequest.getMessageQueue().getTopic(), pullResult.getMsgFoundList().size());
    
                                    // 将拉取到的消息存入 ProcessQueue，然后将拉取到的消息提交给 ConsumeMessageService 中供消费者消费，该方法是一个异步方法
                                    // 也就是 PullCallBack 将消息提交给 ConsumeMessageService 中就会立即返回，至于这些消息如何消费， PullCallBack 不关注
                                    boolean dispathToConsume = processQueue.putMessage(pullResult.getMsgFoundList());
                                    DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(pullResult.getMsgFoundList(), processQueue, pullRequest.getMessageQueue(), dispathToConsume);
    
                                    // 将消息提交给消费者线程之后 PullCallBack 将立即返回，可以说本次消息拉取顺利完成，然后根据 PullInterval 参数，如果 pullInterval > 0 ，则等待 pullInterval 毫秒后将
                                    // PullRequest 对象放入到 PullMessageService 的 PullRequestQueue 中，该消息队列的下次拉取即将被激活，达到持续消息拉取，实现准实时拉取消息的效果
                                    if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                        DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                                    } else {
                                        DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                    }
                                }
    
                                if (pullResult.getNextBeginOffset() < prevRequestOffset || firstMsgOffset < prevRequestOffset) {
                                    // ignore code
                                }
    
                                break;
                            
                            /**
                             * 如果返回 NO_NEW_MSG （没有新消息） NO_MATCHED_MSG （没有匹配消息），直接使用服务器端校正的偏移量进行下一次消息的拉取。接下来看看服务端是如何校正 Offset
                             * NO_NEW_MSG ，对应 GetMessageResult.OFFSET_FOUND_ NULL GetMessageResult.OFFSET_OVERFLOW_ONE
                             * 
                             * OFFSET_OVERFLOW_ONE：待拉取 offset 等于消息队列最大的偏移量，如果有新的消息到达， 此时会创建一个新的 ConsumeQueue 文件，按照上一个 ConsumeQueue 的最大偏移量就是下一个文件的起始偏移，
                             * 所以如果按照该 offset 第二次拉取消息时能成功
                             * OFFSET_FOUND _NULL：是根据 ConsumeQueue 的偏移量没有找到内容，将偏移定位到下一个 ConsumeQueue ，其实就是 offset ＋（一个 ConsumeQueue 包含多少个条目 = MappedFileSize / 20） 
                             */
                            case NO_NEW_MSG:
                                pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                                DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                break;
                            case NO_MATCHED_MSG:
                                pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                                DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                break;

                            case OFFSET_ILLEGAL:
                                log.warn("the pull request offset illegal, {} {}",
                                    pullRequest.toString(), pullResult.toString());
                                pullRequest.setNextOffset(pullResult.getNextBeginOffset());
    
                                pullRequest.getProcessQueue().setDropped(true);
                                DefaultMQPushConsumerImpl.this.executeTaskLater(new Runnable() {
    
                                    @Override
                                    public void run() {
                                        try {
                                            DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(pullRequest.getMessageQueue(),
                                                pullRequest.getNextOffset(), false);
    
                                            DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest.getMessageQueue());
    
                                            DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());
    
                                            log.warn("fix the pull request offset, {}", pullRequest);
                                        } catch (Throwable e) {
                                            log.error("executeTaskLater Exception", e);
                                        }
                                    }
                                }, 10000);
                                break;
                            default:
                                break;
                        }
                    }
                }
    
                @Override
                public void onException(Throwable e) {
                    if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("execute the pull request exception", e);
                    }
    
                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);
                }
            };
    
            boolean commitOffsetEnable = false;
            long commitOffsetValue = 0L;
            if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
                commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
                if (commitOffsetValue > 0) {
                    commitOffsetEnable = true;
                }
            }
    
            String subExpression = null;
            boolean classFilter = false;
            SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
            if (sd != null) {
                if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                    subExpression = sd.getSubString();
                }
    
                classFilter = sd.isClassFilterMode();
            }
    
            // commitOffset:从内存中读取消息的进度
            // suspend:表示消息拉取时是否支持挂起
            // subscription:消息过滤机制为表达式，则设置该标记位
            // class filter:消息过滤机制为类过滤模式
            int sysFlag = PullSysFlag.buildSysFlag(commitOffsetEnable, true, subExpression != null, classFilter);
            try {
                this.pullAPIWrapper.pullKernelImpl(
                    pullRequest.getMessageQueue(), // 从哪个消息队列拉取消息
                    subExpression,      // 消息过滤表达式
                    subscriptionData.getExpressionType(), // 消息表达式类型，分为 TAG 和 SQL92
                    subscriptionData.getSubVersion(), 
                    pullRequest.getNextOffset(), // 消息拉取偏移量
                    this.defaultMQPushConsumer.getPullBatchSize(), // 本次拉取的最大消息条数，默认为 32 条
                    sysFlag,  // 拉取系统标记
                    commitOffsetValue, // 当前 MessageQueue 的消费进度（内存中）
                    BROKER_SUSPEND_MAX_TIME_MILLIS, 
                    CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND, // 消息拉取的超时时间
                    CommunicationMode.ASYNC,  // 消息拉取模式，默认为异步拉取
                    pullCallback // 从 Broker 拉取消息之后的回调方法
                );
            } catch (Exception e) {
                log.error("pullKernelImpl exception", e);
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);
            }
        }

        private void makeSureStateOK() throws MQClientException {
            if (this.serviceState != ServiceState.RUNNING) {
                throw new MQClientException("The consumer service state not OK, " + this.serviceState + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
            }
        }

    }

    public static class PullSysFlag {

        // commitOffset:从内存中读取消息的进度
        private final static int FLAG_COMMIT_OFFSET = 0x1 << 0;
        // suspend:表示消息拉取时是否支持挂起
        private final static int FLAG_SUSPEND = 0x1 << 1;
        // subscription:消息过滤机制为表达式，则设置该标记位
        private final static int FLAG_SUBSCRIPTION = 0x1 << 2;
        // FLAG_CLASS_FILTER:消息过滤机制为类过滤模式
        private final static int FLAG_CLASS_FILTER = 0x1 << 3;
    
        public static int buildSysFlag(final boolean commitOffset, final boolean suspend,
            final boolean subscription, final boolean classFilter) {
            int flag = 0;
    
            if (commitOffset) {
                flag |= FLAG_COMMIT_OFFSET;
            }
    
            if (suspend) {
                flag |= FLAG_SUSPEND;
            }
    
            if (subscription) {
                flag |= FLAG_SUBSCRIPTION;
            }
    
            if (classFilter) {
                flag |= FLAG_CLASS_FILTER;
            }
    
            return flag;
        }
    }

    public class PullAPIWrapper {

        public PullResult pullKernelImpl(final MessageQueue mq, final String subExpression, final String expressionType,
                final long subVersion, final long offset, final int maxNums, final int sysFlag, final long commitOffset,
                final long brokerSuspendMaxTimeMillis, final long timeoutMillis,
                final CommunicationMode communicationMode, final PullCallback pullCallback)
                throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), this.recalculatePullFromWhichNode(mq), false);
            if (null == findBrokerResult) {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
                findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), this.recalculatePullFromWhichNode(mq), false);
            }

            if (findBrokerResult != null) {
                // 省略代码

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
                // 如果消息过滤模式为类过滤，则需要根据主题名称、 broker 地址找到注册在 Broker 上的 FilterServer 地址，从 FilterServer 上拉取消息，否则从 Broker 上拉取消息
                if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                    brokerAddr = computPullFromWhichFilterServer(mq.getTopic(), brokerAddr);
                }

                // 使用 Netty 向 Broker 发送请求，从Broker 上拉取消息
                PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(brokerAddr, requestHeader, timeoutMillis, communicationMode, pullCallback);

                return pullResult;
            }

            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }

    }

    public class MQClientInstance {

        private final PullMessageService pullMessageService;

        // MQConsumerInner 有两个实现类：DefaultMQPushConsumerImpl 和 DefaultMQPullConsumerImpl
        private final ConcurrentMap<String, MQConsumerInner> consumerTable = new ConcurrentHashMap<String, MQConsumerInner>();

        public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
            this(clientConfig, instanceIndex, clientId, null);
        }
    
        public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
            // 省略代码.....
    
            if (this.clientConfig.getNamesrvAddr() != null) {
                this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
                log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
            }
    
            this.clientId = clientId;
            this.mQAdminImpl = new MQAdminImpl(this);
            this.pullMessageService = new PullMessageService(this);
            this.rebalanceService = new RebalanceService(this);
    
            // 省略代码
        }

        public MQConsumerInner selectConsumer(final String group) {
            return this.consumerTable.get(group);
        }

        public void start() throws MQClientException {

            synchronized (this) {
                switch (this.serviceState) {
                    case CREATE_JUST:
                        this.serviceState = ServiceState.START_FAILED;
                        // If not specified,looking address from name server
                        if (null == this.clientConfig.getNamesrvAddr()) {
                            this.mQClientAPIImpl.fetchNameServerAddr();
                        }
                        // Start request-response channel
                        this.mQClientAPIImpl.start();
                        // Start various schedule tasks
                        this.startScheduledTask();
                        // Start pull service
                        // 启动 PullMessageService
                        this.pullMessageService.start();
                        // Start rebalance service
                        this.rebalanceService.start();
                        // Start push service
                        this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                        log.info("the client factory [{}] start OK", this.clientId);
                        this.serviceState = ServiceState.RUNNING;
                        break;
                    case RUNNING:
                        break;
                    case SHUTDOWN_ALREADY:
                        break;
                    case START_FAILED:
                        throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                    default:
                        break;
                }
            }
        }




    }

    public static class MixAll {

        public static final String RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";

        public static String getRetryTopic(final String consumerGroup) {
            return RETRY_GROUP_TOPIC_PREFIX + consumerGroup;
        }

    }

    public class MQClientManager {

        private AtomicInteger factoryIndexGenerator = new AtomicInteger();

        private static MQClientManager instance = new MQClientManager();

        private ConcurrentMap<String, MQClientInstance> factoryTable = new ConcurrentHashMap<String, MQClientInstance>();

        // 获取或者创建一个 MQClientInstance 实例
        public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
            // 获取到 ClientId，由 消费者的 ip 地址 + @ + InstanceName 组成
            String clientId = clientConfig.buildMQClientId();
            MQClientInstance instance = this.factoryTable.get(clientId);
            // 如果没有对应的 MQClientInstance，就创建；有的话，就直接返回
            if (null == instance) {
                instance = new MQClientInstance(clientConfig.cloneClientConfig(), this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
                MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
                if (prev != null) {
                    instance = prev;
                    log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
                } else {
                    log.info("Created new MQClientInstance for clientId:[{}]", clientId);
                }
            }
    
            return instance;
        }

    }

    public class ClientConfig {
        // 消费者的 ip 地址
        private String clientIP = RemotingUtil.getLocalAddress();
        // 默认为 DEFAULT 字符串
        private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");

        // 生成消费者的 id
        public String buildMQClientId() {
            StringBuilder sb = new StringBuilder();
            sb.append(this.getClientIP());
    
            sb.append("@");
            sb.append(this.getInstanceName());
            if (!UtilAll.isBlank(this.unitName)) {
                sb.append("@");
                sb.append(this.unitName);
            }
    
            return sb.toString();
        }

        public void setNamesrvAddr(String namesrvAddr) {
            this.namesrvAddr = namesrvAddr;
        }

        public void setInstanceName(String instanceName) {
            this.instanceName = instanceName;
        }

        public void changeInstanceNameToPID() {
            if (this.instanceName.equals("DEFAULT")) {
                this.instanceName = String.valueOf(UtilAll.getPid());
            }
        }

    }

    public abstract class ServiceThread implements Runnable {

        private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    
        protected final Thread thread;

        protected volatile boolean stopped = false;
    
        public ServiceThread() {
            this.thread = new Thread(this, this.getServiceName());
        }
    
        public abstract String getServiceName();
    
        public void start() {
            this.thread.start();
        }

        public boolean isStopped() {
            return stopped;
        }
    }

    public class PullMessageService extends ServiceThread {

        private final Logger log = ClientLogger.getLog();

        private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<PullRequest>();

        private final MQClientInstance mQClientFactory;

        private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "PullMessageServiceScheduledThread");
                }
            });
    
        public PullMessageService(MQClientInstance mQClientFactory) {
            this.mQClientFactory = mQClientFactory;
        }
    
        // 延迟 timeDelay 将 pullRequest 放入到 pullRequestQueue 队列中
        public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    PullMessageService.this.executePullRequestImmediately(pullRequest);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        }
    
        public void executePullRequestImmediately(final PullRequest pullRequest) {
            try {
                this.pullRequestQueue.put(pullRequest);
            } catch (InterruptedException e) {
                log.error("executePullRequestImmediately pullRequestQueue.put", e);
            }
        }
    
        public void executeTaskLater(final Runnable r, final long timeDelay) {
            this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
        }
    
        public ScheduledExecutorService getScheduledExecutorService() {
            return scheduledExecutorService;
        }
    
        private void pullMessage(final PullRequest pullRequest) {
            // 从 MQClientInstance 中获取到和 ConsumerGroup 对应的 MQConsumerInner 对象，在 MQClientInstance 中，消费者组名和消费者一一对应，
            // 并且消费者组名 ConsumerGroupName 不能重复
            final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
            if (consumer != null) {
                // 将 consumer 强制转换为 DefaultMQPushConsumerImpl，这也就说明 PullServiceMessage，这个线程只为 PUSH 模式服务
                DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
                impl.pullMessage(pullRequest);
            } else {
                log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
            }
        }
    
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");
    
            // stopped 是父类 ServiceThread 中的 volatile 属性
            while (!this.isStopped()) {
                try {
                    // 从 pullRequestQueue 中获取 PullRequest 消息拉取任务，如果 pullRequestQueue 为空，则线程将阻塞，直到有拉取任务被放入
                    PullRequest pullRequest = this.pullRequestQueue.take();
                    if (pullRequest != null) {
                        // 调用 pullMessage 方法进行消息拉取
                        this.pullMessage(pullRequest);
                    }
                } catch (InterruptedException e) {
                } catch (Exception e) {
                    log.error("Pull Message Service Run Method exception", e);
                }
            }
    
            log.info(this.getServiceName() + " service end");
        }
    
        @Override
        public void shutdown(boolean interrupt) {
            super.shutdown(interrupt);
            ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
        }
    
        @Override
        public String getServiceName() {
            return PullMessageService.class.getSimpleName();
        }
    
    }

    public class PullRequest {
        // 消费者组
        private String consumerGroup;
        // 待拉取的消费队列
        private MessageQueue messageQueue;
        // 消息处理队列，从 Broker 拉取到的消息先存入 ProccessQueue 中， 然后再提交到消费者消费线程池消费
        private ProcessQueue processQueue;
        // 待拉取的 MessageQueue 偏移量
        private long nextOffset;
        // lockedFile 是否被锁定
        private boolean lockedFirst = false;
    }

    public class ProcessQueue {

        // 消息存储容器，键为消息在 ConsumeQueue 中的偏移量，而 MessageExt 为消息实体
        private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
        // 读写锁，控制多线程并发修改 msgTreeMap 和 msgTreeMapTemp
        private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();
        // 当前 ProcessQueue 中的消息总数
        private final AtomicLong msgCount = new AtomicLong();
        // 当前 ProcessQueue 中包含的最大队列偏移量
        private volatile long queueOffsetMax = 0L;
        // 当前 ProcessQueue 是否被丢弃
        private volatile boolean dropped = false;
        // 上一次开始消息拉取时间戳
        private volatile long lastPullTimestamp;
        // 上一次消息消费的时间戳
        private volatile long lastConsumeTimestamp = System.currentTimeMillis();

    }

    public class MQClientAPIImpl {

        public PullResult pullMessage(final String addr, final PullMessageRequestHeader requestHeader,
                final long timeoutMillis, final CommunicationMode communicationMode, final PullCallback pullCallback) throws Exception {

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);

            switch (communicationMode) {
            case ONEWAY:
                assert false;
                return null;
            case ASYNC:
                this.pullMessageAsync(addr, request, timeoutMillis, pullCallback);
                return null;
            case SYNC:
                return this.pullMessageSync(addr, request, timeoutMillis);
            default:
                assert false;
                break;
            }

            return null;
        }

        private void pullMessageAsync(final String addr, final RemotingCommand request, final long timeoutMillis, final PullCallback pullCallback) throws Exception {

            this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {
                    RemotingCommand response = responseFuture.getResponseCommand();
                    if (response != null) {
                        try {
                            // 根据响应结果 ResponseFuture 的状态，解码并生成 PullResultExt 对象
                            // ResponseCode 到 PullStatus 状态编码转换
                            // SUCCESS -> FOUND
                            // PULL_RETRY_IMMEDIATELY -> NO_MATCHED_MSG
                            // PULL_OFFSET_MOVED -> OFFSET_ILLEGAL
                            // PULL_NOT_FOUND -> NO_NEW_MSG
                            PullResult pullResult = MQClientAPIImpl.this.processPullResponse(response);
                            assert pullResult != null;
                            pullCallback.onSuccess(pullResult);
                        } catch (Exception e) {
                            pullCallback.onException(e);
                        }
                    } else {
                        if (!responseFuture.isSendRequestOK()) {
                            pullCallback.onException(new MQClientException());
                        } else if (responseFuture.isTimeout()) {
                            pullCallback.onException(new MQClientException());
                        } else {
                            pullCallback.onException();
                        }
                    }
                }
            });
        }

        private PullResult processPullResponse(final RemotingCommand response)throws MQBrokerException, RemotingCommandException {

            PullStatus pullStatus = PullStatus.NO_NEW_MSG;
            switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                pullStatus = PullStatus.FOUND;
                break;
            case ResponseCode.PULL_NOT_FOUND:
                pullStatus = PullStatus.NO_NEW_MSG;
                break;
            case ResponseCode.PULL_RETRY_IMMEDIATELY:
                pullStatus = PullStatus.NO_MATCHED_MSG;
                break;
            case ResponseCode.PULL_OFFSET_MOVED:
                pullStatus = PullStatus.OFFSET_ILLEGAL;
                break;

            default:
                throw new MQBrokerException(response.getCode(), response.getRemark());
            }

            PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.decodeCommandCustomHeader(PullMessageResponseHeader.class);

            return new PullResultExt(pullStatus, responseHeader.getNextBeginOffset(), responseHeader.getMinOffset(),
                    responseHeader.getMaxOffset(), null, responseHeader.getSuggestWhichBrokerId(), response.getBody());
        }

    }

    public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {

        @Override
        public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws Exception {
            final Channel channel = this.getAndCreateChannel(addr);
            if (channel != null && channel.isActive()) {
                try {
                    if (this.rpcHook != null) {
                        this.rpcHook.doBeforeRequest(addr, request);
                    }
                    this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
                } catch (RemotingSendRequestException e) {
                    // ignore code
                }
            } else {
                // ignore code
            }
        }

        public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback) throws Exception {
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
                        }
                    });
                } catch (Exception e) {
                    // ignore code
                }
            } else {
                // ignore code
            }
        }

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
                    // ignore code
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

    public class ResponseFuture {

        private final InvokeCallback invokeCallback;

        public ResponseFuture(int opaque, long timeoutMillis, InvokeCallback invokeCallback, SemaphoreReleaseOnlyOnce once) {
            this.opaque = opaque;
            this.timeoutMillis = timeoutMillis;
            this.invokeCallback = invokeCallback;
            this.once = once;
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