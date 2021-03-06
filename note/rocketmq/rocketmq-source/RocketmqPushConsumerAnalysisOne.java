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

        @Override
        public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
            Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
            if (subTable != null) {
                if (subTable.containsKey(topic)) {
                    // 更新 rebalanceImpl 中的路由信息
                    // 每个 DefaultMQPushConsumerImpl 中都持有一个 RebalanceImpl 对象
                    this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
                }
            }
        }

        public void registerMessageListener(MessageListener messageListener) {
            this.messageListenerInner = messageListener;
        }

        public MessageListener getMessageListenerInner() {
            return messageListenerInner;
        }

        // 首先判断消费端有没有显式设置最大重试次数 MaxReconsumeTimes， 如果没有，则设置默认重试次数为 16，否则以设置的最大重试次数为准
        private int getMaxReconsumeTimes() {
            // default reconsume times: 16
            if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
                return 16;
            } else {
                return this.defaultMQPushConsumer.getMaxReconsumeTimes();
            }
        }

        // DefaultMQPushConsumerImpl#subscribe
        // 构建主题订阅信息 SubscriptionData 并加入到 Rebalancelmpl 的订阅消息中
        // 这里的订阅关系主要来自于 DefaultMQPushConsumer#subscribe(String topic, String subExpression) 方法
        public void subscribe(String topic, String subExpression) throws MQClientException {
            try {
                // 创建一个 SubscriptionData 类型的对象，保存了 topic、subString、tagsSet 属性，其中 tagsSet 保存了 subString 中的各个 tag
                // 也就是例子中的 TagA、TagB、TagC，而 codeSet 中保存了对应 Tag 的 hashcode 值用于消息过滤
                SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), topic, subExpression);
                // rebalanceImpl 中的 subscriptionInner 类型是 ConcurrentMap<String/* topic */, SubscriptionData>
                // 也就是一个 topic 对应一个 subscriptionData
                this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                if (this.mQClientFactory != null) {
                    // 向所有的 Broker 发送当前进程中的 Consumer 和 Producer 信息
                    this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                }
            } catch (Exception e) {
                throw new MQClientException("subscription exception", e);
            }
        }

        private void copySubscription() throws MQClientException {
            try {
                // 查找消费者的订阅主题信息，将其中的信息转换成 subscribtionData，然后保存到 rebalanceImpl 中的 subscriptionInner 属性中
                // 消费者在订阅主题 topic 的时候实际上是通过调用 defaultMQPushConsumerImpl 中的 subscribe 方法，将自己要订阅的 topic 以及 tag 等
                // 信息存储在 RebalanceImpl 中的 subscriptionInner 属性中，一个 topic 对应一个 subscribtionData
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
                 * 下面会订阅重试主题消息（如果是集群模式的话），从这里可以看出，RocketMQ 消息重试是以消费组为单位，而不是主题。消息重试主题名为 %RETRY%＋消费组名，
                 * 消费者在启动的时候会自动订阅该主题，参与该主题的消息队列负载
                 */
                switch (this.defaultMQPushConsumer.getMessageModel()) {
                    case BROADCASTING:
                        break;
                    case CLUSTERING:
                        // 重试主题为：%RETRY% + ConsumerGroup
                        final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                        // 创建一个 subscribtionData，表明订阅消息的主题为：%RETRY% + ConsumerGroup，订阅的 Tag 为：*，也就是订阅重试主题下所有的 Tag，
                        // consumerGroup 和当前创建的 consumerGroup 相同。
                        SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), retryTopic, SubscriptionData.SUB_ALL);
                        // 将上面创建好的重试订阅信息 subscribtionData 保存到 rebalanceImpl 中的 subscriptionInner 属性中。而 consumer 用户自己订阅的信息
                        // 也会保存到 subscriptionInner 属性中
                        this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                throw new MQClientException("subscription exception", e);
            }
        }

        /**
         * 一共有两种方式让broker重发，先尝试给 broker 发送 send_msg_back 的命令，如果失败了，则通过 consumer 预留的 producer 给 %RETRY%topic 发送消息，
         * 前面 consumer 启动的时候已经讲过，所有 consumer 都订阅 %RETRY%topic，所以等于是自己给自己发一条消息。
         * 
         * consumer 发送消费失败的消息和普通的 producer 发送消息的调用路径前面不太一样，使用下面的方法将消息发送回 Broker
         */
        // DefaultMQPushConsumerImpl#sendMessageBack
        public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName) throws Exception{
            try {
                String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName) : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
                // 首先尝试直接发送 CONSUMER_SEND_MSG_BACK 命令给 broker
                this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg, this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());
            } catch (Exception e) {
                log.error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e);
                // 如果消费失败的消息发送回 broker 失败了，会再重试一次，和 try 里面的方法不一样的地方是这里直接修改 topic 为 %RETRY%topic + consumerGroup
                // 然后和 producer 发送消息的方法一样发送到 broker。这个 producer 是在 consumer 启动的时候预留的
                Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
                String originMsgId = MessageAccessor.getOriginMessageId(msg);
                MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

                newMsg.setFlag(msg.getFlag());
                MessageAccessor.setProperties(newMsg, msg.getProperties());
                MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
                // 将消息的重试次数加一
                MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
                MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
                // 消息的延迟级别 delayLevel = 重试次数 + 3
                newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

                this.mQClientFactory.getDefaultMQProducer().send(newMsg);
            }
        }

        // DefaultMQPushConsumerImpl#start
        public synchronized void start() throws MQClientException {
            switch (this.serviceState) {
                case CREATE_JUST:
                    log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}");
                    this.serviceState = ServiceState.START_FAILED;
                    // 基本的参数检查，group name 不能是 DEFAULT_CONSUMER
                    this.checkConfig();

                    // copySubscription 执行以下两步操作：
                    // 1.将 DefaultMQPushConsumer 的订阅信息 copy 到 rebalanceImpl 中
                    // 2.如果是 CLUSTERING 模式，则自动订阅 %RETRY%topic，可以进行消息重试；如果是 BROADCASTING 模式，则不会进行消息重试
                    //
                    // 那这个 %RETRY% 开头的 topic 是做什么的呢？我们知道 consumer 消费消息失败的话（其实也就是我们业务代码消费消息失败），
                    // broker 会延时一定的时间重新推送消息给 consumer，重新推送不是跟其它新消息一起过来，而是通过单独的 %RETRY% 的 topic 过来
                    this.copySubscription();

                    // 修改 InstanceName 参数值为 pid
                    // 当消息模式为 CLUSTERING 时，且此 Consumer 没有指定 InstanceName 时，就会修改 InstanceName 为 pid
                    if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                        this.defaultMQPushConsumer.changeInstanceNameToPID();
                    }
                    
                    /**
                     * 新建一个 MQClientInstance，客户端管理类，所有 Consumer 和 Producer 的网络通讯操作由它管理，这个是和 Producer 共用一个实现
                     * 这个 MQClientInstance 对象默认是一个进程中只有一个，因为 MQClientInstance 的 key 是 ip + @ + instanceName，而 instanceName
                     * 如果用户不自己指定的话，就会转变为当前进程的 pid
                     */
                    this.mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);

                    this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                    this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
                    // 对于同一个 group 内的 consumer，RebalanceImpl 负责分配具体每个 consumer 应该消费哪些 queue 上的消息,以达到负载均衡的目的。
                    // Rebalance 支持多种分配策略，比如平均分配、一致性 Hash 等(具体参考 AllocateMessageQueueStrategy 实现类)。默认采用平均分配策略(AVG)
                    this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
                    this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);
                    // PullRequest 封装实现类，封装了和 broker 的通信接口
                    this.pullAPIWrapper = new PullAPIWrapper(mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
                    this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                    // 初始化消息进度，如果消息消费是集群模式（负载均衡），那么消息进度保存在 Broker 上; 如果是广播模式，那么消息消进度存储在消费端
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

                    // 加载消息消费的进度，从磁盘或者是 Broker 中加载消息的消费进度
                    // 如果是 LocalFileOffsetStore 的话，由于消息消费进度是存储在 Consumer 端，因此从本地磁盘中读取消费进度
                    // 如果是 RemoteBrokerOffsetStore 的话，load 方法是一个空方法，不做任何处理
                    this.offsetStore.load();

                    // 根据是否是顺序消费，创建消费端消费线程服务。ConsumeMessageService 主要负责消息消费，内部维护一个线程池
                    // 消息到达 Consumer 后会缓存到队列中，ConsumeMessageService 另起线程回调 Listener 消费。同时对于在缓存队列中等待的消息，
                    // 会定时检查是否已超时，通知 Broker 重发
                    if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                        this.consumeOrderly = true;
                        this.consumeMessageService = new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                    } else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                        this.consumeOrderly = false;
                        this.consumeMessageService = new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                    }

                    // 启动了消息消费服务，这里其实只是开启检查 ProcessQueue 中缓存的消息是否过期的功能
                    this.consumeMessageService.start();

                    // 向 MQClientInstance 注册消费者，并启动 MQClientlnstance，在一个进程中，只有一个 MQClientInstance, MQClientInstance 只会启动一次
                    boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                    if (!registerOK) {
                        this.serviceState = ServiceState.CREATE_JUST;
                        this.consumeMessageService.shutdown();
                        throw new MQClientException();
                    }

                    // 启动 MQClientInstance，会启动 PullMessageService 和 RebalanceService，并且会启动客户端，也就是 NettyRemotingClient
                    mQClientFactory.start();
                    log.info("the consumer [{}] start OK.");
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case RUNNING:
                case START_FAILED:
                case SHUTDOWN_ALREADY:
                    throw new MQClientException();
                default:
                    break;
            }
            
            // 从 NameServer 更新 topic 路由和订阅信息
            this.updateTopicSubscribeInfoWhenSubscriptionChanged();
            this.mQClientFactory.checkClientInBroker();
            // 发送心跳，同步 consumer 配置到 broker，同步 FilterClass 到 FilterServer(PushConsumer)
            this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            // 进行一次 Rebalance，启动 RebalanceImpl，这里才真正开始的 Pull 消息的操作
            this.mQClientFactory.rebalanceImmediately();
        }

        // DefaultMQPushConsumerImpl#rebalanceImmediately
        public void rebalanceImmediately() {
            this.rebalanceService.wakeup();
        }

        /**
         * 消息拉取分为 3 个部分：
         * 1.客户端封装消息拉取请求
         * 2.消息服务器查找并返回消息
         * 3.消息拉取客户端处理返回的消息
         */
        // DefaultMQPushConsumerImpl#pullMessage
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
                // 检查当前 Consumer 的状态是否为 RUNNING，如果不是，则当前消费者被挂起，将拉取任务延迟 3000 ms 再次放入到 PullMessageService 的拉取任务队列中，
                // 进行拉取操作，然后结束本次消息的拉取
                this.makeSureStateOK();
            } catch (MQClientException e) {
                log.warn("pullMessage exception, consumer state not ok", e);
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);
                return;
            }

            // ignore code

            /**
             * 接下来进行消息拉取的流控
             * 
             * rocketmq 拉取消息其实是一个循环的过程，这里就来到了一个问题，如果消息队列消费的速度跟不上消息发送的速度，那么就会出现消息堆积，
             * 如果不进行流控的话，会有很多的 message 存在于我们的内存中，会导致我们的 JVM 出现 OOM 也就是内存溢出。
             */
    
            // 消息的总数
            long cachedMessageCount = processQueue.getMsgCount().get();
            // ProcessQueue 中消息的大小，cachedMessageSizeInMiB 单位为 MB
            long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);
    
            // 如果 ProcessQueue 当前处理的消息条数超过了 pullThresholdForQueue = 1000 ，也就是堆积未处理的消息过多，将触发流控，放弃本次拉取任务
            // 将拉取任务延迟 50 ms 之后再次加入到拉取任务队列中，进行拉取操作
            if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                if ((queueFlowControlTimes++ % 1000) == 0) {
                   // 打印警告日志
                }
                return;
            }
    
            // 如果 ProcessQueue 当前未处理的消息总大小超过了 pullThresholdSizeForQueue = 1000 MB，也就是堆积的消息过大，也将触发流控，逻辑同上
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
                // 如果 processQueue 被锁定的话
                // processQueue 被锁定（设置其 locked）属性是在 RebalanceImpl#lock/lockAll 方法中，在 consumer 向 Broker 发送锁定消息队列请求之后，
                // Broker 会返回已经锁定好的消息队列集合，接着就会依次遍历这些消息队列 mq，并且从缓存 processQueueTable 中获取到和 mq 对应的 ProcessQueue，
                // 并且将这些 ProcessQueue 的 locked 属性设置为 true
                // RebalanceImpl#lock 是对新分配给 consumer 的 mq 进行加锁，而 RebalanceImpl#lockAll 则是周期性的进行加锁
                if (processQueue.isLocked()) {
                    // 该处理队列是第一次拉取任务，则首先计算拉取偏移量，然后向消息服务端拉取消息
                    // pullRequest 第一次被处理的时候，lockedFirst 属性为 false，之后都为 true
                    if (!pullRequest.isLockedFirst()) {
                        final long offset = this.rebalanceImpl.computePullFromWhere(pullRequest.getMessageQueue());
                        // 如果 Broker 的 consume offset 小于 pullRequest 的 offset，则说明 Broker 可能比较繁忙，来不及更新消息队列的 offset
                        boolean brokerBusy = offset < pullRequest.getNextOffset();
                        log.info("the first time to pull message, so fix offset from broker");
                        if (brokerBusy) {
                            log.info("the first time to pull message, but pull request offset larger than broker consume offset");
                        }
    
                        pullRequest.setLockedFirst(true);
                        // 拉取的偏移量以 Broker 为准
                        pullRequest.setNextOffset(offset);
                    }
                
                // 如果消息处理队列未被锁定，则延迟 3s 后再将 PullRequest 对象放入到拉取任务中
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
            // Pull Command 发送之后，返回的结果处理
            PullCallback pullCallback = new PullCallback() {

                /**                 
                 * 在消息返回后，会将消息放入ProcessQueue，然后通知 ConsumeMessageService 来异步处理消息，然后再次提交 Pull 请求。这样对于用户端来说，
                 * 只有 ConsumeMessageService 回调 listener 这一步是可见的，其它都是透明的。
                 * @param pullResult
                 */
                @Override
                public void onSuccess(PullResult pullResult) {
                    if (pullResult != null) {
                        // 调用 pullAPIWrapper#processPullResult 方法将消息字节数组解码成消息列表填充 msgFoundList，并且对消息进行消息过滤（TAG）模式
                        pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult, subscriptionData);
    
                        switch (pullResult.getPullStatus()) {
                            case FOUND:
                                long prevRequestOffset = pullRequest.getNextOffset();
                                // 更新 PullRequest 的下一次拉取偏移量
                                pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                                long pullRT = System.currentTimeMillis() - beginTimestamp;
                                DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(), pullRequest.getMessageQueue().getTopic(), pullRT);
    
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
    
                                    // 将拉取到的消息存入 ProcessQueue
                                    boolean dispathToConsume = processQueue.putMessage(pullResult.getMsgFoundList());
                                    // 将拉取到的消息提交给 ConsumeMessageService 中供消费者消费，该方法是一个异步方法
                                    // 也就是 PullCallBack 将消息提交给 ConsumeMessageService 中就会立即返回，至于这些消息如何消费， PullCallBack 不关注
                                    // 在 ConsumeMessageService 中进行消息的消费时，会调用 MessageListener 对消息进行实际的处理，
                                    // 处理完成会通知 ProcessQueue，从 ProcessQueue 中移除掉这些处理完的 msg
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
                             * 如果返回 NO_NEW_MSG （没有新消息） NO_MATCHED_MSG （没有匹配消息），直接使用服务器端校正的偏移量进行下一次消息的拉取。
                             * 接下来看看服务端是如何校正 Offset。NO_NEW_MSG ，对应 GetMessageResult.OFFSET_FOUND_ NULL GetMessageResult.OFFSET_OVERFLOW_ONE
                             * 
                             * OFFSET_OVERFLOW_ONE：待拉取 offset 等于消息队列最大的偏移量，如果有新的消息到达， 此时会创建一个新的 ConsumeQueue 文件，
                             * 按照上一个 ConsumeQueue 的最大偏移量就是下一个文件的起始偏移，所以如果按照该 offset 第二次拉取消息时能成功
                             * OFFSET_FOUND _NULL：是根据 ConsumeQueue 的偏移量没有找到内容，将偏移定位到下一个 ConsumeQueue ，其实就是 
                             * offset ＋（一个 ConsumeQueue 包含多少个条目 = MappedFileSize / 20） 
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
                                log.warn("the pull request offset illegal, {} {}", pullRequest.toString(), pullResult.toString());
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
                    BROKER_SUSPEND_MAX_TIME_MILLIS, // Broker 长轮询时间，也就是消息拉取过程中允许 Broker 挂起的时间，默认为 15s
                    CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND, // 消息拉取的超时时间，也就是客户端阻塞等待的时间
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

    public static class FilterAPI {

        /**
         * 以 consumer.subscribe("test-topic", "TagA || TagB || TagC"); 方法调用为例解释下面的参数：
         * @param consumerGroup： 上面 consumer 方法所属的 consumerGroup
         * @param topic: test-topic
         * @param subString: TagA || TagB || TagC
         * @return
         * @throws Exception
         */
        public static SubscriptionData buildSubscriptionData(final String consumerGroup, String topic, String subString) throws Exception {

            SubscriptionData subscriptionData = new SubscriptionData();
            subscriptionData.setTopic(topic);
            subscriptionData.setSubString(subString);

            // 如果说 subString 为 null 或者为空字符串，或者为 *，则表示订阅 topic 下所有的 Tag
            if (null == subString || subString.equals(SubscriptionData.SUB_ALL) || subString.length() == 0) {
                subscriptionData.setSubString(SubscriptionData.SUB_ALL);
            } else {
                // 将 subString 中的各个 tag （使用 || 进行分割）保存到 subscriptionData 的 tagSet 中
                // 并且将 tag 字符串的 hashcode 值保存到 codeSet 中，用于以后的消息过滤
                String[] tags = subString.split("\\|\\|");
                if (tags.length > 0) {
                    for (String tag : tags) {
                        if (tag.length() > 0) {
                            String trimString = tag.trim();
                            if (trimString.length() > 0) {
                                subscriptionData.getTagsSet().add(trimString);
                                subscriptionData.getCodeSet().add(trimString.hashCode());
                            }
                        }
                    }
                } else {
                    throw new Exception("subString split error");
                }
            }

            return subscriptionData;
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
            // 根据 BrokerName、BrokerId 从 MQClientInstance 中获取到 Broker 的地址。相同名称的 Broker 形成主从结构，其 BrokerId 会不一样
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), this.recalculatePullFromWhichNode(mq), false);
            // 如果查找不到 Broker 就更新一下路由信息，然后再次获取
            if (null == findBrokerResult) {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
                findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), this.recalculatePullFromWhichNode(mq), false);
            }

            if (findBrokerResult != null) {
                // 省略代码

                // 与长轮询有关的参数有三个：
                // 第一个是 sysFlag 中的第 2 个 bit 位，sysFlag 的构造为：{commitOffset：是否确认消息，suspend：是否长轮询，subscription：是否过滤消息，classFilter：是否类过滤}
                // 第二个是 brokerSuspendMaxTimeMillis，Broker 端长轮询不可能无限期等待下去，因此需要传递这个长轮询时间给到 broker，如果超过这个时间还没有消息到达，那么直接返回空的Response
                // 第三个是 timeoutMillis，broker在长轮询的时候，客户端也需要阻塞等待结果，但也不能无限制等待下去，如果超过 timeoutMillis 还没收到返回，那么我本地也需要做对应处理
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
                // 如果消息过滤模式为类过滤，则需要根据主题名称、 broker 地址找到注册在 Broker 上的 FilterServer 地址，从 FilterServer 上拉取消息，
                // 否则从 Broker 上拉取消息
                if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                    brokerAddr = computPullFromWhichFilterServer(mq.getTopic(), brokerAddr);
                }

                // 使用 MQClientAPIImpl 向 Broker 发送请求，从 Broker 上拉取消息
                // 这里根据 communicationMode 不同使用不同的拉取模式，如果是异步 pullResult 直接返回 null
                // 从 Broker 端拉取到消息之后，会回调 pullBack 中的 onSuccess/onException 方法对消息进行处理
                PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(brokerAddr, requestHeader, 
                                                                                timeoutMillis, communicationMode, pullCallback);

                return pullResult;
            }

            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }

        // 将消息字节数组解码成消息列表填充 msgFoundList，并且对消息进行消息过滤（TAG）模式
        public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult, final SubscriptionData subscriptionData) {

            PullResultExt pullResultExt = (PullResultExt) pullResult;
            // 设置下次从该 mq 拉取消息时，应该从哪个 brokerId 上拉取消息
            this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());
            if (PullStatus.FOUND == pullResult.getPullStatus()) {
                // 将消息字节数组解码成消息列表填充
                ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
                List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

                // 对解码得到的消息列表进行 Tag 过滤操作，也就是判断 msg 的 Tag 是否在 subscriptionData 的 tagSet 集合中
                List<MessageExt> msgListFilterAgain = msgList;
                if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                    msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                    for (MessageExt msg : msgList) {
                        if (msg.getTags() != null) {
                            if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                                msgListFilterAgain.add(msg);
                            }
                        }
                    }
                }

                if (this.hasHook()) {
                    FilterMessageContext filterMessageContext = new FilterMessageContext();
                    filterMessageContext.setUnitMode(unitMode);
                    filterMessageContext.setMsgList(msgListFilterAgain);
                    this.executeHook(filterMessageContext);
                }

                for (MessageExt msg : msgListFilterAgain) {
                    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET, Long.toString(pullResult.getMinOffset()));
                    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET, Long.toString(pullResult.getMaxOffset()));
                }

                pullResultExt.setMsgFoundList(msgListFilterAgain);
            }

            pullResultExt.setMessageBinary(null);

            return pullResult;
        }

    }

    public class MQClientInstance {

        private final PullMessageService pullMessageService;

        // MQConsumerInner 有两个实现类：DefaultMQPushConsumerImpl 和 DefaultMQPullConsumerImpl
        private final ConcurrentMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<String, MQConsumerInner>();

        private final ConcurrentMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<String, MQProducerInner>();

        private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();

        private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable = new ConcurrentHashMap<String, HashMap<Long, String>>();

        private ServiceState serviceState = ServiceState.CREATE_JUST;

        public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
            this(clientConfig, instanceIndex, clientId, null);
        }
    
        public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
            this.clientConfig = clientConfig;
            this.instanceIndex = instanceIndex;
            this.nettyClientConfig = new NettyClientConfig();
            this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
            this.nettyClientConfig.setUseTLS(clientConfig.isUseTLS());
            this.clientRemotingProcessor = new ClientRemotingProcessor(this);
            this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook, clientConfig);

            if (this.clientConfig.getNamesrvAddr() != null) {
                this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
                log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
            }

            this.clientId = clientId;
            this.mQAdminImpl = new MQAdminImpl(this);

            // Pull 请求服务，异步发送请求到 broker 并负责将返回结果放到缓存队列
            this.pullMessageService = new PullMessageService(this);
            // 定时或者被触发做 subscribe queue 的 re-balance
            this.rebalanceService = new RebalanceService(this);
            // 初始化一个自用的 producer，名称为 CLIENT_INNER_PRODUCER，主要用于在消费失败或者超时的时候，发送重试的消息给 Broker
            this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
            this.defaultMQProducer.resetClientConfig(clientConfig);
            this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);
        }

        public MQConsumerInner selectConsumer(final String group) {
            return this.consumerTable.get(group);
        }

        // MQClientInstance#start
        public void start() throws MQClientException {

            synchronized (this) {
                switch (this.serviceState) {
                    case CREATE_JUST:
                        this.serviceState = ServiceState.START_FAILED;
                        // If not specified,looking address from name server
                        // 如果 NameServer 为空，尝试从 http server 中获取 namesrv 地址，这里看出适合于有统一配置中心的系统
                        if (null == this.clientConfig.getNamesrvAddr()) {
                            this.mQClientAPIImpl.fetchNameServerAddr();
                        }
                        // Start request-response channel
                        // 启动 MQClientAPIImpl，其实也就是启动 remotingClient，
                        // 在 Producer/Consumer/Broker 中，remotingClient 真正用来对方进行通信
                        this.mQClientAPIImpl.start();
                        // Start various schedule tasks
                        // 开启定时任务：
                        // 1.定时获取 NameServer 的地址
                        // 2.定时从 NameServer 上获取路由信息
                        // 3.定时清除已经下线的 Broker，并且向 Broker 发送心跳
                        // 4.定时保存消费进度
                        // 5.根据负载调整本地处理消息的线程池 corePool 大小
                        this.startScheduledTask();
                        // 启动 PullMessageService，开启拉消息服务，使用一个单独的线程来进行消息的拉取操作
                        this.pullMessageService.start();
                        // 启动 rebalance service，最终会调用 RebalanceImpl 类对象，来给 Consumer 重新调整和分配 queue，触发 rebalance 的情况如下：
                        // 
                        // 1.定时触发(20sec)做 rebalance
                        // 2.当 consumer list 发生变化后需要重新做负载均衡，比如同一个 group 中新加入了 consumer 或者有 consumer 下线; 
                        // 3.当 consumer 启动的时候，也会进行负载均衡
                        this.rebalanceService.start();
                        // Start push service
                        // 启动自用的 producer，用来在 Consumer 消费消息失败的时候，重新发送消息给 Broker
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

        // 遍历已经注册的消费者，对消费者执行 doRebalance 操作
        // MQClientInstance#doRebalance
        public void doRebalance() {
            for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    try {
                        // 最终会调用 RebalanceImpl#doRebalance 方法来进行消息队列负载和重新分布
                        impl.doRebalance();
                    } catch (Throwable e) {
                        log.error("doRebalance exception", e);
                    }
                }
            }
        }

        private void startScheduledTask() {
            // 获取 NameServer 的地址
            if (null == this.clientConfig.getNamesrvAddr()) {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
    
                    @Override
                    public void run() {
                        try {
                            MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                        } catch (Exception e) {
                            log.error("ScheduledTask fetchNameServerAddr exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
            }

            // 从 NameServer 获取数据更新 topicRouteInfo
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        MQClientInstance.this.updateTopicRouteInfoFromNameServer();
                    } catch (Exception e) {
                        log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                    }
                }
            }, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);
    
            // 清除已经下线的broker，并发送心跳
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        MQClientInstance.this.cleanOfflineBroker();
                        MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
                    } catch (Exception e) {
                        log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                    }
                }
            }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);
    
            // 保存消费进度
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        // 遍历 consumerTable 中的所有 MQConsumerInner，将其中的 offsetStore 进行持久化
                        // 对于广播模式来说，offsetStore 是 LocalFileOffsetStore，会将其持久化到本地
                        // 对于集群模式来说，offsetStore 是 RemoteBrokerOffsetStore，会将其同步到 Broker
                        MQClientInstance.this.persistAllConsumerOffset();
                    } catch (Exception e) {
                        log.error("ScheduledTask persistAllConsumerOffset exception", e);
                    }
                }
            }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);
    
            // 根据负载调整本地处理消息的线程池 corePool 大小
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        MQClientInstance.this.adjustThreadPool();
                    } catch (Exception e) {
                        log.error("ScheduledTask adjustThreadPool exception", e);
                    }
                }
            }, 1, 1, TimeUnit.MINUTES);
        }

        // Producer 在启动的时候，会将自己填加到 producerTable 中
        public boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
            if (null == group || null == producer) {
                return false;
            }
    
            MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
            if (prev != null) {
                log.warn("the producer group[{}] exist already.", group);
                return false;
            }
    
            return true;
        }

        // Consumer 在启动的时候，同样也会将自己添加到 consumerTable 中
        public boolean registerConsumer(final String group, final MQConsumerInner consumer) {
            if (null == group || null == consumer) {
                return false;
            }
    
            MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
            if (prev != null) {
                log.warn("the consumer group[" + group + "] exist already.");
                return false;
            }
    
            return true;
        }

        // 在 MQClientInstance#startScheduledTask 方法中，会开启一个定时任务，定期的从 NameServer 上获取各个主题 topic 的路由信息
        // 也就是每隔一段时间调用下面的这个方法
        public void updateTopicRouteInfoFromNameServer() {
            Set<String> topicList = new HashSet<String>();
    
            // Consumer
            {
                Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
                // 遍历每一个消费者，将每一个消费者所订阅的各个主题 topic 加入到 topicList 中
                while (it.hasNext()) {
                    Entry<String, MQConsumerInner> entry = it.next();
                    MQConsumerInner impl = entry.getValue();
                    if (impl != null) {
                        Set<SubscriptionData> subList = impl.subscriptions();
                        if (subList != null) {
                            for (SubscriptionData subData : subList) {
                                topicList.add(subData.getTopic());
                            }
                        }
                    }
                }
            }
    
            // Producer
            {
                Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
                // 遍历每一个生产者 producer，将每一个 producer 所发布的各个主题 topic 加入到 topicList 中
                while (it.hasNext()) {
                    Entry<String, MQProducerInner> entry = it.next();
                    MQProducerInner impl = entry.getValue();
                    if (impl != null) {
                        Set<String> lst = impl.getPublishTopicList();
                        topicList.addAll(lst);
                    }
                }
            }
    
            // 依次遍历每一个主题 topic，然后向 NameServer 获取每一个 topic 的路由信息，并且在获取到最新的路由信息之后，
            // 更新所有的 consumer 和 producer 中保存的路由信息
            for (String topic : topicList) {
                this.updateTopicRouteInfoFromNameServer(topic);
            }
        }

        public boolean updateTopicRouteInfoFromNameServer(final String topic) {
            return updateTopicRouteInfoFromNameServer(topic, false, null);
        }

        public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault, DefaultMQProducer defaultMQProducer) {
            try {
                if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    try {
                        TopicRouteData topicRouteData;
                        // 如果 isDefault 的值为 true，则使用默认主题 "TBW102" 去查询
                        if (isDefault && defaultMQProducer != null) {
                            topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(), 1000 * 3);
                            if (topicRouteData != null) {
                                // 如果查询到路由信息，则替换路由信息中读写队列个数为消息生产者默认的队列个数
                                for (QueueData data : topicRouteData.getQueueDatas()) {
                                    int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                                    data.setReadQueueNums(queueNums);
                                    data.setWriteQueueNums(queueNums);
                                }
                            }
                        
                        // 如果 isDefault 为 false，则使用参数 topic 去查询
                        } else {
                            // 向 NameServer 发送 RequestCode 为 GET_ROUTEINTO_BY_TOPIC 的请求，获取要查询主题的路由信息
                            topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
                        }
                        
                        if (topicRouteData != null) {
                            TopicRouteData old = this.topicRouteTable.get(topic);
                            // 如果路由信息找到，与本地缓存中的路由信息进行对比，判断路由信息是否发生了改变，如果未发生变化，则直接返回 false
                            boolean changed = topicRouteDataIsChange(old, topicRouteData);
                            if (!changed) {
                                changed = this.isNeedUpdateTopicRouteInfo(topic);
                            } else {
                                log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                            }

                            // 如果路由信息发生了变化
                            if (changed) {
                                TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();

                                // topicRouteData 中的 List<BrokerData> 属性表明这个 topic 保存在哪些 Broker 上
                                // 所以将这些 BrokerData 保存到 MQClientInstance 的 brokerAddrTable 中，即
                                // Map<String /* broker name */, Map<Long /* broker id */, String /* broker address */>>
                                for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                    this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                                }

                                // Update Pub info
                                {
                                    TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                    publishInfo.setHaveTopicRouterInfo(true);
                                    Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
                                    // 根据最新的 topic 路由信息更新各个 DefaultMQProducerImpl 中的路由信息 topicPublishInfoTable
                                    while (it.hasNext()) {
                                        Entry<String, MQProducerInner> entry = it.next();
                                        MQProducerInner impl = entry.getValue();
                                        if (impl != null) {
                                            impl.updateTopicPublishInfo(topic, publishInfo);
                                        }
                                    }
                                }

                                // Update sub info
                                {
                                    Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                    Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
                                    // 根据最新得 topic 路由信息更新各个 RebalanceImpl （也就是消息消费者的）中的 topicSubscribeInfoTable
                                    while (it.hasNext()) {
                                        Entry<String, MQConsumerInner> entry = it.next();
                                        MQConsumerInner impl = entry.getValue();
                                        if (impl != null) {
                                            impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                        }
                                    }
                                }
                                log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
                                this.topicRouteTable.put(topic, cloneTopicRouteData);
                                return true;
                            }
                        } else {
                            log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
                        }
                    } catch (Exception e) {
                        if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.equals(MixAll.DEFAULT_TOPIC)) {
                            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                        }
                    } finally {
                        this.lockNamesrv.unlock();
                    }
                } else {
                    log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LOCK_TIMEOUT_MILLIS);
                }
            } catch (InterruptedException e) {
                log.warn("updateTopicRouteInfoFromNameServer Exception", e);
            }

            return false;
        }

        // Producer 在发送消息时，调用此方法获取到 Broker 的具体 ip 地址，注意这里是获取 Master Broker 的地址，不是 Slave 的地址
        public String findBrokerAddressInPublish(final String brokerName) {
            HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
            if (map != null && !map.isEmpty()) {
                return map.get(MixAll.MASTER_ID);
            }
            return null;
        }

        // Consumer 在发送消息时，调用此方法获取到 Broker 的具体 ip 地址
        public FindBrokerResult findBrokerAddressInSubscribe(final String brokerName, final long brokerId, final boolean onlyThisBroker) {
            String brokerAddr = null;
            boolean slave = false;
            boolean found = false;

            HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
            if (map != null && !map.isEmpty()) {
                brokerAddr = map.get(brokerId);
                slave = brokerId != MixAll.MASTER_ID;
                found = brokerAddr != null;

                // 如果没有找到对应 BrokerId 的 Broker 地址，并且并不局限于这个 Broker，那么就可以继续循环直到找到一个 Broker 地址为止
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

        // MQClientInstance#sendHeartbeatToAllBrokerWithLock
        public void sendHeartbeatToAllBrokerWithLock() {
            if (this.lockHeartbeat.tryLock()) {
                try {
                    // 将此 Consumer 和 Producer 中的信息发送到所有的 Broker
                    this.sendHeartbeatToAllBroker();
                    this.uploadFilterClassSource();
                } catch (final Exception e) {
                    log.error("sendHeartbeatToAllBroker exception", e);
                } finally {
                    this.lockHeartbeat.unlock();
                }
            } else {
                log.warn("lock heartBeat, but failed.");
            }
        }

        // MQClientInstance#sendHeartbeatToAllBroker
        private void sendHeartbeatToAllBroker() {
            // 在发送心跳包到所有的 Broker 之前（包括 Master 和 Slave），先收集好 Producer 和 Consumer 的必要的信息。
            final HeartbeatData heartbeatData = this.prepareHeartbeatData();
            final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
            final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
            // 如果 producer 和 consumer 的数据都为空，那么则直接返回
            if (producerEmpty && consumerEmpty) {
                log.warn("sending heartbeat, but no consumer and no producer");
                return;
            }

            if (!this.brokerAddrTable.isEmpty()) {
                long times = this.sendHeartbeatTimesTotal.getAndIncrement();
                // 获取到所有 Broker 组的地址，一个 Broker 组 = 一个 Master Broker 和多个 Slave Broker，一个 Broker 组的名称都是一样的
                Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<String, HashMap<Long, String>> entry = it.next();
                    String brokerName = entry.getKey();
                    HashMap<Long, String> oneTable = entry.getValue();
                    if (oneTable != null) {
                        // 一个 entry1 表示一个 Broker 组中一个 Broker 的 id 和地址
                        for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                            Long id = entry1.getKey();
                            String addr = entry1.getValue();
                            if (addr != null) {
                                // 如果此 MQClientInstance 中没有 Consumer，并且 Broker 的 id 不是 MASTER_ID 的话，就直接忽略掉这个 Broker
                                // 这是因为 Consumer 会与 Master Broker 以及 Slave Broker 同时建立连接，而 Producer 只会与 Master 建立连接
                                if (consumerEmpty) {
                                    if (id != MixAll.MASTER_ID)
                                        continue;
                                }

                                try {
                                    int version = this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);
                                    if (!this.brokerVersionTable.containsKey(brokerName)) {
                                        this.brokerVersionTable.put(brokerName, new HashMap<String, Integer>(4));
                                    }
                                    this.brokerVersionTable.get(brokerName).put(addr, version);
                                    if (times % 20 == 0) {
                                        log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                                        log.info(heartbeatData.toString());
                                    }
                                } catch (Exception e) {
                                    // 省略代码
                                }
                            }
                        }
                    }
                }
            }
        }

        // 在发送心跳包到所有的 Broker 之前（包括 Master 和 Slave），先收集好 Producer 和 Consumer 的必要的信息。
        private HeartbeatData prepareHeartbeatData() {
            HeartbeatData heartbeatData = new HeartbeatData();
    
            // clientID = ip + @ + instanceName，instanceName 如果用户没有设置的话，就会被 rocketmq 自动改为 pid
            // clientId 表示是由哪个 MQClientInstance 发送的心跳请求
            heartbeatData.setClientID(this.clientId);
    
            // Consumer
            // 遍历 MQClientInstance 中 consumerTable 中的 consumer 信息，将其发送给 Broker
            for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    ConsumerData consumerData = new ConsumerData();
                    consumerData.setGroupName(impl.groupName());
                    consumerData.setConsumeType(impl.consumeType());
                    consumerData.setMessageModel(impl.messageModel());
                    consumerData.setConsumeFromWhere(impl.consumeFromWhere());
                    consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                    consumerData.setUnitMode(impl.isUnitMode());
                    // add consumerData to heartbeatData
                    heartbeatData.getConsumerDataSet().add(consumerData);
                }
            }
    
            // Producer
            // 遍历 MQClientInstance 中的 producerTable 中的 producer 信息，将其发送给 Broker
            for (Map.Entry<String/* group */, MQProducerInner> entry : this.producerTable.entrySet()) {
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    ProducerData producerData = new ProducerData();
                    producerData.setGroupName(entry.getKey());
                    // add producerData to heartbeatData
                    heartbeatData.getProducerDataSet().add(producerData);
                }
            }
    
            return heartbeatData;
        }


    }

    public class RebalanceService extends ServiceThread {
        
        private static long waitInterval = Long.parseLong(System.getProperty("rocketmq.client.rebalance.waitInterval", "20000"));
        private final Logger log = ClientLogger.getLog();
        private final MQClientInstance mqClientFactory;
    
        public RebalanceService(MQClientInstance mqClientFactory) {
            this.mqClientFactory = mqClientFactory;
        }
    
        // RebalanceService 每隔 20s 执行一次 mqClientFactory.doRebalance 方法
        // RebalanceService#run
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                // 默认阻塞等待 20s
                this.waitForRunning(waitInterval);
                // 调用 RebalanceImpl 的 doRebalance 操作
                this.mqClientFactory.doRebalance();
            }

            log.info(this.getServiceName() + " service end");
        }
    
        @Override
        public String getServiceName() {
            return RebalanceService.class.getSimpleName();
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
            // 获取到 ClientId，由 消费者的 ip 地址 + @ + InstanceName 组成，其中 InstanceName 如果用户不进行设置的话，就会自动设置为进程号 pid
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
            // 如果用户没有设置 instanceName 的话，就将其设置为进程号 pid
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
    
        // PullMessageService#executePullRequestImmediately 主要有两个地方会调用这个方法：
        // 1.在 DefaultMQPushConsumerImpl#pullMessage 方法中，一次消息拉取任务执行完成之后，又将 PullRequest 对象放入到 pullRequestQueue 中，
        // 这样 Consumer 就可以进行连续不断地拉取操作。
        // 2.在 RebalanceImpl 中创建，如果有新分配给消费者 Consumer 的 MessageQueue 的话，就创建一个新的 PullRequest，执行对应的 mq 的消息拉取工作。
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
    
        // PullMessageService#pullMessage
        private void pullMessage(final PullRequest pullRequest) {
            // 从 MQClientInstance 中获取到和 ConsumerGroup 对应的 MQConsumerInner 对象（其实就是 DefaultMQPushConsumerImpl 对象，
            // DefaultMQPushConsumerImpl 实现了 MQConsumerInner 接口），在 MQClientInstance 中，消费者组名和消费者一一对应，并且消费者组名
            // ConsumerGroupName 不能重复
            final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
            if (consumer != null) {
                // 将 consumer 强制转换为 DefaultMQPushConsumerImpl，这也就说明 PullServiceMessage，这个线程只为 PUSH 模式服务
                DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
                impl.pullMessage(pullRequest);
            } else {
                log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
            }
        }
    
        // PullMessageService#run
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

    /*
     * ProcessQueue 是 MessageQueue 在消费端的重现以及快照，PullMessageService 每次从 Broker 端默认拉取 32 条消息，按照消息的队列偏移量顺序
     * 放在 ProcessQueue 中，然后交给消费线程池处理，消息成功消费后从 ProcessQueue 中移除掉
     */
    public class ProcessQueue {

        // 消息存储容器，键为消息在 ConsumeQueue 中的偏移量，而 MessageExt 为消息实体
        private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
        // 读写锁，控制多线程并发修改 msgTreeMap 和 msgTreeMapTemp
        private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();

        /**
         * A subset of msgTreeMap, will only be used when orderly consume
         */
        private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();

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

        // 判断锁是否过期，默认为 30s
        public boolean isLockExpired() {
            return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
        }

        public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
            // 移除消费超时的消息，默认超过 15 分钟未消费的消息将延迟 3 个级别再消费
        }

        public boolean putMessage(final List<MessageExt> msgs) {
            // 添加消息，PullMessageService 拉取到消息后，先调用这个方法将消息添加到 processQueue 中
        }

        // ConsumeMessageOrderlyService#ConsumeRequest#run 方法进行消息的消费时，从 ProcessQueue 的 msgTreeMap 中
        // 获取消息，同时将获取到的消息保存到 consumingMagOrderlyTreeMap 中。这个 map 是 msgTreeMap 的一个子集，只在
        // 顺序消息消费的时候使用
        // ProcessQueue#takeMessags
        public List<MessageExt> takeMessags(final int batchSize) {
            List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
            final long now = System.currentTimeMillis();
            try {
                this.lockTreeMap.writeLock().lockInterruptibly();
                this.lastConsumeTimestamp = now;
                try {
                    if (!this.msgTreeMap.isEmpty()) {
                        for (int i = 0; i < batchSize; i++) {
                            // 从 msgTreeMap 上读取一条消息，并且在读取的同时，会将这条消息从 msgTreeMap 中移除掉
                            Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                            if (entry != null) {
                                result.add(entry.getValue());
                                // 将从 msgTreeMap 中获取到的消息保存到 consuimgMsgOrderlyTreeMap 中
                                consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                            } else {
                                break;
                            }
                        }
                    }
                    if (result.isEmpty()) {
                        consuming = false;
                    }
                } finally {
                    this.lockTreeMap.writeLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("take Messages exception", e);
            }
            return result;
        }

        // commit 方法就是将 consumingMsgOrderlyTreeMap 中的消息移除掉维护 msgCount 和 msgSize 这两个变量
        // ProcessQueue#commit
        public long commit() {
            try {
                this.lockTreeMap.writeLock().lockInterruptibly();
                try {
                    Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
                    // 将 consumingMsgOrderlyTreeMap 中的该批消息从 ProcessQueue 中移除，
                    // 更新 msgCount 和 msgSize 这两个变量
                    msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size());
                    for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                        msgSize.addAndGet(0 - msg.getBody().length);
                    }
                    this.consumingMsgOrderlyTreeMap.clear();
                    // 从这里可以看出，offset 表示消息消费队列的逻辑偏移量，类似于数组的下标，代表第 n 个 ConsumeQueue 条目
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

        // ProcessQueue#takeMessags
        public void makeMessageToCosumeAgain(List<MessageExt> msgs) {
            try {
                this.lockTreeMap.writeLock().lockInterruptibly();
                try {
                    for (MessageExt msg : msgs) {
                        // 将消息从 consumingMsgOrderlyTreeMap 中移除掉
                        this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                        // 将消息重新保存到 msgTreeMap 中
                        this.msgTreeMap.put(msg.getQueueOffset(), msg);
                    }
                } finally {
                    this.lockTreeMap.writeLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("makeMessageToCosumeAgain exception", e);
            }
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