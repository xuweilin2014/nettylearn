public class RocketmqPushConsumerAnalysisTwo{

    public class PullRequest {
        private final RemotingCommand requestCommand;
        private final Channel clientChannel;
        private final long timeoutMillis;
        private final long suspendTimestamp;
        private final long pullFromThisOffset;
        private final SubscriptionData subscriptionData;
        private final MessageFilter messageFilter;

        public PullRequest(RemotingCommand requestCommand, Channel clientChannel, long timeoutMillis, long suspendTimestamp, long pullFromThisOffset, SubscriptionData subscriptionData,
                MessageFilter messageFilter) {
            this.requestCommand = requestCommand;
            this.clientChannel = clientChannel;
            // timeoutMills 表示的是长轮询的超时时间
            this.timeoutMillis = timeoutMillis;
            // suspendTimestamp 表示的是创建 PullRequest 的当前时间戳
            // timeoutMillis 和 suspendTimestamp 这两个参数加起来就表示这个长轮询请求的到期时间
            this.suspendTimestamp = suspendTimestamp;
            this.pullFromThisOffset = pullFromThisOffset;
            this.subscriptionData = subscriptionData;
            this.messageFilter = messageFilter;
        }
    }

    public class PullMessageProcessor implements NettyRequestProcessor {

        @Override
        public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
            return this.processRequest(ctx.channel(), request, true);
        }

        private RemotingCommand processRequest(final Channel channel, RemotingCommand request, boolean brokerAllowSuspend) throws RemotingCommandException {
            
            RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
            final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
            final PullMessageRequestHeader requestHeader = (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);
            // 将 request 中的 opaque 参数设置到 response 中，opaque 也就是 requestId
            response.setOpaque(request.getOpaque());

            // 省略代码

            // 这个就是 DefaultMQPushConsumerImpl 中 sysFlag 的 FLAG_SUSPEND 位，表示 Consumer 是否希望 Broker 开启长轮询
            final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
            // 从 requestHeader 中获得，这个参数也是 DefaultMQPushConsumerImpl 中的
            // CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND，表示 Consumer 希望 Broker 端长轮询挂起的时间
            final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

            // 省略代码

            MessageFilter messageFilter;
            
            // 构建消息过滤对象 ExpressionForRetryMessageFilter，支持对重试主题的过滤
            if (this.brokerController.getBrokerConfig().isFilterSupportRetry()) {
                messageFilter = new ExpressionForRetryMessageFilter(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager());
            // 构建消息过滤对象 ExpressionMessageFilter，不支持对重试主题消息的过滤
            } else {
                messageFilter = new ExpressionMessageFilter(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager());
            }

            // 调用 MessageStore#getMessage 来查找消息
            final GetMessageResult getMessageResult = this.brokerController.getMessageStore().getMessage(
                    requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(),
                    requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), messageFilter);
            
            if (getMessageResult != null) {
                // 根据 PullResult 来填充 responseHeader 中的 nextBeginOffset、minOffset、maxOffset
                response.setRemark(getMessageResult.getStatus().name());
                responseHeader.setNextBeginOffset(getMessageResult.getNextBeginOffset());
                responseHeader.setMinOffset(getMessageResult.getMinOffset());
                responseHeader.setMaxOffset(getMessageResult.getMaxOffset());

                // 根据主从同步延迟，设置下一次拉取任务的 brokerId
                if (getMessageResult.isSuggestPullingFromSlave()) {
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
                } else {
                    responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
                }

                switch (this.brokerController.getMessageStoreConfig().getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    break;
                case SLAVE:
                    // 如果当前 broker 是 slave 的话，并且根据设置不可以从 slave 读取消息的话，
                    // 就将 response code 设置为 PULL_RETRY_IMMEDIATELY，重新拉取消息
                    if (!this.brokerController.getBrokerConfig().isSlaveReadEnable()) {
                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                        responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
                    }
                    break;
                }

                // 如果配置允许从 slave 端读取消息，那么会配置下次建议拉取的 broker Id
                if (this.brokerController.getBrokerConfig().isSlaveReadEnable()) {
                    // consume too slow ,redirect to another machine
                    if (getMessageResult.isSuggestPullingFromSlave()) {
                        responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
                    }
                    // consume ok
                    else {
                        responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                    }
                // 如果不允许从 slave 读取消息，则直接建议下一次从 master broker 拉取消息
                } else {
                    responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
                }

                // 根据 GetMessageResult 编码转换成 ResponseCode
                switch (getMessageResult.getStatus()) {
                    case FOUND:
                        response.setCode(ResponseCode.SUCCESS);
                        break;
                    case MESSAGE_WAS_REMOVING:
                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                        break;
                    case NO_MATCHED_LOGIC_QUEUE:
                    case NO_MESSAGE_IN_QUEUE:
                        if (0 != requestHeader.getQueueOffset()) {
                            response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                            log.info("the broker store no queue data, fix the request offset {} to {}, Topic: {} QueueId: {} Consumer Group: {}");
                        } else {
                            response.setCode(ResponseCode.PULL_NOT_FOUND);
                        }
                        break;
                    case NO_MATCHED_MESSAGE:
                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                        break;
                    case OFFSET_FOUND_NULL:
                        response.setCode(ResponseCode.PULL_NOT_FOUND);
                        break;
                    case OFFSET_OVERFLOW_BADLY:
                        response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                        log.info("the request offset: {} over flow badly, broker max offset: {}, consumer: {}");
                        break;
                    case OFFSET_OVERFLOW_ONE:
                        response.setCode(ResponseCode.PULL_NOT_FOUND);
                        break;
                    case OFFSET_TOO_SMALL:
                        response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                        log.info("the request offset too small. group={}, topic={}, requestOffset={}, brokerMinOffset={}, clientIp={}",
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueOffset(),
                            getMessageResult.getMinOffset(), channel.remoteAddress());
                        break;
                    default:
                        assert false;
                        break;
                }

                switch (response.getCode()) {
                    case ResponseCode.PULL_NOT_FOUND:
                    
                    // brokerAllowSuspend 表示 Broker 是否支持挂起，即是否允许在未找到消息时暂时挂起线程。第一次调用时默认为 true。
                    // 如果该参数为 true，表示支持挂起，如果没有找到消息则挂起
                    // 如果该参数为 false，未找到消息时直接返回客户端消息未找到
                    if (brokerAllowSuspend && hasSuspendFlag) {
                        // 如果支持长轮询，则根据是否开启长轮询来决定挂起方式。如果 Broker 支持长轮询，挂起超时时间来源于请求参数 requestHeader，
                        // Push 模式默认为 15s，然后创建拉取任务 PullRequest 并且提交到 PullRequestHoldService 线程中
                        long pollingTimeMills = suspendTimeoutMillisLong; 
                        if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                            pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
                        }

                        /** 
                         * RocketMQ 轮询机制由两个线程共同完成：
                         * 1.PullRequestHoldService: 每隔 5s 重试一次
                         * 2.DefaultMessageStore#ReputMessageService: 每处理一次重新拉取，Thread.sleep(1) 继续下一次检查
                         */
    
                        String topic = requestHeader.getTopic();
                        long offset = requestHeader.getQueueOffset();
                        int queueId = requestHeader.getQueueId();
                        // 创建拉取任务 PullRequest，pollingTimeMills 表示的是长轮询的超时时间，now 表示的是当前的时间，这两个时间参数会用来计算
                        // 长轮询的时间间隔
                        PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills, this.brokerController.getMessageStore().now(), offset, subscriptionData, messageFilter);
                        // 将创建好的 PullRequest 提交到 PullRequestHoldService 线程中，PullRequestHoldService 线程每隔 5s 重试一次
                        this.brokerController.getPullRequestHoldService().suspendPullRequest(topic, queueId, pullRequest);
                        // 关键，设置 response = null，则此时此次调用不会向客户端输出任何字节，客户端网络请求的读事件不会触发，客户端处于等待状态
                        response = null;
                        break;
                    }
                }
            } else {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("store getMessage return null");
            }

            boolean storeOffsetEnable = brokerAllowSuspend;
            storeOffsetEnable = storeOffsetEnable && hasCommitOffsetFlag;
            storeOffsetEnable = storeOffsetEnable && this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;
            
            // 如果 commitlog 标记可用，并且当前节点为主节点，那么就更新消息消费进度
            if (storeOffsetEnable) {
                this.brokerController.getConsumerOffsetManager().commitOffset(
                        RemotingHelper.parseChannelRemoteAddr(channel), requestHeader.getConsumerGroup(),
                        requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
            }
            return response;
            
        }

        // PullMessageProcessor#executeRequestWhenWakeup
        public void executeRequestWhenWakeup(final Channel channel, final RemotingCommand request) throws RemotingCommandException {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        // 这里又是长轮询的核心代码，其核心是设置 brokerAllowSuspend 为 false，表示不支持拉取线程挂起，也就是说，在拉取不到消息时不再进行长轮询
                        // 即当根据偏移量无法获取到消息时将不挂起线程等待新消息的到来，而是直接返回告诉客户端本次消息拉取未找到消息
                        // 因为这里 brokerAllowSuspend 为 false，也就表明不支持长轮询，因此不会将 response 置为 null
                        final RemotingCommand response = PullMessageProcessor.this.processRequest(channel, request, false);

                        // 获取到拉取结果，返回客户端
                        if (response != null) {
                            response.setOpaque(request.getOpaque());
                            response.markResponseType();
                            try {
                                channel.writeAndFlush(response).addListener(new ChannelFutureListener() {
                                    @Override
                                    public void operationComplete(ChannelFuture future) throws Exception {
                                        if (!future.isSuccess()) {
                                            // 打印日志
                                        }
                                    }
                                });
                            } catch (Throwable e) {
                                // 打印日志
                            }
                        }
                    } catch (RemotingCommandException e1) {
                        log.error("excuteRequestWhenWakeup run", e1);
                    }
                }
            };
            this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, channel, request));
        }

    }

    public class ManyPullRequest {

        private final ArrayList<PullRequest> pullRequestList = new ArrayList<>();
    
        public synchronized void addPullRequest(final PullRequest pullRequest) {
            this.pullRequestList.add(pullRequest);
        }
    
        public synchronized void addPullRequest(final List<PullRequest> many) {
            this.pullRequestList.addAll(many);
        }
    
        public synchronized List<PullRequest> cloneListAndClear() {
            if (!this.pullRequestList.isEmpty()) {
                List<PullRequest> result = (ArrayList<PullRequest>) this.pullRequestList.clone();
                this.pullRequestList.clear();
                return result;
            }
    
            return null;
        }
    }

    public class PullRequestHoldService extends ServiceThread {

        // topic@queueId -> ManyPullRequest
        private ConcurrentMap<String, ManyPullRequest> pullRequestTable = new ConcurrentHashMap<String, ManyPullRequest>(1024);

        // PullRequestHoldService#suspendPullRequest
        public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
            // 根据消息主题和消息队列构建 key，从 pullRequestTable 中获取到该【主题@队列ID】对应的 ManyPullRequest，它内部持有一个 PullRequest 列表，
            // 表示同一【主题@队列ID】累积拉取的消息队列
            String key = this.buildKey(topic, queueId);
            ManyPullRequest mpr = this.pullRequestTable.get(key);
            if (null == mpr) {
                mpr = new ManyPullRequest();
                ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
                if (prev != null) {
                    mpr = prev;
                }
            }
            // 将 PullRequest 放入到 ManyPullRequest 中
            mpr.addPullRequest(pullRequest);
        }

        // PullRequestHoldService#buildKey
        private String buildKey(final String topic, final int queueId) {
            StringBuilder sb = new StringBuilder();
            // key = topic + @ + queueId
            sb.append(topic);
            sb.append(TOPIC_QUEUEID_SEPARATOR);
            sb.append(queueId);
            return sb.toString();
        }

        @Override
        public void run() {
            log.info("{} service started", this.getServiceName());
            while (!this.isStopped()) {
                try {
                    // 如果开启了长轮询机制，每 5s 尝试一次，判断消息是否到达
                    // 如果未开启长轮询，则等待 shortPollingTimeMills 之后再尝试，shortPollingTimeMills 默认为 1s
                    // 现在有一种场景，在 PullRequest 休眠的5秒钟，如果有消息到达，也需要等待下次调度。
                    //
                    // 如果开启了长轮询，则需要等待 5s 之后才能被唤醒去检查是否有新的消息到达，这样消息的实时性比较差。
                    // RocketMQ 在这边做了优化，在下面是通过 notifyMessageArriving 来做消息是否达到的处理以及再次触发消息拉取。
                    // 因此可以在消息达到的时候直接触发 notifyMessageArriving，来拉取消息返回到客户端。这个逻辑封装在 NotifyMessageArrivingListener 中。
                    // 而这个 Listener 会在消息做 reput 的时候触发
                    //
                    // 简单的来讲，我们生产的消息落到 broker 之后，先是持久化到 commitlog，然后在通过 reput 持久化到 consumequeue 和 index。也正因为持久化到 
                    // consumequeue，我们的客户端才能感知到这条消息的存在。然后在reput这个操作中顺带激活了长轮询休眠的PullRequest
                    if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                        this.waitForRunning(5 * 1000);
                    } else {
                        this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                    }

                    long beginLockTimestamp = this.systemClock.now();
                    // 依次检查 pullRequestTable 中的 PullRequest，也就是判断是否有符合其条件的消息到达 Broker
                    this.checkHoldRequest();
                    long costTime = this.systemClock.now() - beginLockTimestamp;
                    if (costTime > 5 * 1000) {
                        log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                    }
                } catch (Throwable e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info("{} service end", this.getServiceName());
        }

        // PullRequestHoldService#checkHoldRequest
        private void checkHoldRequest() {
            // 遍历拉取任务列表，根据键 【主题@队列】 获取到消息消费队列最大偏移量，如果该偏移量大于待拉取的偏移量，说明有新的消息到达，
            // 调用 notifyMessageArriving 触发消息拉取
            for (String key : this.pullRequestTable.keySet()) {
                String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
                if (2 == kArray.length) {
                    String topic = kArray[0];
                    int queueId = Integer.parseInt(kArray[1]);
                    final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    try {
                        this.notifyMessageArriving(topic, queueId, offset);
                    } catch (Throwable e) {
                        log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                    }
                }
            }
        }

        public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
            String key = this.buildKey(topic, queueId);
            ManyPullRequest mpr = this.pullRequestTable.get(key);
            if (mpr != null) {
                // 从 ManyPullRequest 中获取所有挂起的拉取任务
                List<PullRequest> requestList = mpr.cloneListAndClear();
                if (requestList != null) {
                    List<PullRequest> replayList = new ArrayList<PullRequest>();

                    for (PullRequest request : requestList) {
                        long newestOffset = maxOffset;
                        // newestOffset 表明消息队列中最新的消息 offset
                        // 如果 newestOffset 小于待拉取的偏移量 offset，则重新获取一下消息队列中最新的消息 offset
                        if (newestOffset <= request.getPullFromThisOffset()) {
                            newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                        }

                        // 如果消息队列的最大偏移量大于待拉取的偏移量，说明有新的消息到达
                        // 调用 executeRequestWhenWakeup 将消息返回给消息拉取客户端，否则等待下一次尝试
                        if (newestOffset > request.getPullFromThisOffset()) {
                            boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode, new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                            // match by bit map, need eval again when properties is not null.
                            if (match && properties != null) {
                                match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                            }

                            if (match) {
                                try {
                                    // executeRequestWhenWakeup 方法会触发再次拉取消息
                                    this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                                } catch (Throwable e) {
                                    log.error("execute request when wakeup failed.", e);
                                }
                                continue;
                            }
                        }

                        // 如果挂起超时时间超时，则不继续等待，直接去拉取消息，拉取不到消息也返回
                        if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                            try {
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }

                        replayList.add(request);
                    }

                    if (!replayList.isEmpty()) {
                        mpr.addPullRequest(replayList);
                    }
                }
            }
        }

    }

    public interface ConsumeMessageService {

        void start();
    
        void shutdown();
    
        void updateCorePoolSize(int corePoolSize);
    
        void incCorePoolSize();
    
        void decCorePoolSize();
    
        int getCorePoolSize();
        // 直接消费消息，主要用于通过管理命令收到消费消息
        ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String brokerName);
    
        void submitConsumeRequest(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue, final boolean dispathToConsume);

    }

    /**
     * PullMessageService 负责对消息队列进行消息拉取，从远端服务器拉取消息后存入消息处理队列 ProcessQueue 中，然后会调用 
     * ConsumeMessageService#submitConsumeRequest 方法进行消息消费，并且使用线程池来消费消息，确保了消息拉取和消息消费的解耦。RocketMQ 使用 
     * ConsumeMessageService 来实现消息消费的处理逻辑。RocketMQ 支持顺序消费和并发消费。
     * 
     * 从服务器拉取到消息后回调 PullCallBack 回调方法后，先将消息放入到 ProccessQueue 中，然后把消息提交到消费线程池中执行，也就是调用
     * ConsumeMessageService#submitConsumeRequest 开始进入到消息消费的世界中来
     */
    public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {

        private static final Logger log = ClientLogger.getLog();
        private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
        private final DefaultMQPushConsumer defaultMQPushConsumer;
        private final MessageListenerConcurrently messageListener;
        private final BlockingQueue<Runnable> consumeRequestQueue;
        private final ThreadPoolExecutor consumeExecutor;
        private final String consumerGroup;
        private final ScheduledExecutorService scheduledExecutorService;
        private final ScheduledExecutorService cleanExpireMsgExecutors;

        public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl, MessageListenerConcurrently messageListener) {

            this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
            this.messageListener = messageListener;

            this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
            this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
            this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

            /** 
             * 接下来创建 3 个线程池，一个普通线程池 consumeExecutor，两个定时线程池 scheduledExecutorService 和 cleanExpiredMsgExecutors 
             * cleanExpiredMsgExecutors:用来定时清理过期的消息
             * consumeExecutor:用来进行消息消费
             * scheduledExecutorService:延后一段时间来进行消息消费
             */
            this.consumeExecutor = new ThreadPoolExecutor(this.defaultMQPushConsumer.getConsumeThreadMin(),
                    this.defaultMQPushConsumer.getConsumeThreadMax(), 1000 * 60, TimeUnit.MILLISECONDS,
                    this.consumeRequestQueue, new ThreadFactoryImpl("ConsumeMessageThread_"));

            this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
            this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));

        }

        public void start() {
            this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    // 清理过期的消息
                    cleanExpireMsg();
                }
            }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
        }

        private void cleanExpireMsg() {
            Iterator<Map.Entry<MessageQueue, ProcessQueue>> it = this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, ProcessQueue> next = it.next();
                ProcessQueue pq = next.getValue();
                pq.cleanExpiredMsg(this.defaultMQPushConsumer);
            }
        }

        // ConsumeMessageConcurrentlyService#submitConsumeRequest
        public void submitConsumeRequest(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue, final boolean dispatchToConsume) {
            // consumeMessageBatchMaxSize，在这里看来也就是一次消息消费任务 ConumeRequest 中包含的消息条数，默认为 1
            final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();

            // msgs.size() 默认最多为 32 条，受 DefaultMQPushConsumer 中的 pullBatchSize 属性控制，
            // 如果 msgs.size() 小于 consumeMessageBatchMaxSize ，则直接将拉取到的消息放入到 ConsumeRequest 中，然后将 consumeRequest
            // 提交到消息消费者线程池中，如果提交过程中出现拒绝提交异常则延迟 5s 再提交，这里其实是给出一种标准的拒绝提交实现方式，
            // 实际过程中由于消费者线程池使用的任务队列为 LinkedBlockingQueue 无界队列，故不会出现拒绝提交异常
            if (msgs.size() <= consumeBatchSize) {
                ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    this.submitConsumeRequestLater(consumeRequest);
                }
                
            // 如果拉取的消息条数大于 consumeMessageBatchMaxSize 则对拉取消息进行分页，每页 consumeMessagBatchMaxSize 条消息，创建多个 ConsumeRequest 
            // 任务并提交到消费线程池。ConsumRequest#run 方法封装了具体消息消费逻辑    
            } else {
                for (int total = 0; total < msgs.size();) {
                    List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                    for (int i = 0; i < consumeBatchSize; i++, total++) {
                        if (total < msgs.size()) {
                            msgThis.add(msgs.get(total));
                        } else {
                            break;
                        }
                    }
                    
                    // msgThis 中的 msg 数目小于等于 consumeBatchSize
                    ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                    try {
                        this.consumeExecutor.submit(consumeRequest);
                    } catch (RejectedExecutionException e) {
                        for (; total < msgs.size(); total++) {
                            msgThis.add(msgs.get(total));
                        }

                        this.submitConsumeRequestLater(consumeRequest);
                    }
                }
            }

        }

        // ConsumeMessageConcurrentlyService#sendMessageBack
        public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
            // 当 msg 第一次进行消息重试时，delayLevel 默认设置为 0
            int delayLevel = context.getDelayLevelWhenNextConsume();
            try {
                this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
                return true;
            } catch (Exception e) {
                log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
            }
            return false;
        }

        // 恢复重试消息主题名。这是为什么呢？这是由消息重试机制决定的，Broker 端在对重试消息进行处理时，
        // 会把原始的消息主题（比如 test-topic）保存到 PROPERTY_RETRY_TOPIC 中，替换为重试主题 %RETRY% + consumer group，
        // 所以在这里要将重试主题更改为原来的主题
        // ConsumeMessageConcurrentlyService#resetRetryTopic
        public void resetRetryTopic(final List<MessageExt> msgs) {
            final String groupTopic = MixAll.getRetryTopic(consumerGroup);
            for (MessageExt msg : msgs) {
                String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                    msg.setTopic(retryTopic);
                }
            }
        }

        /**
         * 首先，我们需要明确，只有当消费模式为 MessageModel.CLUSTERING(集群模式) 时，Broker 才会自动进行重试，对于广播消息是不会重试的。
         * 集群消费模式下，当消息消费失败，RocketMQ 会通过消息重试机制重新投递消息（sendMessageBack），努力使该消息消费成功。当消费者消费该重试消息后，
         * 需要返回结果给 broker，告知 broker 消费成功（ConsumeConcurrentlyStatus.CONSUME_SUCCESS）或者需要重新消费（ConsumeConcurrentlyStatus.RECONSUME_LATER）。
         * 
         * 只要返回 ConsumeConcurrentlyStatus.RECONSUME_LATER，RocketMQ 就会认为这批消息消费失败了。
         * 为了保证消息是肯定被至少消费成功一次，RocketMQ 会把这批消息重发回 Broker（topic 不是原 topic 而是这个消费组的 RETRY topic），在延迟的某个时间点
         * （默认是10秒，业务可设置）后，再次投递到这个 ConsumerGroup。而如果一直这样重复消费都持续失败到一定次数（默认16次），就会投递到 DLQ 死信队列
         * 
         * RocketMQ 规定，以下三种情况统一按照消费失败处理并会发起重试。
         * 
         * 业务消费方返回 ConsumeConcurrentlyStatus.RECONSUME_LATER
         * 业务消费方返回 null
         * 业务消费方主动/被动抛出异常
         * 
         * 前两种情况较容易理解，当返回 ConsumeConcurrentlyStatus.RECONSUME_LATER 或者 null 时, rocketmq 会知道消费失败，后续就会发起消息重试，重新投递该消息。
         * 注意 对于抛出异常的情况，只要我们在业务逻辑中显式抛出异常或者非显式抛出异常，rocketmq 也会重新投递消息，如果业务对异常做了捕获，那么该消息将不会发起重试。
         * 因此对于需要重试的业务，消费方在捕获异常的时候要注意返回 ConsumeConcurrentlyStatus.RECONSUMELATER 或 null 并输出异常日志，打印当前重试次数。
         * （推荐返回 ConsumeConcurrentlyStatus.RECONSUMELATER）
         */
        public void processConsumeResult(final ConsumeConcurrentlyStatus status, final ConsumeConcurrentlyContext context, final ConsumeRequest consumeRequest) {
            // 从哪里开始重试，ackIndex默认是int最大值，除非用户自己指定了从哪些消息开始重试
            int ackIndex = context.getAckIndex();

            if (consumeRequest.getMsgs().isEmpty())
                return;

            // 根据消息监听器返回的结果，计算 ackIndex，如果返回 CONSUME_SUCCESS，ackIndex 设置为 msgs.size() - 1，
            // 如果返回 RECONSUME_LATER，ackIndex = -1，这是为下文发送 msg back（ACK）做准备
            switch (status) {
                // 即使是CONSUME_SUCCESS，也可能部分消息需要重试, ackIndex 表示的是消费成功的消息的索引
                case CONSUME_SUCCESS:
                    if (ackIndex >= consumeRequest.getMsgs().size()) {
                        ackIndex = consumeRequest.getMsgs().size() - 1;
                    }
                    int ok = ackIndex + 1;
                    int failed = consumeRequest.getMsgs().size() - ok;
                    break;
                case RECONSUME_LATER:
                    ackIndex = -1;
                    break;
                default:
                    break;
            }

            switch (this.defaultMQPushConsumer.getMessageModel()) {
                // 对于广播模式，是不会进行重试消息，有可能是因为代价太大，只是以警告级别输出到日志中
                case BROADCASTING:
                    for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                        MessageExt msg = consumeRequest.getMsgs().get(i);
                        log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                    }
                    break;
                
                // 对于集群模式，如果消息消费成功，由于 ackIndex = consumeRequest.getMsgs().size() - 1，而 i = ackIndex + 1 等于 consumeRequest.getMsgs().size()
                // 因此，并不会执行消息重试，也就是 sendMessageBack 方法。如果消息消费失败（RECONSUME_LATER），则该消息需要进行消息重试
                case CLUSTERING:
                    List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
                    for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                        MessageExt msg = consumeRequest.getMsgs().get(i);
                        // 进行消息重试
                        boolean result = this.sendMessageBack(msg, context);
                        // 如果消息重试失败，则把消息添加到 msgBackFailed 中，稍后再进行一次消费
                        // 则把发送失败的消息再次封装称为 ConsumeRequest，然后延迟 5s 重新消费，如果 ACK 消息发送成功，则该消息会进行延迟消费
                        if (!result) {
                            msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                            msgBackFailed.add(msg);
                        }
                    }

                    if (!msgBackFailed.isEmpty()) {
                        consumeRequest.getMsgs().removeAll(msgBackFailed);
                        // 发回 broker 失败，则再次尝试本地消费
                        this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                    }
                    break;
                default:
                    break;
            }

            // 将消费前缓存的消息从 ProcessQueue 中清除
            // 这里有一个问题，在普通消息消费的时候，是并发处理，如果出现offset靠后的消息先被消费完，但是我们的offset靠前的还没有被消费完，
            // 这个时候出现了宕机，我们的offset靠前的这部分数据是否会丢失呢？也就是下次消费的时候是否会从offset靠后的没有被消费的开始消费呢？
            // 如果不是的话，rocketmq是怎么做到的呢？
            // 
            // 将消费前缓存的消息从 ProcessQueue 中清除，如果不深入进去看内部逻辑，这里会误以为，它会将当前消息的offset给更新到最新的消费进度，
            // 那问题三中说的中间的offset是有可能被丢失的，但实际上是不会发生的，具体的逻辑保证在removeMessage中。
            // 在removeMessage中通过msgTreeMap去做了一个保证，msgTreeMap是一个TreeMap，根据offset升序排序，如果treeMap中有值的话，他返回的offset就会是当前msgTreeMap中的firstKey，
            // 而不是当前的offset，从而就解决了上面的问题
            long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
            // 更新 offset
            if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
                this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
            }
        }

    }

    class ConsumeRequest implements Runnable {

        private final List<MessageExt> msgs;
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        // ConsumeRequest#run
        // 消息处理的逻辑比较简单，就是回调Consumer启动时注册的Listener。无论Listener是否处理成功，消息都会从ProcessQueue中移除掉
        @Override
        public void run() {
            // 先检查 processQueue 的 dropped，
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}");
                return;
            }

            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;
            ConsumeMessageContext consumeMessageContext = null;

            // 执行消息消费的钩子函数 ConsumeMessageHook#consumeMessageBefore 函数...
            // 钩子函数的注册是通过 consumer.getDefaultMQPushConsumerlmpl().registerConsumeMessageHook(hook)

            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                ConsumeMessageConcurrentlyService.this.resetRetryTopic(msgs);
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }
                // 执行具体的消息消费，调用应用程序消息监昕器的 consumeMessage 方法，进入到具体的消息消费业务逻辑，返回该批消息的消费结果，
                // 最终将返回 CONSUME_SUCCESS （消费成功）或 RECONSUME_LATER （需要重新消费）
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                // 打印日志
                hasException = true;
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
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            if (null == status) {
                // 打印日志
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            // 执行消息消费的钩子函数 ConsumeMessageHook#consumeMessageAfter 函数...

            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager().incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            // 执行业务消息消费后，在处理结果前再次验证一下 ProcessQueue 的 isDropped 状态值。前面说过，在 RebalanceImpl#updateProcessQueueTableInRebalance
            // 方法中，如果当前 consumer 中的某一个 ProcessQueue 不再被使用，那么就将这个 PrcocessQueue 状态设置为 dropped。
            // 也就是如果由于新的消费者加入或者原先的消费者出现宕机导致原先分配给消费者的队列在负载之后分配给别的消费者，那么
            // 从 rocketmq 的角度看，消息就可能会被重复消费，所以必须将 ProcessQueue 设置为 dropped。并且在这里，如果
            // dropped 为 true，就不对结果进行处理。
            if (!processQueue.isDropped()) {
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
            int delayLevel = context.getDelayLevelWhenNextConsume();
    
            try {
                this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
                return true;
            } catch (Exception e) {
                log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
            }
    
            return false;
        }

    }

    public class DefaultMQPushConsumerImpl implements MQConsumerInner {

        public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName) throws Exception{
            try {
                String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName) : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
                this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg, this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());
            } catch (Exception e) {
                log.error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e);
                // ignore code
            }
        }

    }

    public class ConsumerSendMsgBackRequestHeader implements CommandCustomHeader {
        // 消费物理偏移量
        @CFNotNull
        private Long offset;
        // 消费组名
        @CFNotNull
        private String group;
        // 延迟级别，RocketMQ 不支持精确的定时消息，而是提供几个延迟级别
        @CFNotNull
        private Integer delayLevel;

        private String originMsgId;

        private String originTopic;

        @CFNullable
        private boolean unitMode = false;
        // 最大重试次数，默认为 16 次
        private Integer maxReconsumeTimes;

    }

    public class SendMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {

        @Override
        // SendMessageProcessor#processRequest
        public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
            SendMessageContext mqtraceContext;
            switch (request.getCode()) {
            // RequestCode.CONSUMER_SEND_MSG_BACK 由下面的方法处理
            case RequestCode.CONSUMER_SEND_MSG_BACK:
                return this.consumerSendMsgBack(ctx, request);
            // RequestCode.SEND_MESSAGE 以及其它的 RequestCode 由下面的方法进行处理
            default:
                SendMessageRequestHeader requestHeader = parseRequestHeader(request);
                if (requestHeader == null) {
                    return null;
                }

                mqtraceContext = buildMsgContext(ctx, requestHeader);
                this.executeSendMessageHookBefore(ctx, request, mqtraceContext);

                RemotingCommand response;
                if (requestHeader.isBatch()) {
                    response = this.sendBatchMessage(ctx, request, mqtraceContext, requestHeader);
                } else {
                    response = this.sendMessage(ctx, request, mqtraceContext, requestHeader);
                }

                this.executeSendMessageHookAfter(response, mqtraceContext);
                return response;
            }
        }

        // SendMessageProcessor#sendMessage
        private RemotingCommand sendMessage(final ChannelHandlerContext ctx, final RemotingCommand request, final SendMessageContext sendMessageContext, 
                final SendMessageRequestHeader requestHeader) throws RemotingCommandException {

            final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
            final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();
            // opaque 等同于 requestId，将其保存到 response 中
            response.setOpaque(request.getOpaque());

            response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
            response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

            log.debug("receive SendMessage request command, {}", request);
            // startTimestamp 表示 Broker 可以开始获取请求的时间戳
            final long startTimstamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();
            if (this.brokerController.getMessageStore().now() < startTimstamp) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimstamp)));
                return response;
            }

            response.setCode(-1);
            // 进行对应的检查
            // 1.检查该 Broker 是否有写权限
            // 2.检查该 topic 是否可以进行消息发送，主要针对默认主题，默认主题不能发送消息，仅仅用于路由查找
            // 3.在 NameServer 端存储主题的配置信息，默认路径是 ${ROCKET_HOME}/store/config/topic.json
            // 4.检查队列 id，如果队列 id 不合法，返回错误码
            super.msgCheck(ctx, requestHeader, response);
            if (response.getCode() != -1) {
                return response;
            }

            final byte[] body = request.getBody();

            int queueIdInt = requestHeader.getQueueId();
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

            if (queueIdInt < 0) {
                queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
            }

            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setTopic(requestHeader.getTopic());
            msgInner.setQueueId(queueIdInt);
            
            // 如果消息的重试次数超过允许的最大重试次数，消息将进入到 DLQ 延迟队列，延迟队列的主题为：%DLQ% + 消费组名
            if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig)) {
                return response;
            }

            // 将 Message 中的消息保存到 MessageExtBrokerInner 中
            msgInner.setBody(body);
            msgInner.setFlag(requestHeader.getFlag());
            MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
            msgInner.setPropertiesString(requestHeader.getProperties());
            msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
            msgInner.setBornHost(ctx.channel().remoteAddress());
            msgInner.setStoreHost(this.getStoreHost());
            msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());

            PutMessageResult putMessageResult = null;
            Map<String, String> oriProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());
            // 如果从消息中获取 PROPERTY_TRANSACTION_PREPARED 属性的值，这个值表明消息是否是事务消息中的 prepare 消息，
            // Broker 端在收到消息存储请求时，如果消息为 prepare 消息，则执行 prepareMessage 方法，否则走普通的消息存储流程
            String traFlag = oriProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
            if (traFlag != null && Boolean.parseBoolean(traFlag)) {
                // 如果 Broker 被配置为拒绝事务消息，那么就会直接返回 NO_PERMISSION
                if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
                    response.setCode(ResponseCode.NO_PERMISSION);
                    response.setRemark("the broker [" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] sending transaction message is forbidden");
                    return response;
                }
                putMessageResult = this.brokerController.getTransactionalMessageService().prepareMessage(msgInner);
            } else {
                // 调用 DefaultMessageStore#putMessage 进行消息存储，
                putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
            }

            return handlePutMessageResult(putMessageResult, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt);
        }

        private RemotingCommand consumerSendMsgBack(final ChannelHandlerContext ctx, final RemotingCommand request) throws RemotingCommandException {

            final RemotingCommand response = RemotingCommand.createResponseCommand(null);
            final ConsumerSendMsgBackRequestHeader requestHeader = (ConsumerSendMsgBackRequestHeader) request.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);

            // .......

            SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getGroup());
            // 先获取消费组的订阅配置信息，如果配置信息为空则返回配置组信息不存在的错误
            if (null == subscriptionGroupConfig) {
                response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
                response.setRemark("subscription group not exist, " + requestHeader.getGroup() + " " + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
                return response;
            }

            // ......

            // 如果重试队列数量小于 1，则直接返回成功，说明该消费组不支持重试
            if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            }

            // 创建重试主题，重试主题名称为 %RETRY% + 消费组名
            String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
            // 创建一个新的消息队列 id，queueId，从重试队列中随机选择一个队列
            int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % subscriptionGroupConfig.getRetryQueueNums();

            int topicSysFlag = 0;
            if (requestHeader.isUnitMode()) {
                topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
            }

            // 使用重试主题，构建 TopicConfig 主题配置消息
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, subscriptionGroupConfig.getRetryQueueNums(),  PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
            if (null == topicConfig) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("topic[" + newTopic + "] not exist");
                return response;
            }

            // ....

            // 根据消息物理偏移量从 commitlog 文件中获取消息，同时将消息的主题存入属性中
            MessageExt msgExt = this.brokerController.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
            if (null == msgExt) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("look message by offset failed, " + requestHeader.getOffset());
                return response;
            }

            // PROPERTY_RETRY_TOPIC 保存的是这个消息原始的 topic，比如 test-topic 
            final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
            if (null == retryTopic) {
                MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
            }
            msgExt.setWaitStoreMsgOK(false);
            int delayLevel = requestHeader.getDelayLevel();

            // 获取最大重试次数
            int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
            if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
                maxReconsumeTimes = requestHeader.getMaxReconsumeTimes();
            }

            // 设置消息重试次数， 如果消息重试次数超过 maxReconsumeTimes ，再次改变 newTopic 主题为 DLQ （"%DLQ%"），该主题的权限为只写，
            // 说明消息一旦进入到 DLQ 队列中， RocketMQ 将不负责再次调度进行消费了， 需要人工干预
            if (msgExt.getReconsumeTimes() >= maxReconsumeTimes || delayLevel < 0) {
                newTopic = MixAll.getDLQTopic(requestHeader.getGroup());
                queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;
                topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, DLQ_NUMS_PER_GROUP, PermName.PERM_WRITE, 0);
                
                // ignore code
            } else {
                // 正常的消息会进入 else 分支，对于首次重试的消息，默认的 delayLevel 是 0，rocketMQ 会将给该 level + 3，也就是加到 3，
                // 这就是说，如果没有显示的配置延时级别，消息消费重试首次，是延迟了第三个级别发起的重试，从表格中看也就是距离首次发送 10s 后重试
                if (0 == delayLevel) {
                    delayLevel = 3 + msgExt.getReconsumeTimes();
                }
                msgExt.setDelayTimeLevel(delayLevel);
            }

            // 根据原先的消息创建一个新的消息对象，重试消息会拥有自己唯一消息 ID(msgID) 并存人到 commitlog 文件中，并不会去更新原先消息（也就是 msgExt），
            //
            // 将消息存入到 CommitLog 件中，这里介绍一个机制，消息重试机制依托于定时任务实现。
            // MessageExtBrokerInner，也就是对重试的消息，rocketMQ 会创建一个新的 MessageExtBrokerInner 对象，它实际上是继承了 MessageExt
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            // 使用新的 topic，也就是 %RETRY% + ConsumerGroup
            msgInner.setTopic(newTopic);
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
            msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));
            // 设置重试队列的队列 id
            msgInner.setQueueId(queueIdInt);
            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(this.getStoreHost());
            // 刷新消息的重试次数为当前次数加 1
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

            String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
            MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);

            // 消息刷盘
            // 按照正常的消息消费流程，消息保存在 broker 之后，consumer 就可以拉取消费了，和普通消息不一样的是拉取消息的并不是 consumer 本来订阅的 topic，
            // 而是 %RETRY%+group
            // 这里保存消息到 CommitLog 中，如果 delayLevel > 0，则将 msgInner 的 topic 由 %RETRY% + consumerGroup 更改为 SCHEDULE_TOPIC_XXXX，
            // 并且将队列 id 改为 delayLevel - 1，然后将原始的 topic（%RETRY% + consumer group）和 queueId 保存到 properties 的 PROPERTY_REAL_TOPIC
            // 和 PROPERTY_REAL_QUEUE_ID 属性中
            PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
            if (putMessageResult != null) {
                switch (putMessageResult.getPutMessageStatus()) {
                    case PUT_OK:
                        String backTopic = msgExt.getTopic();
                        String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                        if (correctTopic != null) {
                            backTopic = correctTopic;
                        }

                        this.brokerController.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(), backTopic);

                        response.setCode(ResponseCode.SUCCESS);
                        response.setRemark(null);

                        return response;
                    default:
                        break;
                }

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(putMessageResult.getPutMessageStatus().name());
                return response;
            }

            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("putMessageResult is null");
            return response;
        }

        public PutMessageResult putMessage(MessageExtBrokerInner msg) {

            // ignore code

            long beginTime = this.getSystemClock().now();
            // 这句代码，通过 commitLog 我们可以认为这里是真实刷盘操作，也就是消息被持久化了
            //
            // 
            // 我们知道后台重放消息服务 ReputMessageService 会一直监督 CommitLog 文件是否添加了新的消息。
            // 当有了新的消息后，重放消息服务会取出消息并封装为 DispatchRequest 请求，然后将其分发给不同的三个分发服务，建立消费队列文件服务就是这其中之一。
            // 而此处当取消息封装为 DispatchRequest 的时候，当遇到定时消息时，又多做了一些额外的事情。
            // 当遇见定时消息时，CommitLog 计算 tagsCode 标签码与普通消息不同。对于定时消息，tagsCode 值设置的是这条消息的投递时间，即建立消费队列文件的时候，
            // 文件中的 tagsCode 存储的是这条消息未来在什么时候被投递
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

    public class NotifyMessageArrivingListener implements MessageArrivingListener {
        private final PullRequestHoldService pullRequestHoldService;
    
        public NotifyMessageArrivingListener(final PullRequestHoldService pullRequestHoldService) {
            this.pullRequestHoldService = pullRequestHoldService;
        }
    
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
            long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
            this.pullRequestHoldService.notifyMessageArriving(topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties);
        }
    }
    

}