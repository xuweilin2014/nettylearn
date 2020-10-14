public class RocketmqPushConsumerAnalysisTwo{

    public class PullRequest {
        private final RemotingCommand requestCommand;
        private final Channel clientChannel;
        private final long timeoutMillis;
        private final long suspendTimestamp;
        private final long pullFromThisOffset;
        private final SubscriptionData subscriptionData;
        private final MessageFilter messageFilter;
    }

    public class PullMessageProcessor implements NettyRequestProcessor {

        private RemotingCommand processRequest(final Channel channel, RemotingCommand request, boolean brokerAllowSuspend) throws RemotingCommandException {

            final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

            switch (response.getCode()) {
                case ResponseCode.PULL_NOT_FOUND:
                
                // brokerAllowSuspend 表示 Broker 是否支持挂起，表示在未找到消息时会挂起。处理消息拉取时默认传入 true。
                // 如果该参数为 true，表示支持挂起，则会先将响应对象 response 设置为 null，将不会立即向客户端写入响应
                // 如果该参数为 false，未找到消息时直接返回客户端消息未找到
                if (brokerAllowSuspend && hasSuspendFlag) {
                    // 如果支持长轮询，挂起超时时间来源于请求参数 requestHeader
                    long pollingTimeMills = suspendTimeoutMillisLong; 
                    if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                        pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
                    }

                    String topic = requestHeader.getTopic();
                    long offset = requestHeader.getQueueOffset();
                    int queueId = requestHeader.getQueueId();
                    // 创建拉取任务 PullRequest，并且提交到 PullRequestHoldService 线程中，PullRequestHoldService 线程每隔 5s 重试一次
                    PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills, this.brokerController.getMessageStore().now(), offset, subscriptionData, messageFilter);
                    this.brokerController.getPullRequestHoldService().suspendPullRequest(topic, queueId, pullRequest);
                    response = null;
                    break;
                }
            }
            
        }

        public void executeRequestWhenWakeup(final Channel channel, final RemotingCommand request) throws RemotingCommandException {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        // 这里又是长轮询的核心代码，其核心是设置 brokerAllowSuspend 为 false，表示不支持拉取线程挂起，
                        // 即当根据偏移量无法获取到消息时将不挂起线程等待新消息的到来，而是直接返回告诉客户端本次消息拉取未找到消息
                        final RemotingCommand response = PullMessageProcessor.this.processRequest(channel, request, false);

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

        public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
            // 根据消息主题和消息队列构建 key，从 pullRequestTable 中获取到该 主题@队列ID 对应的 ManyPullRequest，它内部持有一个 PullRequest 列表，
            // 表示同一 主题@队列ID 累积拉取的消息队列
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

        private String buildKey(final String topic, final int queueId) {
            StringBuilder sb = new StringBuilder();
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
                    // 如果开启了长轮询机制，每 5s 尝试一次，判断消息是否到达。如果未开启长轮询，则等待 shortPollingTimeMills 之后再尝试，默认 1s
                    if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                        this.waitForRunning(5 * 1000);
                    } else {
                        this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                    }

                    long beginLockTimestamp = this.systemClock.now();
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

        private void checkHoldRequest() {
            // 遍历拉取任务列表，根据键 主题@队列 获取到消息消费队列最大偏移量，如果该偏移量大于待拉取的偏移量，说明有新的消息到达，调用 notifyMessageArriving 触发消息拉取
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

        public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset,
                final Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
            String key = this.buildKey(topic, queueId);
            ManyPullRequest mpr = this.pullRequestTable.get(key);
            if (mpr != null) {
                // 从 ManyPullRequest 中获取所有挂起的拉取任务
                List<PullRequest> requestList = mpr.cloneListAndClear();
                if (requestList != null) {
                    List<PullRequest> replayList = new ArrayList<PullRequest>();

                    for (PullRequest request : requestList) {
                        long newestOffset = maxOffset;
                        if (newestOffset <= request.getPullFromThisOffset()) {
                            newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                        }

                        // 如果消息队列的最大偏移量大于待拉取的偏移量，并且消息匹配，则调用 executeRequestWhenWakeup 将消息返回给消息拉取客户端，否则等待下一次尝试
                        if (newestOffset > request.getPullFromThisOffset()) {
                            boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode, new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                            // match by bit map, need eval again when properties is not null.
                            if (match && properties != null) {
                                match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                            }

                            if (match) {
                                try {
                                    this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                                } catch (Throwable e) {
                                    log.error("execute request when wakeup failed.", e);
                                }
                                continue;
                            }
                        }

                        // 如果挂起超时时间超时，则不继续等待将直接返回客户消息未找到
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
     * PullMessageService 负责对消息队列进行消息拉取，从远端服务器拉取消息后存入消息处理队列 ProcessQueue 中，然后会调用 ConsumeMessageService#submitConsumeRequest 方法进行消息消费，
     * 并且使用线程池来消费消息，确保了消息拉取和消息消费的解耦。RocketMQ 使用 ConsumeMessageService 来实现消息消费的处理逻辑。RocketMQ 支持顺序消费和并发消费。
     * 
     * 从服务器拉取到消息后回调 PullCallBack 回调方法后，先将消息放入到 ProccessQueue 中，然后把消息提交到消费线程池中执行，
     * 也就是调用 ConsumeMessageService#submitConsumeRequest 开始进入到消息消费的世界中来
     */
    public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {

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
             
            // 如果拉取的消息条数大于 consumeMessageBatchMaxSize 则对拉取消息进行分页，每页 consumeMessagBatchMaxSize 条消息，创建多个 ConsumeRequest 任务并提交到
            // 消费线程池。ConsumRequest#run 方法封装了具体消息消费逻辑    
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

        // 恢复重试消息主题名。这是为什么呢？这是由消息重试机制决定的，RocketMQ 将消息存入 commitlog 文件时，如果发现消息的延时级别 delayTimeLevel > 0 会首先
        // 将重试主题存入在消息的属性中，然后设置主题名称为 SCHEDULE_TOPIC，以便时间到后重新参与消息消费
        public void resetRetryTopic(final List<MessageExt> msgs) {
            final String groupTopic = MixAll.getRetryTopic(consumerGroup);
            for (MessageExt msg : msgs) {
                String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                    msg.setTopic(retryTopic);
                }
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

        @Override
        public void run() {
            // 先检查 processQueue 的 dropped，
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;
            ConsumeMessageContext consumeMessageContext = null;

            // 执行消息消费的钩子函数 ConsumeMessageHook#consumeMessageBefore 函数
            // 钩子函数的注册是通过 consumer.getDefaultMQPushConsumerlmpl().registerConsumeMessageHook(hook)
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

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
                // 最终将返回 CONSUME_SUCCESS （消费成功）或 RECONSUME LATER （需要重新消费）
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

            // 执行消息消费的钩子函数 ConsumeMessageHook#consumeMessageAfter 函数
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager().incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            if (!processQueue.isDropped()) {
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        /**
         * 首先，我们需要明确，只有当消费模式为 MessageModel.CLUSTERING(集群模式) 时，Broker 才会自动进行重试，对于广播消息是不会重试的。
         * 集群消费模式下，当消息消费失败，RocketMQ 会通过消息重试机制重新投递消息（sendMessageBack），努力使该消息消费成功。当消费者消费该重试消息后，需要返回结果给 broker，
         * 告知 broker 消费成功（ConsumeConcurrentlyStatus.CONSUME_SUCCESS）或者需要重新消费（ConsumeConcurrentlyStatus.RECONSUME_LATER）。
         * 
         * 只要返回ConsumeConcurrentlyStatus.RECONSUME_LATER，RocketMQ就会认为这批消息消费失败了。
         * 为了保证消息是肯定被至少消费成功一次，RocketMQ 会把这批消息重发回 Broker（topic 不是原 topic 而是这个消费租的 RETRY topic），在延迟的某个时间点（默认是10秒，业务可设置）后，
         * 再次投递到这个ConsumerGroup。而如果一直这样重复消费都持续失败到一定次数（默认16次），就会投递到 DLQ 死信队列
         * 
         * RocketMQ 规定，以下三种情况统一按照消费失败处理并会发起重试。
         * 
         * 业务消费方返回 ConsumeConcurrentlyStatus.RECONSUME_LATER
         * 业务消费方返回 null
         * 业务消费方主动/被动抛出异常
         * 
         * 前两种情况较容易理解，当返回 ConsumeConcurrentlyStatus.RECONSUME_LATER 或者 null 时，broker 会知道消费失败，后续就会发起消息重试，重新投递该消息。
         * 注意 对于抛出异常的情况，只要我们在业务逻辑中显式抛出异常或者非显式抛出异常，broker 也会重新投递消息，如果业务对异常做了捕获，那么该消息将不会发起重试。
         * 因此对于需要重试的业务，消费方在捕获异常的时候要注意返回 ConsumeConcurrentlyStatus.RECONSUMELATER 或 null 并输出异常日志，打印当前重试次数。（推荐返回ConsumeConcurrentlyStatus.RECONSUMELATER）
         */
        public void processConsumeResult(final ConsumeConcurrentlyStatus status, final ConsumeConcurrentlyContext context, final ConsumeRequest consumeRequest) {
            int ackIndex = context.getAckIndex();

            if (consumeRequest.getMsgs().isEmpty())
                return;

            // 根据消息监听器返回的结果，计算 ackIndex，如果返回 CONSUME_SUCCESS，ackIndex 设置为 msgs.size() - 1，
            // 如果返回 RECONSUME_LATER，ackIndex = -1，这是为下文发送 msg back（ACK）做准备
            switch (status) {
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
                        // 如果消息重试失败，则把发送失败的消息再次封装称为 ConsumeRequest，然后延迟 5s 重新消费，如果 ACK 消息发送成功，则该消息会进行延迟消费
                        if (!result) {
                            msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                            msgBackFailed.add(msg);
                        }
                    }

                    if (!msgBackFailed.isEmpty()) {
                        consumeRequest.getMsgs().removeAll(msgBackFailed);
                        this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                    }
                    break;
                default:
                    break;
            }

            long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
            if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
                this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
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

    public class MQClientAPIImpl {

        public void consumerSendMessageBack(final String addr, final MessageExt msg, final String consumerGroup,
                final int delayLevel, final long timeoutMillis, final int maxConsumeRetryTimes)throws RemotingException, MQBrokerException, InterruptedException {

            ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);

            requestHeader.setGroup(consumerGroup);
            requestHeader.setOriginTopic(msg.getTopic());
            requestHeader.setOffset(msg.getCommitLogOffset());
            requestHeader.setDelayLevel(delayLevel);
            requestHeader.setOriginMsgId(msg.getMsgId());
            requestHeader.setMaxReconsumeTimes(maxConsumeRetryTimes);

            // 以同步的方式发送 ACK 请求到服务端
            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
            assert response != null;
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    return;
                }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark());
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

            // 创建重试主题，重试主题名称为 %RETRY% + 消费组名，并且从重试队列中随机选择一个队列
            String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
            int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % subscriptionGroupConfig.getRetryQueueNums();

            int topicSysFlag = 0;
            if (requestHeader.isUnitMode()) {
                topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
            }

            // 使用重试主题，构建 TopicConfig 主题配置消息
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                    newTopic, subscriptionGroupConfig.getRetryQueueNums(), PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
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
                
                // ....
            } else {
                if (0 == delayLevel) {
                    delayLevel = 3 + msgExt.getReconsumeTimes();
                }

                msgExt.setDelayTimeLevel(delayLevel);
            }

            // 根据原先的消息创建一个新的消息对象，重试消息会拥有自己唯一消息 ID(msgID) 并存人到 commitlog 文件中，并不会去更新原先消息，其它属性与原先消息保持相同，主题名称为重试主题
            // 将消息存入到 CommitLog 件中，这里介绍一个机制，消息重试机制依托于定时任务实现，具体如下：
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setTopic(newTopic);
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
            msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

            msgInner.setQueueId(queueIdInt);
            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(this.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

            String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
            MessageAccessor.setOriginMessageId(msgInner,
                    UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);

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

    }
    

}