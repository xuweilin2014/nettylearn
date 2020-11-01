public class RocketmqProducer{

    /**
     * 消息生产者发送消息的流程：
     * 
     * 1.检查 producer 是否处于运行状态，也就是调用 DefaultMQProducerImpl#makeSureOK 方法，确保 serviceState 等于 RUNNING
     * 2.调用 DefaultMQProducerImpl#tryToFindTopicPublishInfo 方法获取到 topic 主题的路由信息
     * 3.如果前面获取到有效的路由信息之后，计算一下重试次数。对于同步模式来说，如果发送失败，还有两次重试机会，但是对于异步和 ONEWAY 方式，
     * 没有重试次数。
     * 4.选择一个消息队列。这里根据 sendLatencyFaultEnable 开关是否打开，来选择是否开启 Broker 的故障规避策略，从而来选择对应的 MessageQueue
     * 5.调用 MQClientAPIImpl 来将消息真正的发送到 Broker 端
     * 6.更新发送到的 Broker 端的"故障信息"（保存在 LatencyFaultToleranceImpl#faultItemTable），这些"故障信息"只有在开启了故障规避策略的时候才会使用到
     */

    /**
     * Broker 的故障规避机制：
     * 
     * 熟悉 RocketMQ 的小伙伴应该都知道，RocketMQ Topic 路由注册中心 NameServer 采用的是最终一致性模型，而且客户端是定时向 NameServer 拉取 Topic 的路由信息，
     * 即客户端（Producer、Consumer）是无法实时感知 Broker 宕机的，这样消息发送者会继续向已宕机的 Broker 发送消息，造成消息发送异常。那 RocketMQ 是如何保证消
     * 息发送的高可用性呢？
     * 
     * RocketMQ 为了保证消息发送的高可用性，在内部引入了重试机制，默认重试 2 次。RocketMQ 消息发送端采取的队列负载均衡默认采用轮循。在 RocketMQ 中消息发送者是
     * 线程安全的，即一个消息发送者可以在多线程环境中安全使用。每一个消息发送者全局会维护一个 Topic 上一次选择的队列，然后基于这个序号进行递增轮循，用 sendWhichQueue 表示。
     * 
     * topicA 在 broker-a、broker-b 上分别创建了 4 个队列，例如一个线程使用 Producer 发送消息时，通过对 sendWhichQueue getAndIncrement() 方法获取下一个队列。
     * 例如在发送之前 sendWhichQueue 该值为 broker-a 的 q1，如果由于此时 broker-a 的突发流量异常大导致消息发送失败，会触发重试，按照轮循机制，下一个选择的队列为 
     * broker-a 的 q2 队列，此次消息发送大概率还是会失败，即尽管会重试 2 次，但都是发送给同一个 Broker 处理，此过程会显得不那么靠谱，即大概率还是会失败，那这样重试的意义
     * 将大打折扣。
     * 
     * 故 RocketMQ 为了解决该问题，引入了故障规避机制，在消息重试的时候，会尽量规避上一次发送的 Broker，回到上述示例，当消息发往 broker-a q1 队列时返回发送失败，
     * 那重试的时候，会先排除 broker-a 中所有队列，即这次会选择 broker-b q1 队列，增大消息发送的成功率。
     * 
     * 但 RocketMQ 提供了两种规避策略，该参数由 sendLatencyFaultEnable 控制，用户可干预，表示是否开启延迟规避机制，默认为不开启。（DefaultMQProducer中设置这两个参数）
     * 
     * sendLatencyFaultEnable 设置为 false：默认值，不开启，延迟规避策略只在重试时生效，例如在一次消息发送过程中如果遇到消息发送失败，规避 broekr-a，
     * 但是在下一次消息发送时，即再次调用 DefaultMQProducer 的 send 方法发送消息时，还是会选择 broker-a 的消息进行发送，只要继续发送失败后，重试时再次规避 broker-a。
     * 
     * sendLatencyFaultEnable 设置为 true：开启延迟规避机制，一旦消息发送失败会将 broker-a "悲观"地认为在接下来的一段时间内该 Broker 不可用，
     * 在为未来某一段时间内所有的客户端不会向该 Broker 发送消息。这个延迟时间就是通过 notAvailableDuration、latencyMax 共同计算的，就首先先计算本次消息
     * 发送失败所耗的时延，然后对应 latencyMax 中哪个区间，即计算在 latencyMax 的下标，然后返回 notAvailableDuration 同一个下标对应的延迟值。
     *
     * 温馨提示：如果所有的 Broker 都触发了故障规避，并且 Broker 只是那一瞬间压力大，那岂不是明明存在可用的 Broker，但经过你这样规避，反倒是没有 Broker 可用来，
     * 那岂不是更糟糕了？针对这个问题，会退化到队列轮循机制，即不考虑故障规避这个因素，按自然顺序进行选择进行兜底。
     */

    /**
     * 故障规避机制的实现原理：
     * 
     * 对于 Broker 的规避机制来说，首先，一个 Broker 可用的标准是这个 Broker 的名称 broker name 不在 LatencyFaultToleranceImpl#faultItemTable 中，
     * 或者当前的时间戳大于这个 Broker 对应的 faultItem 的 startTimestamp，也就是可用的时间戳。
     * 
     * 在 DefaultMQProducerImpl#sendDefaultImpl 方法中，每次发送完消息到 Broker 端之后，都会把对 LatencyFaultToleranceImpl#faultItemTable 进行更新，要么
     * 创建一个新的 faultItem，要么会对已经存在的进行更新。这里的更新主要是对 faultItem 的 currentLatency 和 startTimestamp 属性进行更新。
     * 其中 currentLatency 就是在 DefaultMQProducerImpl#sendDefaultImpl 方法中，发送完一个消息花费的时间。根据这个 currentLatency，如果比较长的话，就说明
     * 从发送消息到接收消息花费的时间比较长，此 Broker 的可用性较低，因此将其不可用的时间（notAvailableTime）设置为一个比较长的时间，再加上当前时间：
     * startTimestamp = System.currentTimeMillis() + notAvailableTime。因此，faultItem 中的 startTimestamp 就表明这个 Broker 下次可以启用的时间。
     * 同理，当消息发送给一个 Broker 的延迟比较低时，这时 rocketmq 就会认为这个 Broker 的可用性很高，一般将其不可用时间（notAvailableTime）设置为 0，
     * 所以其 faultItem 的 startTimestamp 就等于当前时间，表明立即可用。
     * 
     * 因此，rocketmq 的 Broker 的故障规避机制就是靠 LatencyFaultToleranceImpl#faultItemTable 实现的，其中每一个 faultItem 都代表了一个 Broker 的可用状态。
     * 然后依据这些 Broker 的可用状态来选择对应的消息队列 MessageQueue。
     */ 

    public class Message implements Serializable {

        private static final long serialVersionUID = 8445773977080406428L;
        // 消息所属的主题
        private String topic;
        
        private int flag;
        // 扩展属性
        // Message 的扩展属性主要包含以下几个方面：
        // 1.tag: 消息 TAG，用于消息过滤
        // 2.keys: Message 索引键，多个用空格隔开，rocketmq 可以根据这些 key 值来完成快速的检索
        // 3.waitStoreMsgOK ：消息发送时是否等消息存储完成后再返回
        // 4.delayTimeLeve：消息延迟级别，用于定时消息或消息重试
        // 这些扩展存储于 properties 属性中
        private Map<String, String> properties;
        // 消息体
        private byte[] body;

        public Message(String topic, byte[] body) {
            this(topic, "", "", 0, body, true);
        }
    
        public Message(String topic, String tags, String keys, int flag, byte[] body, boolean waitStoreMsgOK) {
            this.topic = topic;
            this.flag = flag;
            this.body = body;
    
            if (tags != null && tags.length() > 0)
                this.setTags(tags);
    
            if (keys != null && keys.length() > 0)
                this.setKeys(keys);
    
            this.setWaitStoreMsgOK(waitStoreMsgOK);
        }

        public void setKeys(String keys) {
            this.putProperty(MessageConst.PROPERTY_KEYS, keys);
        }

        public void setTags(String tags) {
            this.putProperty(MessageConst.PROPERTY_TAGS, tags);
        }

        public void setDelayTimeLevel(int level) {
            this.putProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(level));
        }

        public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
            this.putProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, Boolean.toString(waitStoreMsgOK));
        }

    }

    public class DefaultMQProducer extends ClientConfig implements MQProducer {
        // 生产者所属的组
        private String producerGroup;
        
        private String createTopicKey = MixAll.DEFAULT_TOPIC;
        // 默认主题在每一个 Broker 上的队列的数量
        private volatile int defaultTopicQueueNums = 4;
        // 发送消息的默认超时时间，默认为 3s
        private int sendMsgTimeout = 3000;
        // 消息体的大小超过该值时，启用压缩
        private int compressMsgBodyOverHowmuch = 1024 * 4;
        // 同步方式发送消息重试次数，默认为 2，总共执行 3 次
        private int retryTimesWhenSendFailed = 2;
        // 异步方式发送消息重试次数，默认为 2
        private int retryTimesWhenSendAsyncFailed = 2;
        // 消息重试时选择另外一个 Broker 时，是否不等待存储结果就返回 默认为 false
        private boolean retryAnotherBrokerWhenNotStoreOK = false;
        // 允许发送的最大消息长度，默认为 4M ，该值最大值为 2^32-1
        private int maxMessageSize = 1024 * 1024 * 4; // 4M

        // 查找该 topic 下的所有消息队列
        @Override
        public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
            return this.defaultMQProducerImpl.fetchPublishMessageQueues(topic);
        }

        // 单向消息发送，消息发送出去后该方法立即返回，不过这里会指定消息队列选择算法，覆盖消息生产者自带的消息队列负载
        @Override
        public void sendOneway(Message msg, MessageQueueSelector selector, Object arg) throws Exception {
            this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
        }

        // 单向消息发送，发送到指定队列
        @Override
        public void sendOneway(Message msg, MessageQueue mq) throws Exception {
            this.defaultMQProducerImpl.sendOneway(msg, mq);
        }

        // 单向消息发送，就是不在乎发送结果，消息发送出去后该方法立即返回
        @Override
        public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
            this.defaultMQProducerImpl.sendOneway(msg);
        }

        // 异步发送消息，sendCallback 参数是消息发送成功后的回调方法
        @Override
        public void send(Message msg, SendCallback sendCallback)throws MQClientException, RemotingException, InterruptedException {
            this.defaultMQProducerImpl.send(msg, sendCallback);
        }

        // 异步发送消息，如果发送超过 timeout 指定的值，则抛出超时异常
        @Override
        public void send(Message msg, SendCallback sendCallback, long timeout)throws MQClientException, RemotingException, InterruptedException {
            this.defaultMQProducerImpl.send(msg, sendCallback, timeout);
        }

        // 异步发送消息，指定消息队列选择算法，并且指定发送的超时时间
        @Override
        public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout) throws Exception {
            this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback, timeout);
        }

        // 异步发送消息，指定消息队列的选择算法
        @Override
        public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback) throws Exception {
            this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback);
        }

        // 同步消息发送，指定消息队列选择算法，覆盖消息生产者默认的消息队列负载，并且指定发送的超时时间
        @Override
        public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout) throws Exception {
            return this.defaultMQProducerImpl.send(msg, selector, arg, timeout);
        }

        // 同步消息发送，指定消息队列选择算法
        @Override
        public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws Exception {
            return this.defaultMQProducerImpl.send(msg, selector, arg);
        }

        // 异步消息发送，发送到指定消息队列，并且指定发送的超时时间
        @Override
        public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout) throws Exception {
            this.defaultMQProducerImpl.send(msg, mq, sendCallback, timeout);
        }

        // 异步发送消息，发送到指定消息队列
        @Override
        public void send(Message msg, MessageQueue mq, SendCallback sendCallback) throws Exception {
            this.defaultMQProducerImpl.send(msg, mq, sendCallback);
        }

        // 同步发送消息，发送到指定队列，并且指定消息发送的超时时间
        @Override
        public SendResult send(Message msg, MessageQueue mq, long timeout) throws Exception {
            return this.defaultMQProducerImpl.send(msg, mq, timeout);
        }

        // 同步发送消息，发送到指定队列
        @Override
        public SendResult send(Message msg, MessageQueue mq) throws Exception {
            return this.defaultMQProducerImpl.send(msg, mq);
        }

        // 同步发送消息，并且指定发送超时时间
        @Override
        public SendResult send(Message msg, long timeout) throws Exception {
            return this.defaultMQProducerImpl.send(msg, timeout);
        }

        // 同步发送消息
        @Override
        public SendResult send(Message msg) throws Exception {
            return this.defaultMQProducerImpl.send(msg);
        }

    }

    public class DefaultMQProducerImpl implements MQProducerInner{

        private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<String, TopicPublishInfo>();

        /**
         * 消息发送的基本流程如下：
         * 
         * 1.验证消息
         * 2.查找路由
         * 3.消息发送（包含异常发送机制）
         */
        private SendResult sendDefaultImpl(Message msg, final CommunicationMode communicationMode, final SendCallback sendCallback, final long timeout)
                throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
            // 消息发送之前，首先确保生产者处于运行状态
            this.makeSureStateOK();
            // 对消息进行验证，具体就是检查消息的主题，消息对象不能为 null，消息体的长度不能等于 0，且不能大于消息的最大长度 4MB
            Validators.checkMessage(msg, this.defaultMQProducer);

            final long invokeID = random.nextLong();
            long beginTimestampFirst = System.currentTimeMillis();
            long beginTimestampPrev = beginTimestampFirst;
            long endTimestamp = beginTimestampFirst;

            // 消息发送之前，首先需要获取主题的路由信息，只有获取了这些信息我们才知道消息要发送到的具体的 Broker 节点
            TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());

            if (topicPublishInfo != null && topicPublishInfo.ok()) {
                MessageQueue mq = null;
                Exception exception = null;
                SendResult sendResult = null;
                // 重试次数，同步模式下默认为 3 次，ONEWAY 或者异步的情况下不进行重试
                int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
                int times = 0;
                String[] brokersSent = new String[timesTotal];

                for (; times < timesTotal; times++) {
                    // lastBrokerName 为上一次发送失败的 Broker 的名称，第一次发送时 mq 为 null
                    String lastBrokerName = null == mq ? null : mq.getBrokerName();
                    // 选择一个消息队列，有两种选择策略：开启 Broker 故障规避；不开启 Broker 故障规避
                    MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
                    if (mqSelected != null) {
                        mq = mqSelected;
                        brokersSent[times] = mq.getBrokerName();
                        try {
                            beginTimestampPrev = System.currentTimeMillis();
                            // 调用 MQClientAPIImpl 进行真正的消息发送
                            sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout);
                            endTimestamp = System.currentTimeMillis();
                            // endTimestamp - startTimestamp 表示此次发送消息的延迟时间
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                            // 如果是异步或者ONEWAY调用的，直接返回 null 结果
                            switch (communicationMode) {
                                case ASYNC:
                                    return null;
                                case ONEWAY:
                                    return null;
                                case SYNC:
                                    // 如果发送没有成功
                                    if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                        // 如果 Broker 端存储消息失败的话
                                        if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
                                            continue;
                                        }
                                    }
                                    
                                    return sendResult;
                                default:
                                    break;
                            }
                         
                        // 消息发送的过程中出现异常的话，更新 faultItemTable，也就是更新当前 Broker 的故障状态
                        } catch (RemotingException e) {
                            endTimestamp = System.currentTimeMillis();
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                            log.warn(msg.toString());
                            exception = e;
                            continue;
                        } catch (MQClientException e) {
                            endTimestamp = System.currentTimeMillis();
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                            log.warn(msg.toString());
                            exception = e;
                            continue;
                        } catch (MQBrokerException e) {
                            endTimestamp = System.currentTimeMillis();
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                            log.warn(msg.toString());
                            exception = e;
                            switch (e.getResponseCode()) {
                            case ResponseCode.TOPIC_NOT_EXIST:
                            case ResponseCode.SERVICE_NOT_AVAILABLE:
                            case ResponseCode.SYSTEM_ERROR:
                            case ResponseCode.NO_PERMISSION:
                            case ResponseCode.NO_BUYER_ID:
                            case ResponseCode.NOT_IN_CURRENT_UNIT:
                                continue;
                            default:
                                if (sendResult != null) {
                                    return sendResult;
                                }

                                throw e;
                            }
                        } catch (InterruptedException e) {
                            endTimestamp = System.currentTimeMillis();
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                            log.warn(msg.toString());
                            log.warn("sendKernelImpl exception", e);
                            log.warn(msg.toString());
                            throw e;
                        }
                    } else {
                        break;
                    }
                }

                // 如果发送成功，那么就直接返回 sendResult
                if (sendResult != null) {
                    return sendResult;
                }

                String info = String.format("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: %s",
                        times, System.currentTimeMillis() - beginTimestampFirst, msg.getTopic(),
                        Arrays.toString(brokersSent));

                info += FAQUrl.suggestTodo(FAQUrl.SEND_MSG_FAILED);

                MQClientException mqClientException = new MQClientException(info, exception);
                if (exception instanceof MQBrokerException) {
                    mqClientException.setResponseCode(((MQBrokerException) exception).getResponseCode());
                } else if (exception instanceof RemotingConnectException) {
                    mqClientException.setResponseCode(ClientErrorCode.CONNECT_BROKER_EXCEPTION);
                } else if (exception instanceof RemotingTimeoutException) {
                    mqClientException.setResponseCode(ClientErrorCode.ACCESS_BROKER_TIMEOUT);
                } else if (exception instanceof MQClientException) {
                    mqClientException.setResponseCode(ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION);
                }

                throw mqClientException;
            }

            List<String> nsList = this.getmQClientFactory().getMQClientAPIImpl().getNameServerAddressList();
            if (null == nsList || nsList.isEmpty()) {
                throw new MQClientException();
            }

            throw new MQClientException().setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION);
        }

        private SendResult sendKernelImpl(final Message msg, final MessageQueue mq,
                final CommunicationMode communicationMode, final SendCallback sendCallback,
                final TopicPublishInfo topicPublishInfo, final long timeout)
                throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
            
            // 根据 MessageQueue 获取到 Broker 的网络地址，如果 MQClientInstance 的 brokerAddrTable 未缓存
            // 该 Broker 的信息，则从 NameServer 上主动更新一下 topic 的路由信息，如果路由更新后还是找不到
            // Broker 的信息，则抛出 MQClientException 异常，提示 Broker 不存在
            String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
            if (null == brokerAddr) {
                tryToFindTopicPublishInfo(mq.getTopic());
                brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
            }

            SendMessageContext context = null;
            if (brokerAddr != null) {
                brokerAddr = MixAll.brokerVIPChannel(this.defaultMQProducer.isSendMessageWithVIPChannel(), brokerAddr);

                byte[] prevBody = msg.getBody();
                try {
                    // for MessageBatch,ID has been set in the generating process
                    if (!(msg instanceof MessageBatch)) {
                        // 为消息分配全局唯一的 id
                        MessageClientIDSetter.setUniqID(msg);
                    }

                    int sysFlag = 0;
                    // 如果消息体默认超过 4K(compressMsgBodyOverHowmuch)，会对消息体采用 zip 压缩，
                    // 并设置消息的系统标记为 MessageSysFlag.COMPRESSED_FLAG
                    if (this.tryToCompressMessage(msg)) {
                        sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
                    }

                    // 如果是事务消息 prepared 消息，则设置消息的系统标记为 MessageSysFlag.TRANSACTION_PREPARED_TYPE
                    final String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    if (tranMsg != null && Boolean.parseBoolean(tranMsg)) {
                        sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
                    }

                    // ignore code

                    // 构建消息发送请求包
                    SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                    requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                    requestHeader.setTopic(msg.getTopic());
                    requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
                    requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums());
                    requestHeader.setQueueId(mq.getQueueId());
                    // 对于消息标记 MessageSysFlag，RocketMQ 不对消息中的 flag 进行任何处理，留给应用程序使用
                    requestHeader.setSysFlag(sysFlag);
                    requestHeader.setBornTimestamp(System.currentTimeMillis());
                    requestHeader.setFlag(msg.getFlag());
                    requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
                    requestHeader.setReconsumeTimes(0);
                    requestHeader.setUnitMode(this.isUnitMode());
                    requestHeader.setBatch(msg instanceof MessageBatch);

                    if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        String reconsumeTimes = MessageAccessor.getReconsumeTime(msg);
                        if (reconsumeTimes != null) {
                            requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME);
                        }

                        String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
                        if (maxReconsumeTimes != null) {
                            requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
                        }
                    }

                    SendResult sendResult = null;
                    switch (communicationMode) {
                        case ASYNC:
                            sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(brokerAddr,
                                mq.getBrokerName(), msg, requestHeader, timeout, communicationMode, sendCallback,
                                topicPublishInfo, this.mQClientFactory,
                                this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed(), context, this);
                            break;
                        case ONEWAY:
                        case SYNC:
                            sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(brokerAddr,
                                    mq.getBrokerName(), msg, requestHeader, timeout, communicationMode, context, this);
                            break;
                        default:
                            assert false;
                            break;
                    }

                    // ignore code

                    return sendResult;
                } catch (RemotingException e) {
                    // ignore code
                } finally {
                    msg.setBody(prevBody);
                }
            }

            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }

        // 选择一个消息队列
        public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
            return this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName);
        }

        /**
         * tryToFindTopicPublishInfo 是查找主题的路由信息的方法。
         * 
         * 第一次发送消息时，本地没有缓存 topic 的路由信息，查询 NameServer 尝试获取，如果路由信息未找到，再次尝试用默认主题 
         * DefaultMQProducerImpl#createTopicKey ，也就是 "TBW102" 去查询，这个时候如果 BrokerConfig#autoCreateTopicEnable 为 true 时，NameServer 将返回路由信息，
         * 如果 autoCreateTopicEnab 为 false 时，将抛出无法找到 topic 路由异常
         */
        private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
            TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
            // 如果没有缓存或没有包含消息队列
            if (null == topicPublishInfo || !topicPublishInfo.ok()) {
                this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
                // 向 NameServer 查询该 topic 路由信息
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
                topicPublishInfo = this.topicPublishInfoTable.get(topic);
            }
    
            // 如果生产者中缓存了 topic 的路由信息，如果该路由信息中包含了消息队列，则直接返回该路由信息。
            if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
                return topicPublishInfo;
            } else {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, this.defaultMQProducer);
                topicPublishInfo = this.topicPublishInfoTable.get(topic);
                return topicPublishInfo;
            }
        }

        public void start() throws MQClientException {
            this.start(true);
        }

        public void start(final boolean startFactory) throws MQClientException {
            switch (this.serviceState) {
                case CREATE_JUST:
                    // 如果下面的流程中出错了，那么 ServiceState 就为 START_FAILED
                    this.serviceState = ServiceState.START_FAILED;
                    // 检查 productGroup 是否符合要求
                    this.checkConfig();
                    // 如果 producer 的 instanceName 为 DEFAULT，那么就将其转变为进程的 pid，这么做是因为 MQClientInstance
                    // 保存在 MQClientManager#factoryTable 属性中的时候，键是 ip + @ + instanceName。这样如果一台机器上面
                    // 部署了多个程序，也就是多个进程，那么这多个进程都会共用一个 MQClientInstance，会造成一些错误
                    if (!this.defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                        this.defaultMQProducer.changeInstanceNameToPID();
                    }
    
                    // 创建 MQClientInstance 实例。一个进程中只存在一个 MQClientManager 实例，维护一个 MQClientInstance 缓存表 
                    // ConcurrentMap<String /*ClientId*/，MQClientinstance> factoryTable， 也就是同一个 clientId 只会创建同一个 MQClientInstance
                    this.mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQProducer, rpcHook);
                    
                    // 向 MQClientlnstance 注册，将当前生产者加入到 MQClientlnstance 管理中，方便后续调用网络请求、进行心跳检测等
                    boolean registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
                    if (!registerOK) {
                        this.serviceState = ServiceState.CREATE_JUST;
                        throw new MQClientException();
                    }
    
                    this.topicPublishInfoTable.put(this.defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());
    
                    // 启动 MQClientInstance
                    if (startFactory) {
                        mQClientFactory.start();
                    }
    
                    log.info("the producer [{}] start OK. sendMessageWithVIPChannel={}", this.defaultMQProducer.getProducerGroup(), this.defaultMQProducer.isSendMessageWithVIPChannel());
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case RUNNING:
                case START_FAILED:
                case SHUTDOWN_ALREADY:
                    throw new MQClientException("The producer service state not OK, maybe started once, "
                        + this.serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
                default:
                    break;
            }
            // 向所有的 Broker 发送心跳包
            this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
        }

    }

    public class MQClientAPIImpl {

        // rocketmq 客户端进行消息发送的入口是 MQClientAPIImpl#sendMessage。请求命令是 RequestCode.SEND_MESSAGE
        public SendResult sendMessage(final String addr, final String brokerName, final Message msg,
                final SendMessageRequestHeader requestHeader, final long timeoutMillis,
                final CommunicationMode communicationMode, final SendCallback sendCallback,
                final TopicPublishInfo topicPublishInfo, final MQClientInstance instance,
                final int retryTimesWhenSendFailed, final SendMessageContext context,
                final DefaultMQProducerImpl producer)
                throws RemotingException, MQBrokerException, InterruptedException {

            RemotingCommand request = null;
            if (sendSmartMsg || msg instanceof MessageBatch) {
                SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
                request = RemotingCommand.createRequestCommand(
                        msg instanceof MessageBatch ? RequestCode.SEND_BATCH_MESSAGE : RequestCode.SEND_MESSAGE_V2,
                        requestHeaderV2);
            } else {
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
            }

            request.setBody(msg.getBody());

            switch (communicationMode) {

            // 单向发送是指消息生产者调用消息发送的 API ，无须等待消息服务器返回本次消息发送结果，并且无须提供回调函数，
            // 表示消息发送压根就不关心本次消息发送是否成功，其实现原理与异步消息发送相同，只是消息发送客户端在收到响应结果后什么都不做而已，
            // 并且没有重试机制
            case ONEWAY:
                this.remotingClient.invokeOneway(addr, request, timeoutMillis);
                return null;

            // 消息异步发送是指消息生产者调用发送的 API 后，无须阻塞等待消息服务器返回本次消息发送结果，只需要提供一个回调函数，
            // 供消息发送客户端在收到响应结果回调。异步方式相比同步方式，消息发送端的发送性能会显著提高，但为了保护消息服务器的负载压力，
            // RocketMQ 对消息发送的异步消息进行了井发控制，通过参数 clientAsyncSemaphoreValue 来控制，默认为 65535
            case ASYNC:
                final AtomicInteger times = new AtomicInteger();
                this.sendMessageAsync(addr, brokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo,
                        instance, retryTimesWhenSendFailed, times, context, producer);
                return null;
                
            case SYNC:
                return this.sendMessageSync(addr, brokerName, msg, timeoutMillis, request);
            default:
                assert false;
                break;
            }

            return null;
        }

    }

    public class MQClientInstance {

        public boolean updateTopicRouteInfoFromNameServer(final String topic) {
            return updateTopicRouteInfoFromNameServer(topic, false, null);
        }

        public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault, DefaultMQProducer defaultMQProducer) {
            try {
                if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    try {
                        TopicRouteData topicRouteData;
                        // 如果 isDefault 的值为 true，则使用默认主题 "TBW102" 去查询，如果查询到路由信息，
                        // 则替换路由信息中读写队列个数为消息生产者默认的队列个数
                        if (isDefault && defaultMQProducer != null) {
                            topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(), 1000 * 3);
                            if (topicRouteData != null) {
                                for (QueueData data : topicRouteData.getQueueDatas()) {
                                    int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                                    data.setReadQueueNums(queueNums);
                                    data.setWriteQueueNums(queueNums);
                                }
                            }
                        
                        // 如果 isDefault 为 false，则使用参数 topic 去查询
                        } else {
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

    }

    public class MQFaultStrategy {
        // 延迟故障容错，维护每个 Broker 的发送消息的延迟
        private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();
        // 发送消息延迟容错开关
        private boolean sendLatencyFaultEnable = false;
        // 延迟级别数组
        private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
        // 不可用时长数组
        private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

        /**
         * 选择消息队列
         * 
         * 根据路由信息选择消息队列，返回的消息队列按照 broker 、序号排序。比如 topic-A 在 Broker-a，Broker-b 上分别创建了 4 个消息队列，
         * 那么返回的消息队列为：['Broker-Name':'Broker-a','QueueId':'0'], ['Broker-Name':'Broker-a','QueueId':'1'], ['Broker-Name':'Broker-a','QueueId':'2']
         * ['Broker-Name':'Broker-a','QueueId':'3'], ['Broker-Name':'Broker-b','QueueId':'0'], ['Broker-Name':'Broker-b','QueueId':'1'],
         * ['Broker-Name':'Broker-b','QueueId':'2'], ['Broker-Name':'Broker-a','QueueId':'3']，rocketmq 具体选择消息队列的策略如下：
         *          
         * 首先消息发送端采用重试机制，由 retryTimesWhenSendFailed 指定【同步】方式重试次数。异步重试机制在收到消息发送结果后执行回调之前进行重试。
         * 由 retryTimesWhenSendAsyncFailed 指定【异步】方式重试次数，接下来就是循环执行，选择消息队列 、发送消息，发送成功则返回，收到异常则重试。选择消息队列有两种方式：
         * 
         * 1) sendLatencyFaultEnable=false，默认不启用 Broker 故障延迟机制
         * 2) sendLatencyFaultEnable=true，启用 Broker 故障延迟机制
         */
        public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
            // 启动 Broker 的故障延迟机制
            if (this.sendLatencyFaultEnable) {
                try {
                    int index = tpInfo.getSendWhichQueue().getAndIncrement();
                    // 根据对消息队列的轮询，获取到一个消息队列
                    for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                        int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                        if (pos < 0)
                            pos = 0;
                        MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                        // 通过 isAvailable 方法验证消息队列是否可用
                        if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                            if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                                return mq;
                        }
                    }
    
                    // 选择一个相对较好的 Broker，并且获得其对应的一个消息队列，不考虑该队列的可用性
                    final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                    int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                    if (writeQueueNums > 0) {
                        final MessageQueue mq = tpInfo.selectOneMessageQueue();
                        if (notBestBroker != null) {
                            mq.setBrokerName(notBestBroker);
                            mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                        }
                        return mq;
                    } else {
                        latencyFaultTolerance.remove(notBestBroker);
                    }
                } catch (Exception e) {
                    log.error("Error occurred when selecting message queue", e);
                }
                // 选择一个消息队列，不考虑队列的可用性
                return tpInfo.selectOneMessageQueue();
            }
    
            // 不启用 Broker 的故障延迟机制，选择一个消息队列，不考虑队列的可用性，只要消息队列的名称不等于 lastBrokerName
            return tpInfo.selectOneMessageQueue(lastBrokerName);
        }

        /**
         * @param brokerName mq 所属的 Broker 的名称
         * @param currentLatency 本次消息发送的时间延迟
         * @param isolation 是否隔离，该参数的含义如果为 true ，则使用默认时长 30s 来计算 Broker 故障规避时长 ，
         * 如果为 false 则使用本次消息发送延迟时间来计算 Broker 的故障规避时长
         */
        public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
            if (this.sendLatencyFaultEnable) {
                long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
                this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
            }
        }
    
        // computeNotAvailableDuration 的作用是计算因本次消息发送故障需要将 Broker 规避的时长，也就是接下来多长时间内
        // 该 Broker 不参与消息发送队列的负载
        // 
        // latencyMax，根据 currentLatency 本次消息的发送延迟，从 latencyMax 尾部向前找到第一个比 currentLatency 
        // 小的索引 index，如果没有找到，返回 0，然后根据这个索引从 notAvailableDuration 数组中取出对应的时间，
        // 在这个时长内 Broker 将设为不可用
        private long computeNotAvailableDuration(final long currentLatency) {
            for (int i = latencyMax.length - 1; i >= 0; i--) {
                if (currentLatency >= latencyMax[i])
                    return this.notAvailableDuration[i];
            }
    
            return 0;
        }


    }

    public class TopicPublishInfo {
        // 是否是顺序消息
        private boolean orderTopic = false;
        
        private boolean haveTopicRouterInfo = false;
        // 该主题的消息队列的集合
        private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();

        // sendWhichQueue是一个利用ThreadLocal本地线程存储自增值的一个类，自增值第一次使用Random类随机取值，
        // 此后如果消息发送出发重试机制，那么每次自增取值。
        // 每选择一次消息队列，sendWhichQueue 的值就会增加一，如果增加到 Integer.MAX_VALUE，就会重置为 0，这个值用于选择消息队列
        private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();

        private TopicRouteData topicRouteData;

        // 选择一个消息队列。在一次消息发送的过程中，可能会多次执行选择消息队列这个方法。
        // lastBrokerName 就是上一次选择的执行发送消息失败的 Broker。
        // 该算法在一次消息发送过程中能成功规避故障的 roker，但如果 Broker 宕机，由于路由算法中的消息队列是按 Broker 排序的，
        // 如果上一次根据路由算法选择的是宕机的 Broker 第一个队列 ，那么随后的下次选择的是宕机的 Broker 第二个队列，
        // 消息发送很有可能会失败，再次引发重试，带来不必要的性能损耗，
        public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
            // 第一次执行消息队列选择的时候，lastBrokerName 为 null，此时直接使用 sendWhichQueue 自增再获取值，
            // 然后进行取模，最后获得相应位置的 MessageQeue
            if (lastBrokerName == null) {
                return selectOneMessageQueue();
            
            // 如果消息发送失败再失败的话，下次再进行消息队列选择时规避上次 MesageQueue 所在的 Broker，
            // 否则还是有可能会再次失败
            } else {
                int index = this.sendWhichQueue.getAndIncrement();
                for (int i = 0; i < this.messageQueueList.size(); i++) {
                    int pos = Math.abs(index++) % this.messageQueueList.size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = this.messageQueueList.get(pos);
                    if (!mq.getBrokerName().equals(lastBrokerName)) {
                        return mq;
                    }
                }
                return selectOneMessageQueue();
            }
        }
    
        public MessageQueue selectOneMessageQueue() {
            int index = this.sendWhichQueue.getAndIncrement();
            int pos = Math.abs(index) % this.messageQueueList.size();
            if (pos < 0)
                pos = 0;
            return this.messageQueueList.get(pos);
        }

    }

    public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {

        private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<String, FaultItem>(16);

        @Override
        public boolean isAvailable(final String name) {
            final FaultItem faultItem = this.faultItemTable.get(name);
            if (faultItem != null) {
                return faultItem.isAvailable();
            }
            return true;
        }

        // 根据 broker 名称从缓存表中获取 FaultItem，如果找到则更新 FaultItem，否则创建 FaultItem
        @Override
        public void updateFaultItem(final String name, final long currentLatency, final long notAvailableDuration) {
            FaultItem old = this.faultItemTable.get(name);
            if (null == old) {
                final FaultItem faultItem = new FaultItem(name);
                faultItem.setCurrentLatency(currentLatency);
                // 设置的 startTime 为当前系统时间 + 需要进行故障规避的时长，也就是 Broker 可以重新启用的时间点
                faultItem.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);

                old = this.faultItemTable.putIfAbsent(name, faultItem);
                if (old != null) {
                    old.setCurrentLatency(currentLatency);
                    old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
                }
            } else {
                old.setCurrentLatency(currentLatency);
                old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
            }
        }

        @Override
        public String pickOneAtLeast() {
            final Enumeration<FaultItem> elements = this.faultItemTable.elements();
            List<FaultItem> tmpList = new LinkedList<FaultItem>();

            while (elements.hasMoreElements()) {
                final FaultItem faultItem = elements.nextElement();
                tmpList.add(faultItem);
            }

            if (!tmpList.isEmpty()) {
                // 首先打乱 tmpList 中的顺序
                Collections.shuffle(tmpList);
                // 对 tmpList 中的 faultItem 进行排序，最后得到的结果就是可用性越高的 faultItem（currentLatency、startTimestamp 都比较小）
                // 在 tmpList 中的排名越靠前
                Collections.sort(tmpList);

                final int half = tmpList.size() / 2;

                // 从 tmpList 的前半部分中选择一个 faultItem 返回，实际上是返回对应的 Broker 名称
                if (half <= 0) {
                    return tmpList.get(0).getName();
                } else {
                    final int i = this.whichItemWorst.getAndIncrement() % half;
                    return tmpList.get(i).getName();
                }
            }

            return null;
        }

    }

    class FaultItem implements Comparable<FaultItem> {

        private final String name;
        // 本次消息发送的延迟，其实就是从发送消息，到消息发送完毕所花费的时间
        private volatile long currentLatency;
        //  startTimestamp 表示此 Broker 可以启用的时间
        private volatile long startTimestamp;
        // 如果现在的时间戳 > Broker 可以启用的时间戳，就表明已经过了 Broker 故障延迟的时间，可以启用
        public boolean isAvailable() {
            return (System.currentTimeMillis() - startTimestamp) >= 0;
        }

    }

    public class TopicRouteData extends RemotingSerializable {

        private String orderTopicConf;
        // topic 分布的队列的元数据
        private List<QueueData> queueDatas;
        // topic 分布的 Broker 的元数据
        private List<BrokerData> brokerDatas;
        // broker 上的过滤服务器的列表
        private HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    }


    public class MQClientManager {
        
        private static MQClientManager instance = new MQClientManager();

        private ConcurrentMap<String/* clientId */, MQClientInstance> factoryTable = new ConcurrentHashMap<String, MQClientInstance>();
    
        public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig) {
            return getAndCreateMQClientInstance(clientConfig, null);
        }
    
        public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
            String clientId = clientConfig.buildMQClientId();
            MQClientInstance instance = this.factoryTable.get(clientId);
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
    
        public void removeClientFactory(final String clientId) {
            this.factoryTable.remove(clientId);
        }
    }

    public class ClientConfig {

        // 为 MQClientInstance 创建一个 clientId，也就是 IP + @ + instanceName
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
    

    }



}