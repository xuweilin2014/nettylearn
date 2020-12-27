public class RocketmqProducer{

    /**
     * RocketMQ 发送普通消息有三种方式：可靠同步发送、可靠异步发送、单向发送。
     * 
     * 同步：发送者向 MQ 执行发送消息的 API 时，同步等待，直到消息服务器返回发送结果
     * 异步：发送者向 MQ 执行发送消息 API 时，指定消息发送成功后的回调函数，然后调用消息发送 API 后，立即返回，消息发送者线程不阻塞。
     * 直到运行结束，消息发送成功或者失败的回调任务在一个新的线程中执行。
     * 单向：消息发送者向 MQ 执行发送消息 API 时，直接返回，不等待消息服务器的结果，也不注册回调函数，简单地说，就是只管发送，
     * 不在乎消息是否成功地存储在消息服务器上
     * 
     * RocketMQ 消息发送需要考虑以下几个问题：
     * 1.消息队列如何进行负载
     * 2.消息发送如何实现高可用
     */

    /**
     * 消息生产者发送消息的流程：
     * 
     * 1.检查 producer 是否处于运行状态，也就是调用 DefaultMQProducerImpl#makeSureOK 方法，确保 serviceState 等于 RUNNING
     * 2.调用 DefaultMQProducerImpl#tryToFindTopicPublishInfo 方法获取到 topic 主题的路由信息
     * 3.如果前面获取到有效的路由信息之后，计算一下重试次数。对于同步模式来说，如果发送失败，还有两次重试机会，但是对于异步和 ONEWAY 方式，没有重试次数。
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
     * 线程安全的，即一个消息发送者可以在多线程环境中安全使用。每一个消息发送者全局会维护一个 Topic 上一次选择的队列，然后基于这个序号进行递增轮循，
     * 用 sendWhichQueue 表示。
     * 
     * topicA 在 broker-a、broker-b 上分别创建了 4 个队列，例如一个线程使用 Producer 发送消息时，通过对 sendWhichQueue getAndIncrement() 方法获取下一个队列。
     * 例如在发送之前 sendWhichQueue 该值为 broker-a 的 q1，如果由于此时 broker-a 的突发流量异常大导致消息发送失败，会触发重试，按照轮循机制，下一个选择的队列为 
     * broker-a 的 q2 队列，此次消息发送大概率还是会失败，即尽管会重试 2 次，但都是发送给同一个 Broker 处理，此过程会显得不那么靠谱，即大概率还是会失败，
     * 那这样重试的意义将大打折扣。
     * 
     * 故 RocketMQ 为了解决该问题，引入了故障规避机制，在消息重试的时候，会尽量规避上一次发送的 Broker，回到上述示例，当消息发往 broker-a q1 队列时返回发送失败，
     * 那重试的时候，会先排除 broker-a 中所有队列，即这次会选择 broker-b q1 队列，增大消息发送的成功率。
     * 
     * 但 RocketMQ 提供了两种规避策略，该参数由 sendLatencyFaultEnable 控制，用户可干预，表示是否开启延迟规避机制，默认为不开启。（DefaultMQProducer中设置这两个参数）
     * 
     * sendLatencyFaultEnable 设置为 false：默认值，不开启。
     * 
     * sendLatencyFaultEnable 设置为 true：开启延迟规避机制，一旦消息发送失败会将 broker-a "悲观"地认为在接下来的一段时间内该 Broker 不可用，
     * 在为未来某一段时间内所有的客户端不会向该 Broker 发送消息。这个延迟时间就是通过 notAvailableDuration、latencyMax 共同计算的，就首先先计算本次消息
     * 发送失败所耗的时延，然后对应 latencyMax 中哪个区间，即计算在 latencyMax 的下标，然后返回 notAvailableDuration 同一个下标对应的延迟值。
     * 
     * 另外，值得注意的是延迟规避策略只在重试时生效，例如在一次消息发送过程中如果遇到消息发送失败，规避 broekr-a，但是在下一次消息发送时，即再次调用 
     * DefaultMQProducer 的 send 方法发送消息时，还是会选择 broker-a 的消息进行发送，只要继续发送失败后，重试时再次规避 broker-a。
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
     * startTimestamp = System.currentTimeMillis() + notAvailableTime。
     * 
     * 因此，faultItem 中的 startTimestamp 就表明这个 Broker 下次可以启用的时间。同理，当消息发送给一个 Broker 的延迟比较低时，这时 rocketmq 就会认为
     * 这个 Broker 的可用性很高，一般将其不可用时间（notAvailableTime）设置为 0，所以其 faultItem 的 startTimestamp 就等于当前时间，表明立即可用。
     * 
     * 因此，rocketmq 的 Broker 的故障规避机制就是靠 LatencyFaultToleranceImpl#faultItemTable 实现的，其中每一个 faultItem 都代表了一个 Broker 的可用状态。
     * 然后依据这些 Broker 的可用状态来选择对应的消息队列 MessageQueue。
     */ 

    public class Message implements Serializable {

        private static final long serialVersionUID = 8445773977080406428L;
        // 消息所属的主题
        private String topic;
        // 此属性 rocketmq 不做处理
        private int flag;
        // 扩展属性
        // Message 的扩展属性主要包含以下几个方面：
        // 1.tag: 消息 TAG，用于消息过滤
        // 2.keys: Message 索引键，多个用空格隔开，rocketmq 可以根据这些 key 值来完成快速的检索，这些 key 会用来构建消息索引文件 IndexFile
        // 3.waitStoreMsgOK ：消息发送时是否等消息存储完成后再返回，默认为 true
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
        // 生产者所属的组，消息服务器在回查事务状态时会随机选择该组中的任何一个生产者发起事务回查请求
        private String producerGroup;
        // 默认 topicKey
        private String createTopicKey = MixAll.DEFAULT_TOPIC;
        // 默认主题在每一个 Broker 上的队列的数量
        private volatile int defaultTopicQueueNums = 4;
        // 发送消息的默认超时时间，默认为 3s
        private int sendMsgTimeout = 3000;
        // 消息体的大小超过该值时，启用压缩，默认为 4K
        private int compressMsgBodyOverHowmuch = 1024 * 4;
        // 同步方式发送消息重试次数，默认为 2，总共执行 3 次
        private int retryTimesWhenSendFailed = 2;
        // 异步方式发送消息重试次数，默认为 2
        private int retryTimesWhenSendAsyncFailed = 2;
        // 消息重试时选择另外一个 Broker 时，是否不等待存储结果就返回 默认为 false
        private boolean retryAnotherBrokerWhenNotStoreOK = false;
        // 允许发送的最大消息长度，默认为 4M ，该值最大值为 2^32-1
        private int maxMessageSize = 1024 * 1024 * 4; // 4M

        public DefaultMQProducer() {
            this(MixAll.DEFAULT_PRODUCER_GROUP, null);
        }

        public DefaultMQProducer(final String producerGroup, RPCHook rpcHook) {
            this.producerGroup = producerGroup;
            // DefaultMQProducer 中包装了 DefaultMQProducerImpl
            defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
        }

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

        // 异步发送消息，sendCallback 参数是消息发送成功后的回调方法，只要是异步发送消息，都会有 sendCallback 参数用来作为回调
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

        // 同步消息发送，指定消息队列选择算法，覆盖 Producer 默认的消息队列负载，并且指定发送的超时时间
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

        @Override
        public void start() throws MQClientException {
            this.defaultMQProducerImpl.start();
        }

    }

    public class DefaultMQProducerImpl implements MQProducerInner{

        private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<String, TopicPublishInfo>();

        public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer, RPCHook rpcHook) {
            this.defaultMQProducer = defaultMQProducer;
            this.rpcHook = rpcHook;
        }

        // 以同步状态发送消息，默认的超时时间为 3s
        // DefaultMQProducerImpl#send
        public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
            return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
        }

        /**
         * 消息发送的基本流程如下：
         * 
         * 1.验证消息
         * 2.查找路由
         * 3.消息发送（包含异常发送机制）
         */
        // MQClientInstance#sendDefaultImpl
        private SendResult sendDefaultImpl(Message msg, final CommunicationMode communicationMode, final SendCallback sendCallback, final long timeout) 
                                    throws MQClientException{
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
                                        // retryAnotherBrokerWhenNotStoreOK 属性表示如果发送消息到 Broker 端失败的话，是否重新发送消息到另外一个 Broker
                                        // 默认为 false
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
                            // 省略代码
                            continue;
                        } catch (MQClientException e) {
                            endTimestamp = System.currentTimeMillis();
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                            // 省略代码
                            continue;
                        } catch (MQBrokerException e) {
                            endTimestamp = System.currentTimeMillis();
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                            // 省略代码
                        } catch (InterruptedException e) {
                            endTimestamp = System.currentTimeMillis();
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                            // 省略代码
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

        /**
         * 
         * @param msg 待发送的消息
         * @param mq 消息将发送到该队列上
         * @param communicationMode 消息发送模式，SYNC、ASYNC、ONEWAY
         * @param sendCallback 异步消息回调函数
         * @param topicPublishInfo 主题路由信息
         * @param timeout 消息发送超时时间
         */
        // DefaultMQProducerImpl#sendKernelImpl 方法
        private SendResult sendKernelImpl(final Message msg, final MessageQueue mq, final CommunicationMode communicationMode, final SendCallback sendCallback,
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
                    // 生产者组
                    requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                    // topic 名称
                    requestHeader.setTopic(msg.getTopic());
                    // 默认创建的主题 key
                    requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
                    // 该主题在单个 Broker 默认队列个数
                    requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums());
                    // 队列 ID
                    requestHeader.setQueueId(mq.getQueueId());
                    // 对于消息标记 MessageSysFlag，RocketMQ 不对消息中的 flag 进行任何处理，留给应用程序使用
                    requestHeader.setSysFlag(sysFlag);
                    // 消息发送时间
                    requestHeader.setBornTimestamp(System.currentTimeMillis());
                    // 消息标记，rocketmq 对消息中的 flag 不做任何处理，仅仅供程序使用
                    requestHeader.setFlag(msg.getFlag());
                    // 消息的扩展属性
                    requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
                    // 消息的重试次数
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

                    // 根据消息发送方式，同步、异步、单向方式进行网络传输
                    switch (communicationMode) {
                        case ASYNC:
                            sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(brokerAddr,
                                mq.getBrokerName(), msg, requestHeader, timeout, communicationMode, sendCallback,
                                topicPublishInfo, this.mQClientFactory, this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed(), context, this);
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
         * DefaultMQProducerImpl#createTopicKey，也就是 "TBW102" 去查询，这个时候如果 BrokerConfig#autoCreateTopicEnable 为 true 时，
         * NameServer 将返回路由信息，如果 autoCreateTopicEnab 为 false 时，将抛出无法找到 topic 路由异常
         */
        // DefaultMQProducerImpl#tryToFindTopicPublishInfo
        private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
            TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
            // 如果没有缓存路由信息
            if (null == topicPublishInfo || !topicPublishInfo.ok()) {
                this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
                // 向 NameServer 查询该 topic 路由信息，这个 topic 就是用户自定义的 topic
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
                topicPublishInfo = this.topicPublishInfoTable.get(topic);
            }

            // 如果生产者中缓存了 topic 的路由信息，或者向 NameServer 发送请求查询到了路由信息的话，则直接返回该路由信息。
            if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
                return topicPublishInfo;
            // 向 NameServer 查询默认 topic 的路由信息，这个 topic 是默认的，也就是 TBW102，这里如果还没有找到的话，就直接抛出异常
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

                    // 如果 producer 的 instanceName 为 DEFAULT（也就是说如果用户没有设置自定义的 instanceName 的话），那么就将其转变为进程的 pid，
                    // 这么做是因为 MQClientInstance 保存在 MQClientManager#factoryTable 属性中的时候，键是 ip + @ + instanceName。
                    // 如果不这样做的话一台机器上面部署了多个程序，也就是多个进程，那么这多个进程都会共用一个 MQClientInstance，会造成一些错误。
                    // 另外，要注意的是，在同一个进程中的 Consumer 和 Producer 获取到的 MQClientInstance 是同一个对象，不过 MQClientInstance
                    // 封装了 rocketmq 网络处理的 API，是 Consumer 和 Producer 与 NameServer 和 Broker 打交道的网络通道
                    if (!this.defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                        this.defaultMQProducer.changeInstanceNameToPID();
                    }
    
                    // 创建 MQClientInstance 实例。整个 JVM 实例中只存在一个 MQClientManager 实例，维护一个 MQClientInstance 缓存表 
                    // ConcurrentMap<String /*ClientId*/，MQClientinstance> factoryTable，其中 clientId = ip + @ + instanceName
                    // instanceName 会被设置成为进程 pid
                    this.mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQProducer, rpcHook);
                    
                    // 向 MQClientlnstance 注册，将当前生产者加入到 MQClientlnstance 管理中，方便后续调用网络请求、进行心跳检测等
                    // 一台机器可能既需要发送消息，也需要消费消息，也就是说既是 Producer，也是 Consumer。这些 instance 都会注册到
                    // MQClientInstance 中方便后续管理，比如会收集这些 instance 中的 topic，一起向 NameServer 获取 topic 的路由信息
                    boolean registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
                    if (!registerOK) {
                        this.serviceState = ServiceState.CREATE_JUST;
                        throw new MQClientException();
                    }
    
                    // 首先将主题 TBW102 加入到 topicPublishInfoTable 中，这个是默认主题
                    // 后面再发送消息的时候，也会将消息的 topic 加入到 topicPublishInfoTable 中
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
                    throw new MQClientException("The producer service state not OK, maybe started once");
                default:
                    break;
            }
            // 向所有的 Broker 发送心跳包
            this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
        }

        // 在 MQClientInstance 中调用 updateTopicRouteInfoFromNameServer 更新 producer 中 topic 的路由信息时，
        // 最终就会调用到此方法，将新的 TopicPublishInfo 对象保存到 topicPublishInfoTable 中
        @Override
        public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
            if (info != null && topic != null) {
                TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
                if (prev != null) {
                    log.info("updateTopicPublishInfo prev is not null, " + prev.toString());
                }
            }
        }

        public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
            this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation);
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
        // MQFaultStrategy#selectOneMessageQueue
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
                        // 通过 isAvailable 方法验证消息队列 mq 是否可用，其实就是判断这个 mq 所属的 Broker 是否可用
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
         * @param isolation 是否隔离，该参数的含义如果为 true，则使用默认时长 30s 来计算 Broker 故障规避时长（也就是最大的规避时长），
         * 如果为 false 则使用本次消息发送延迟时间来计算 Broker 的故障规避时长
         */
        public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
            if (this.sendLatencyFaultEnable) {
                long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
                this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
            }
        }
    
        // computeNotAvailableDuration 的作用是计算因本次消息发送故障需要将 Broker 规避的时长，也就是接下来多长时间内
        // 该 Broker 不参与消息发送队列的选择
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

        // 选择一个消息队列。在一次消息发送的过程中，可能会多次执行选择消息队列这个方法。lastBrokerName 就是上一次选择的执行发送消息失败的 Broker。
        //
        // 如果没有开启故障规避的话，该算法在一次消息发送过程中进行消息重试的时候能成功规避故障的 Broker，但如果第一次 Producer 根据路由算法选择的是宕机
        // 的 Broker 第一个队列 ，那么第二次 Producer 发送消息时还是会选择是宕机的 Broker 上的队列，消息发送很有可能会失败，再次引发重试，带来不必要的性能损耗。
        //
        // 如果开启了故障规避的话，第一次发送消息与后面发送消息都会规避已经发生了故障的 Broker。
        public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
            // 第一次执行消息队列选择的时候，lastBrokerName 为 null，此时直接使用 sendWhichQueue 自增再获取值，
            // 然后进行取模，最后获得相应位置的 MessageQeue
            if (lastBrokerName == null) {
                return selectOneMessageQueue();
            
            // 如果消息发送失败的话，下次在消息重试的时候，进行消息队列选择会规避上次 MesageQueue 所在的 Broker，否则还是有可能会再次失败
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
        // LatencyFaultToleranceImpl#updateFaultItem
        @Override
        public void updateFaultItem(final String name, final long currentLatency, final long notAvailableDuration) {
            FaultItem old = this.faultItemTable.get(name);
            if (null == old) {
                final FaultItem faultItem = new FaultItem(name);
                // 如果 isolation 为 false 的话，currentLatency 为发送消息到 Broker 所消耗的时间
                // 如果 isolation 为 true 的话，currentLatency 为默认的 30s
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

                // 从 tmpList 的前半部分中选择一个 faultItem 返回，实际上是返回对应的 Broker 名称，这里说明返回的是一个相对来说可用性较高的 Broker
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
        // broker name，也就是这个 FaultItem 对应的 Broker 的名称
        private final String name;
        // 本次消息发送的延迟，其实就是从发送消息，到消息发送完毕所花费的时间
        private volatile long currentLatency;
        //  startTimestamp 表示此 Broker 可以启用的时间
        private volatile long startTimestamp;

        // 如果现在的时间戳 > Broker 可以启用的时间戳，就表明已经过了 Broker 故障延迟的时间，可以启用
        // FaultItem#isAvailable
        public boolean isAvailable() {
            return (System.currentTimeMillis() - startTimestamp) >= 0;
        }

        // FaultItem 是可以进行排序的，并且可用性越高（startTimestamp 越小，currentLatency 越小，可用性越高）
        @Override
        public int compareTo(final FaultItem other) {
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable())
                    return -1;

                if (other.isAvailable())
                    return 1;
            }

            if (this.currentLatency < other.currentLatency)
                return -1;
            else if (this.currentLatency > other.currentLatency) {
                return 1;
            }

            if (this.startTimestamp < other.startTimestamp)
                return -1;
            else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }

            return 0;
        }

    }


    public class MQClientManager {
        
        private static MQClientManager instance = new MQClientManager();

        private ConcurrentMap<String/* clientId */, MQClientInstance> factoryTable = new ConcurrentHashMap<String, MQClientInstance>();
    
        public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig) {
            return getAndCreateMQClientInstance(clientConfig, null);
        }
    
        public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
            // clientId = ip + @ + clientId
            String clientId = clientConfig.buildMQClientId();
            MQClientInstance instance = this.factoryTable.get(clientId);

            if (null == instance) {
                // 创建一个 MQClientInstance 
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
        // ClientConfig#buildMQClientId
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

    public abstract class AbstractSendMessageProcessor implements NettyRequestProcessor {

        protected RemotingCommand msgCheck(final ChannelHandlerContext ctx, final SendMessageRequestHeader requestHeader, final RemotingCommand response) {
            // 检查该 Broker 是否有写权限
            if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())
                    && this.brokerController.getTopicConfigManager().isOrderTopic(requestHeader.getTopic())) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] sending message is forbidden");
                return response;
            }

            // 检查该 topic 是否可以进行消息发送，也就是该 topic 与 rocketmq 中的默认主题 TBW102 是否重复
            if (!this.brokerController.getTopicConfigManager().isTopicCanSendMessage(requestHeader.getTopic())) {
                String errorMsg = "the topic[" + requestHeader.getTopic() + "] is conflict with system reserved words.";
                log.warn(errorMsg);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(errorMsg);
                return response;
            }

            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
            if (null == topicConfig) {
                int topicSysFlag = 0;
                if (requestHeader.isUnitMode()) {
                    if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                    } else {
                        topicSysFlag = TopicSysFlag.buildSysFlag(true, false);
                    }
                }

                log.warn("the topic {} not exist, producer: {}", requestHeader.getTopic(), ctx.channel().remoteAddress());
                topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageMethod(
                        requestHeader.getTopic(), requestHeader.getDefaultTopic(),
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getDefaultTopicQueueNums(),
                        topicSysFlag);

                if (null == topicConfig) {
                    if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                                requestHeader.getTopic(), 1, PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
                    }
                }

                if (null == topicConfig) {
                    response.setCode(ResponseCode.TOPIC_NOT_EXIST);
                    response.setRemark("topic[" + requestHeader.getTopic() + "] not exist, apply first please!" + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
                    return response;
                }
            }

            // 获取到消息应该发送到的 queueId
            int queueIdInt = requestHeader.getQueueId();
            int idValid = Math.max(topicConfig.getWriteQueueNums(), topicConfig.getReadQueueNums());
            // 检查队列 id，如果队列 id 不合法，就直接返回错误码
            if (queueIdInt >= idValid) {
                String errorInfo = String.format("request queueId[%d] is illegal, %s Producer: %s", queueIdInt, topicConfig.toString(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
                log.warn(errorInfo);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(errorInfo);
                return response;
            }
            return response;
        }

    }



}