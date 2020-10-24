public class RocketmqAnalysis{

    /**
     * 消息消费者概述：
     * 消息服务器与消费者之间的消息传送也有两种方式：推模式（PushConsumer）、拉模式（PullConsumer）。所谓的拉模式，是消费端主动发起拉消息请求（也就是由用户手动调用消息拉取 API)；
     * 而推模式是 PushConsumer 在启动后，Consumer客户端会主动循环发送Pull请求到broker，如果没有消息，broker会把请求放入等待队列，新消息到达后返回response。
     * 所以本质上，两种方式都是通过客户端Pull来实现的。RocketMQ 消息推模式的实现基于拉模式，在拉模式上包装一层，一个拉取任务完成后开始下一个拉取任务。
     * 
     * 集群模式下，多个消费者如何对消息队列进行负载呢？消息队列负载机制遵循一个通用的思想 一个消息 列同一时间 只允许被一个消费者消费，一个消费者可 消费多个消息队列
     * 
     * RocketMQ 支持局部顺序消息消费，也就是保证同一个消息队列上的消息顺序消费。不支持消息的全局顺序消费，如果要实现某一主题的全局顺序消息消费，可以将该主题的队列数设置为 1，牺牲高可用性
     * 
     * RocketMQ 支持两种消息过滤模式:表达式（TAG 和 SQL92）与类过滤模式
     */

    /**
     * 1.同步刷盘与异步刷盘
     * 
     * 同步刷盘：同步刷盘就是消息一进来就马上将消息写入到磁盘里面，写完之后告诉消息发送者消息发送成功。好处就是数据安全性高
     * 异步刷盘：异步刷盘就是数据不是实时写到磁盘中，他会根据刷盘策略进行写入磁盘(数据可能丢失,性能高)
     * 
     * 2.异步复制与同步双写
     * 
     * 异步复制：主节点收到消息之后立马就返回给用户，然后再开一个线程与从节点进行复制，效率特别高，数据可能丢失
     * 同步双写：主节点收到消息之后不立马返回给用户，会等从节点复制成功之后再返回发送成功的消息给用户。数据安全性高，性能低一点
     * 
     * 3.定时消息
     * 
     * RocketMQ 支持定时消息，也就是生产者生产了一些消息到 Broker 中，然后消费者不会立即消费，要等到特定的时间点才会去消费。如果要支持任意精度的定时消息消费，必须在消息服
     * 务端对消息进行排序，势必带来很大的性能损耗，故 RocketMQ 不支持任意进度的定时消息，而只支持特定延迟级别。
     */

    /**
     * 根据使用者对读取操作的控制情况，消费者可分为两种类型：
     * 1.DefaultMQPushConsumer
     * 
     * DefaultMQPushConsumer 由系统控制读取操作，收到消息后自动调用传人的处理方法来处理。并且在加入其它 DefaultMQPushConsumer 之后，会自动做负载均衡。
     * DefaultMQPushConsumer 需要设置三个参数：一是 Consumer 所属的 GroupName，二是 NameServer 的地址和端口号，三是 Topic 的名称。其中 GroupName 用于
     * 把多个 Consumer 组织到一起，提高并发处理能力。GroupName 需要和消息模式一起使用。RocketMQ 支持两种消息模式（MessageModel）：
     * 
     * Clustering:同一个 ConsumerGroup 里的每个 Consumer 只消费所订阅消息一部分内容，同一个 ConsumerGroup 所有的 Consumer 的内容合来才是所订阅 Topic 从而达到负载均衡的目的
     * Broadcasting:同一个 ConsumerGroup 中的每个 Consumer 都可以消费到所订阅的 Topic 的全部消息，也就是一个消息会被多次分发
     * 
     * 2.DefaultMQPullConsumer: 读取操作中的大部分功能由使用者自主控制
     */

    /**
     * RocketMQ 并没有真正实现推模式，而是消费者主动向消息服务器拉取消息，RocketMQ 推模式是循环向消息服务端发送消息拉取请求，如果消息消费者向 RocketMQ 发送消息拉取时，消息并未到达消费队列，如果不启用长轮询机制，
     * 则会在服务端等待 shortPollingTimeMills 时间后（挂起）再去判断消息是否已到达消息队列，如果消息未到达则提示消息拉取客户端 PULL_NOT_FOUND （消息不存在），如果开启长轮询模式， RocketMQ 一方面会每 5s 轮询检查一次消息是否可达 
     * 同时，一有新消息到达后立马通知挂起线程再次验证新消息是否是自己感兴趣的消息，如果是则从 commitlog 文件提取消息返回给消息拉取客户端，否则直到挂起超时，超时时间由消息拉取方在消息拉取时封装在请求参数中，
     * PUSH 模式默认为 15s，PULL 模式通过 DefaultMQPullConsumer#setBrokerSuspendMaxTimeMillis 设置。RocketMQ 通过在 Broker 端配置 longPollingEnable 为 true 来开启长轮询模式
     */
    public interface MQPushConsumer extends MQConsumer {
        /**
         * Start the consumer
         */
        void start() throws MQClientException;
    
        /**
         * Shutdown the consumer
         */
        void shutdown();
    
        /**
         * Register the message listener
         */
        @Deprecated
        void registerMessageListener(MessageListener messageListener);
    
        void registerMessageListener(final MessageListenerConcurrently messageListener);
    
        void registerMessageListener(final MessageListenerOrderly messageListener);
    
        /**
         * Subscribe some topic
         *
         * @param fullClassName full class name,must extend org.apache.rocketmq.common.filter. MessageFilter
         * @param filterClassSource class source code,used UTF-8 file encoding,must be responsible for your code safety
         */
        void subscribe(final String topic, final String fullClassName, final String filterClassSource) throws MQClientException;
    
        /**
         * Subscribe some topic with selector. This interface also has the ability of subscribe(String, String),
         * and, support other message selection, such as {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}.
         * Choose Tag: {@link MessageSelector#byTag(java.lang.String)}
         * Choose SQL92: {@link MessageSelector#bySql(java.lang.String)}
         *
         * @param selector message selector({@link MessageSelector}), can be null.
         */
        void subscribe(final String topic, final MessageSelector selector) throws MQClientException;
    
        /**
         * Unsubscribe consumption some topic
         * @param topic message topic
         */
        void unsubscribe(final String topic);
    }

    public class DefaultMQPushConsumer extends ClientConfig implements MQPushConsumer {
        /**
         * Internal implementation. Most of the functions herein are delegated to it.
         */
        protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

        /**
         * Consumers of the same role is required to have exactly same subscriptions and consumerGroup to correctly achieve load balance. 
         * 需要注意的是，同一个 ConsumerGroup 中（也就是 GroupName 相同）的所有 Consumer 的订阅内容也必须全部相同。也就是 Topic + Tag 必须全部保持一致，
         * 否则会出现消息消费错乱的情况
         */
        private String consumerGroup;

        /**
         * ConsumerGroup 的消息模式，有两种：Clustering 和 Broadcasting，默认为 Clustering，也就是负载均衡
         */
        private MessageModel messageModel = MessageModel.CLUSTERING;

        /**
         * Consuming point on consumer booting.
         *
         * There are three consuming points:
         * 
         * CONSUME_FROM_LAST_OFFSET: consumer clients pick up where it stopped previously. If it were a newly booting up consumer client, according
         * aging of the consumer group
         * 
         * CONSUME_FROM_FIRST_OFFSET: Consumer client will start from earliest messages available.
         * 
         * CONSUME_FROM_TIMESTAMP: Consumer client will start from specified timestamp, which means messages born prior to consumeTimestamp will be ignored
         */
        private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

        private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30));

        /**
         * Queue allocation algorithm specifying how message queues are allocated to each consumer clients.
         * 消费者端消息列队的负载均衡策略，4.2 版本中内置了 5 种实现方式：
         * 平均分配策略(默认)(AllocateMessageQueueAveragely)
         * 环形分配策略(AllocateMessageQueueAveragelyByCircle)
         * 手动配置分配策略(AllocateMessageQueueByConfig)
         * 机房分配策略(AllocateMessageQueueByMachineRoom)
         * 一致性哈希分配策略(AllocateMessageQueueConsistentHash)
         * 
         * 默认使用第一种，平均分配策略
         */
        private AllocateMessageQueueStrategy allocateMessageQueueStrategy;

        /**
         * Max re-consume times. -1 means 16 times.
         *
         * If messages are re-consumed more than maxReconsumeTimes before success, it's be directed to a deletion queue waiting.
         */
        private int maxReconsumeTimes = -1;

        /**
         * Subscription relationship
         * topic -> subscribtion expression
         */
        private Map<String, String> subscription = new HashMap<String, String>();

        /**
         * Message listener
         */
        private MessageListener messageListener;

        /**
         * Offset Storage
         */
        private OffsetStore offsetStore;

        /**
         * Minimum consumer thread number
         */
        private int consumeThreadMin = 20;

        /**
         * Max consumer thread number
         */
        private int consumeThreadMax = 64;

        public DefaultMQPushConsumer(final String consumerGroup) {
            this(consumerGroup, null, new AllocateMessageQueueAveragely());
        }

        public DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook, AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
            this.consumerGroup = consumerGroup;
            this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
            this.defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
        }

        /**
         * This method gets internal infrastructure readily to serve. Instances must call this method after configuration.
         */
        @Override
        public void start() throws MQClientException {
            this.defaultMQPushConsumerImpl.start();
        }

        /**
         * Shutdown this client and releasing underlying resources.
         */
        @Override
        public void shutdown() {
            this.defaultMQPushConsumerImpl.shutdown();
        }

        /**
         * Register a callback to execute on message arrival for concurrent consuming.
         */
        @Override
        public void registerMessageListener(MessageListenerConcurrently messageListener) {
            this.messageListener = messageListener;
            this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
        }

        /**
         * Register a callback to execute on message arrival for orderly consuming.
         */
        @Override
        public void registerMessageListener(MessageListenerOrderly messageListener) {
            this.messageListener = messageListener;
            this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
        }

        /**
         * Subscribe a topic to consuming subscription.
         * @param topic         topic to subscribe.
         * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" if null or * expression,meaning subscribe all
         */
        @Override
        public void subscribe(String topic, String subExpression) throws MQClientException {
            this.defaultMQPushConsumerImpl.subscribe(topic, subExpression);
        }

        /**
         * Subscribe a topic to consuming subscription.
         * 基于主题的订阅消息，消息过滤方式使用类模式
         * @param topic             topic to consume.
         * @param fullClassName     full class name,must extend org.apache.rocketmq.common.filter. MessageFilter
         * @param filterClassSource class source code,used UTF-8 file encoding,must be responsible for your code safety
         */
        @Override
        public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
            this.defaultMQPushConsumerImpl.subscribe(topic, fullClassName, filterClassSource);
        }

        /**
         * Subscribe a topic by message selector.
         *
         * @param topic           topic to consume.
         * @param messageSelector {@link org.apache.rocketmq.client.consumer.MessageSelector}
         * @see org.apache.rocketmq.client.consumer.MessageSelector#bySql
         * @see org.apache.rocketmq.client.consumer.MessageSelector#byTag
         */
        @Override
        public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
            this.defaultMQPushConsumerImpl.subscribe(topic, messageSelector);
        }

        public Map<String, String> getSubscription() {
            return subscription;
        }
    }

    /**
     * Strategy Algorithm for message allocating between consumers
     * 消费者端的消息队列的负载均衡算法
     */
    public interface AllocateMessageQueueStrategy {

        /**
         * Allocating by consumer id
         *
         * @param consumerGroup current consumer group
         * @param currentCID    current consumer id
         * @param mqAll         message queue set in current topic
         * @param cidAll        consumer set in current consumer group
         * @return The allocate result of given strategy
         * 
         * 返回选择好的消息队列
         */
        List<MessageQueue> allocate(final String consumerGroup, final String currentCID, final List<MessageQueue> mqAll,
                final List<String> cidAll);

        /**
         * return The strategy name
         */
        String getName();
    }

    public static class MessageSelector {

        private String type;
    
        private String expression;
    
        private MessageSelector(String type, String expression) {
            this.type = type;
            this.expression = expression;
        }
    
        /**
         * Use SLQ92 to select message.
         * 如果 sql 字符串为 null 或者为空字符串，那么等同于选择所有的消息
         */
        public static MessageSelector bySql(String sql) {
            return new MessageSelector(ExpressionType.SQL92, sql);
        }
    
        /**
         * Use tag to select message.
         * 如果 tag 为 null 或者空字符串或者 *，那么等同于选择所有的消息
         */
        public static MessageSelector byTag(String tag) {
            return new MessageSelector(ExpressionType.TAG, tag);
        }
    }


    public interface OffsetStore {

        // 从消息进度存储文件加载消息进度到内存
        void load() throws MQClientException;
    
        // 更新内存中的消息消费进度
        void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);
    
        // 读取消息消费进度，READ_FROM_MEMORY 从内存中；READ_FROM_STORE 从磁盘上；MEMORY_FIRST_THEN_STORE 先从内存中读取，再从磁盘
        long readOffset(final MessageQueue mq, final ReadOffsetType type);
    
        // 持久化指定消息队列集合进度到磁盘
        void persistAll(final Set<MessageQueue> mqs);
    
        // 克隆该主题下所有的消息队列的消息消费进度
        void removeOffset(MessageQueue mq);
    
        // 克隆该主题下所有的消息队列的消息消费进度
        Map<MessageQueue, Long> cloneOffsetTable(String topic);
    
        // 更新存储在 Broker 端的消息消费进度，这种情况一般下是使用集群模式
        void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;
    }

    // 广播模式的消息消费进度存储在消费者本地
    public class LocalFileOffsetStore implements OffsetStore {

        // 消息进度存储目录
        public final static String LOCAL_OFFSET_STORE_DIR = System.getProperty( "rocketmq.client.localOffsetStoreDir", 
                                System.getProperty("user.home") + File.separator + ".rocketmq_offsets");

        // 消息客户端
        private final MQClientInstance mQClientFactory;

        // 消息消费组
        private final String groupName;

        // 消息消息进度存储文件（路径）
        // LOCAL_OFFSET_STORE_DIR/.rocketmq_offsets/{mQClientFactory.getClientId()}/groupName/offsets/json
        private final String storePath;

        // 消息消费进度（内存）
        private ConcurrentMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<MessageQueue, AtomicLong>();

        public LocalFileOffsetStore(MQClientInstance mQClientFactory, String groupName) {
            this.mQClientFactory = mQClientFactory;
            this.groupName = groupName;
            this.storePath = LOCAL_OFFSET_STORE_DIR + File.separator +
                    this.mQClientFactory.getClientId() + File.separator +
                    this.groupName + File.separator +
                    "offsets.json";
        }
    
        @Override
        public void load() throws MQClientException {
            // 从 storePath 中读取文件内容，并且将读取的字符串转换为 JSON 对象 OffsetSerializeWrapper
            // OffsetSerializeWrapper 内部就是 ConcurrentMap<MessageQueue, AtomicLong> offset 的简单封装
            OffsetSerializeWrapper offsetSerializeWrapper = this.readLocalOffset();

            if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
                // 将 offsetSerializeWrapper 中的 offsetTable 数据，也就是从文件中读取到的消息消费进度保存到 offsetTable 中
                offsetTable.putAll(offsetSerializeWrapper.getOffsetTable());
    
                for (MessageQueue mq : offsetSerializeWrapper.getOffsetTable().keySet()) {
                    AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                    log.info("load consumer's offset, {} {} {}", this.groupName, mq, offset.get());
                }
            }
        }


        // readLocalOffset 会首先尝试从 storePath 中尝试加载，如果从该文件读取到的内容为空，尝试从 storePath + ".bak"
        // 文件中去加载，如果还没有找到则返回 null
        private OffsetSerializeWrapper readLocalOffset() throws MQClientException {
            String content = null;

            try {
                content = MixAll.file2String(this.storePath);
            } catch (IOException e) {
                log.warn("Load local offset store file exception", e);
            }

            if (null == content || content.length() == 0) {
                return this.readLocalOffsetBak();
            } else {
                OffsetSerializeWrapper offsetSerializeWrapper = null;
                try {
                    offsetSerializeWrapper = OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
                } catch (Exception e) {
                    log.warn("readLocalOffset Exception, and try to correct", e);
                    return this.readLocalOffsetBak();
                }
    
                return offsetSerializeWrapper;
            }
        }

        // 持久化消息进度就是将 ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable 序列化到磁盘文件中
        // 持久化消息消费进度的时机是在 MQClientInstance#startScheduleTask 方法中启动一个定时任务，默认每隔 5s 持久化一次
        public void persistAll(Set<MessageQueue> mqs) {
            if (null == mqs || mqs.isEmpty())
                return;
    
            OffsetSerializeWrapper offsetSerializeWrapper = new OffsetSerializeWrapper();
            for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
                if (mqs.contains(entry.getKey())) {
                    AtomicLong offset = entry.getValue();
                    // 将 offsetTable 中的 offset 数据保存到 offsetSerializeWrapper 对象中
                    offsetSerializeWrapper.getOffsetTable().put(entry.getKey(), offset);
                }
            }
    
            // 将 offsetSerializeWrapper 对象转成一个 json 字符串
            String jsonString = offsetSerializeWrapper.toJson(true);
            if (jsonString != null) {
                try {
                    // 将 json 字符串保存到路径为 storePath 的文件
                    MixAll.string2File(jsonString, this.storePath);
                } catch (IOException e) {
                    log.error("persistAll consumer offset Exception, " + this.storePath, e);
                }
            }
        }

    }

    // 消息消费进度保存在远程 Broker 上或者内存中（也就是 offsetTable 中）
    // Broker 端默认每隔 10s 持久化一次消息的消费进度，存储文件名，${RocketMQ_HOME}/store/config/consumerOffset.json
    // 如果要持久化消息进度，则请求命令 UPDATE_CONSUMER_OFFSET 会更新 ConsumerOffsetManager 的 
    // Map<String /* topic@group */, Map<Integer /*消息队列 ID */,  Long /*消息消费进度*/>>，在集群模式中，以主题和消息组为键，保存该主题下所有队列的消息消费进度
    public class RemoteBrokerOffsetStore implements OffsetStore {

        private final MQClientInstance mQClientFactory;
        
        private final String groupName;
    
        private ConcurrentMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<MessageQueue, AtomicLong>();

        @Override
        public void load() {
        }

        // 读取某个消息消费队列的消费进度 offset
        // READ_FROM_MEMORY 表示只从内存中（offsetTable）获取到消息消费进度
        // READ_FROM_STORE 表示只从 Broker 端获取到消息的消费进度（对于 LocalFileOffsetStore，READ_FROM_STORE 表示只从磁盘文件中获取到消息消费进度）
        // MEMORY_FIRST_THEN_STORE 表示先从内存中，再从 Broker 端获取到消息的消费进度
        @Override
        public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
            if (mq != null) {
                switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return -1;
                    }
                }
                case READ_FROM_STORE: {
                    try {
                        // 向 Broker 发送请求，获取到消息的消费进度
                        long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
                        AtomicLong offset = new AtomicLong(brokerOffset);
                        // 更新 mq 对应的 offset
                        this.updateOffset(mq, offset.get(), false);
                        return brokerOffset;
                    }
                    // No offset in broker
                    catch (MQBrokerException e) {
                        return -1;
                    }
                    // Other exceptions
                    catch (Exception e) {
                        log.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                        return -2;
                    }
                }
                default:
                    break;
                }
            }

            return -1;
        }

        @Override
        public void persistAll(Set<MessageQueue> mqs) {
            if (null == mqs || mqs.isEmpty())
                return;

            final HashSet<MessageQueue> unusedMQ = new HashSet<MessageQueue>();
            if (!mqs.isEmpty()) {
                for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
                    MessageQueue mq = entry.getKey();
                    AtomicLong offset = entry.getValue();
                    if (offset != null) {
                        if (mqs.contains(mq)) {
                            try {
                                // 向 Broker 发送请求，更新 Broker 端保存的 MessageQueue 的消息消费进度
                                this.updateConsumeOffsetToBroker(mq, offset.get());
                            } catch (Exception e) {
                                log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
                            }
                        } else {
                            unusedMQ.add(mq);
                        }
                    }
                }
            }

            if (!unusedMQ.isEmpty()) {
                for (MessageQueue mq : unusedMQ) {
                    this.offsetTable.remove(mq);
                    log.info("remove unused mq, {}, {}", mq, this.groupName);
                }
            }
        }

        @Override
        public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws Exception {
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
            if (null == findBrokerResult) {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
                findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
            }

            if (findBrokerResult != null) {
                UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
                requestHeader.setTopic(mq.getTopic());
                requestHeader.setConsumerGroup(this.groupName);
                requestHeader.setQueueId(mq.getQueueId());
                requestHeader.setCommitOffset(offset);

                if (isOneway) {
                    this.mQClientFactory.getMQClientAPIImpl()
                            .updateConsumerOffsetOneway(findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
                } else {
                    this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffset(findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
                }
            } else {
                throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
            }
        }

        private long fetchConsumeOffsetFromBroker(MessageQueue mq) throws Exception{
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
            if (null == findBrokerResult) {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
                findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
            }

            if (findBrokerResult != null) {
                QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
                requestHeader.setTopic(mq.getTopic());
                requestHeader.setConsumerGroup(this.groupName);
                requestHeader.setQueueId(mq.getQueueId());

                return this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            } else {
                throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
            }
        }

    }










}