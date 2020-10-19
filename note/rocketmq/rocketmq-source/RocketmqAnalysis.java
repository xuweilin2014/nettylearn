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










}