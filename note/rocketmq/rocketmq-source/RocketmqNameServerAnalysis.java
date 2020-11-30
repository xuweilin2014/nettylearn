public class NameServerAnalysis{

    /**
     * NameServer 的主要作用是为消息生产者和消息消费者提供关于主题 Topic 的路由信息，那么 NameServer 需要存储路由的基础信息，还要能够管理 Broker 节点，包括路由
     * 注册、路由删除等功能
     * 
     * Broker 消息服务器在启动时向所有的 NameServer 注册，消息生产者（Producer）在发送消息之前先从 NameServer 获取 Broker 服务器地址列表，然后根据负载算法从列表中
     * 选择一台消息服务器进行消息发送。NameServer 与每台 Broker 服务器保持长连接，并且间隔 30s 检测 Broker 是否存活，如果检测到 Broker 宕机，
     * 则从路由表中将其移除。但是这个路由变化不会马上通知消息生产者，这么设计是为了降低 NameServer 实现的复杂性，在消息发送端提供了容错机制来保证
     * 消息发送的高可用性。
     * 
     * NameServer 本身的高可用可以通过部署多台 NameServer 服务器来实现，但是彼此之间互不通信，也就是 NameServer 服务器之间在某一时刻的数据并不会完全相同，但是这对于
     * 消息发送不会造成任何影响。Rocketmq NameServer 追求简单高效。
     */

    /**
     * RocketMQ 基于订阅发布机制，一个 Topic 拥有多个消息队列，一个 Broker 为每一主题 topic 默认创建 4 个读队列 4 个写队列。
     * 多个 Broker 组成一个集群，也就是由 BrokerName 相同的多台 Broker 组成 Master-Slave 架构。brokerId 为 0 代表 Master，大于 0 表示 Slave。
     * BrokerLivelnfo 中的 lastUpdateTimestamp 存储上次收到 Broker 心跳包的时间。
     */

    /**
     * RocketMQ 路由注册是通过 Broker 与 NameServer 的心跳功能实现的。Broker 启动时向集群中的所有的 NameServer 发送心跳包，
     * 然后每隔 30s 向集群中所有的 NameServer 发送心跳包，NameServer 收到 Broker 心跳包时会更新 brokerLiveTable 缓存中 BrokerLiveInfo
     * 的 lastUpdateTimestamp，然后 NameServer 每隔 10s 扫描 brokerLiveTable，如果连续 120s 没有收到心跳包， 
     * NameServer 将移除该 Broker 的路由信息 同时关闭掉 Socket 连接。
     */

    /**
     * 由于其它角色会主动向 NameServer 上报状态，所以 NameServer 的主要逻辑在 DefaultRequestProcessor 类中，根据上报
     * 消息里的请求码做相应处理，更新存储的对应信息。此外，连接断开的事件也会触发状态更新（具体的逻辑在 BrokerHousekeepingService 类
     * 中）
     * 
     * NamesrvController在创建时, 实例化了RouteInfoManager和BrokerHouseKeepingService两个对象. Name Server中最重要的就是RouteInfoManager类.
     * Name Server所有的Topic和Borker信息都保存在RouteInfoManager中, RouteInfoManager保存所有的路由信息. Netty服务端接收到请求后, 
     * 回调请求处理程序DefaultRequestProcessor, defaultRequestProcessor根据请求类型RequestCode, 例如注册Broker或者新建Topic请求, 
     * 来更新RouteInfoManager路由信息.
     */

    /**
     * Name Server 是专为 RocketMQ 设计的轻量级名称服务，具有简单、可集群横吐扩展、无状态，节点之间互不通信等特点。
     * 
     * 可以看到，Broker 集群、Producer 集群、Consumer 集群都需要与 NameServer 集群进行通信：
     * Broker 集群:
     * Broker 用于接收生产者发送消息，或者消费者消费消息的请求。一个 Broker 集群由多组 Master/Slave 组成，Master 可写可读，Slave 只可以读，
     * Master 将写入的数据同步给 Slave。每个 Broker 节点，在启动时，都会遍历 NameServer 列表，与每个 NameServer 建立长连接，注册自己的信息，之后定时上报。
     * 
     * Producer 集群:
     * 消息的生产者，通过 NameServer 集群获得 Topic 的路由信息，包括 Topic 下面有哪些 Queue，这些 Queue 分布在哪些 Broker 上等。
     * Producer 只会将消息发送到 Master 节点上，因此只需要与 Master 节点建立连接。
     * 
     * Consumer 集群:
     * 消息的消费者，通过 NameServer 集群获得 Topic 的路由信息，连接到对应的 Broker 上消费消息。注意，由于 Master 和 Slave 都可以读取消息，
     * 因此 Consumer 会与 Master 和 Slave 都建立连接。 
     */

    public class NamesrvConfig {

        // rocketmq 主目录，可以通过设置环境变量 【ROCKETMQ_HOME】 来配置 RocketMQ 主目录
        private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
        // NameServer 存储 KV 配置属性的持久化路径
        private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "kvConfig.json";
        // NameServer 默认的配置文件路径
        // NameServer 启动时如果要通过自定义的配置文件配置 NameServer 启动属性的话，请使用 -c 选项
        private String configStorePath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "namesrv.properties";
        private String productEnvName = "center";
        private boolean clusterTest = false;

        // 是否支持顺序消息，默认是不支持
        private boolean orderMessageEnable = false;

    }

    // NameServer 的网络参数
    public class NettyServerConfig implements Cloneable {
        // NameServer 监听端口，该值会被默认初始化为 9876
        private int listenPort = 8888;

        private int serverWorkerThreads = 8;
        private int serverCallbackExecutorThreads = 0;
        private int serverSelectorThreads = 3;

        // 消息请求井发度（Broker 端参数）
        private int serverOnewaySemaphoreValue = 256;

        // 异步消息发送最大并发度（Broker端参数）
        private int serverAsyncSemaphoreValue = 64;

        // 网络连接最大空闲时间，默认 120s，如果连接空闲时间超过该参数设置的值，连接将被关闭
        private int serverChannelMaxIdleTimeSeconds = 120;

        // 网络 socket 发送/接收缓存区大小， 默认 64k
        private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;
        private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;

        // ByteBuffer 是否开启缓存，建议开启
        private boolean serverPooledByteBufAllocatorEnable = true;

        // 是否启用 Epoll IO 模型， Linux 环境建议开启
        private boolean useEpollNativeSelector = false;

    }


    /**
     * 1.获取并解析配置参数，包括 NamesrvConfig 和 NettyServerConfig；
     * 2.调用 NamesrvController.initialize() 初始化 NamesrvController；
     * 3.若初始化失败，则直接关闭 NamesrvController；
     * 4.然后调用 NamesrvController.start() 方法来开启 NameServer 服务；
     * 5.注册 ShutdownHookThread 服务。在 JVM 退出之前，调用 NamesrvController.shutdown() 来进行关闭服务，释放资源；
     */
    public static class NamesrvStartup {

        public static NamesrvController main0(String[] args) {

            System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
            try {
                //PackageConflictDetect.detectFastjson();
    
                Options options = ServerUtil.buildCommandlineOptions(new Options());
                commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
                if (null == commandLine) {
                    System.exit(-1);
                    return null;
                }
    
                // 创建 NamesrvConfig，NameServer 业务参数
                final NamesrvConfig namesrvConfig = new NamesrvConfig();
                // 创建 NettyServerConfig，NameServer 网络参数
                final NettyServerConfig nettyServerConfig = new NettyServerConfig();
                nettyServerConfig.setListenPort(9876);

                // 在解析启动的时候，把指定的配置文件或者启动命令中的选项填充到 namesrvConfig、nettyServerConfig 对象中
                // 参数的来源有以下两种方式：
                // -c configFile: 通过 -c 命令指定配置文件的地址
                // 使用 "--属性名 属性值"，例如 --listenPort 9876
                if (commandLine.hasOption('c')) {
                    String file = commandLine.getOptionValue('c');
                    if (file != null) {
                        InputStream in = new BufferedInputStream(new FileInputStream(file));
                        properties = new Properties();
                        properties.load(in);
                        MixAll.properties2Object(properties, namesrvConfig);
                        MixAll.properties2Object(properties, nettyServerConfig);
    
                        namesrvConfig.setConfigStorePath(file);
    
                        System.out.printf("load config properties file OK, " + file + "%n");
                        in.close();
                    }
                }
    
                // 打印
                if (commandLine.hasOption('p')) {
                    MixAll.printObjectProperties(null, namesrvConfig);
                    MixAll.printObjectProperties(null, nettyServerConfig);
                    System.exit(0);
                }
    
                MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);
    
                // ignore code

                final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

                // remember all configs to prevent discard
                controller.getConfiguration().registerConfig(properties);

                boolean initResult = controller.initialize();
                if (!initResult) {
                    controller.shutdown();
                    System.exit(-3);
                }

                // 注册一个 JVM 钩子函数，增加一个优雅停机的线程
                // 从这里可以得知，如果在程序中使用了线程池，一种优雅停机的方式就是注册一个JVM钩子函数，在JVM进程关闭之前，先将线程池关闭，即使释放资源
                Runtime.getRuntime().ShutdownHook(new ShutdownHookThread(log, new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        // 关闭掉 controller
                        controller.shutdown();
                        return null;
                    }
                }));

                // 启动 NamesrvController，以便监听 Broker 和消息生产者的网络请求
                controller.start();

                String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
                log.info(tip);
                System.out.printf(tip + "%n");

                return controller;
            }catch (Throwable e) {
                e.printStackTrace();
                System.exit(-1);
            }
    
            return null;
        }

    }

    public class NamesrvController {

        // NameServer配置属性：包括rocketmqHome（RocketMQ home目录），kvConfigPath（KV配置文件路径），configStorePath（Store配置文件路径）等
        private final NamesrvConfig namesrvConfig;

        // netty 网络通信相关的配置数据
        private final NettyServerConfig nettyServerConfig;

        // NamesrvController 定时任务执行线程池，包含两个任务：打印配置，移除掉不处于激活状态的 Broker
        private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("NSScheduledThread"));
        
        // KV配置属性管理器，主要管理NameServer的配置
        private final KVConfigManager kvConfigManager;
        
        // NameServer数据的载体，记录Broker,Topic等信息
        private final RouteInfoManager routeInfoManager;

        private RemotingServer remotingServer;

        private BrokerHousekeepingService brokerHousekeepingService;

        private ExecutorService remotingExecutor;

        public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
            this.namesrvConfig = namesrvConfig;
            this.nettyServerConfig = nettyServerConfig;
            this.kvConfigManager = new KVConfigManager(this);
            this.routeInfoManager = new RouteInfoManager();
            this.brokerHousekeepingService = new BrokerHousekeepingService(this);
            this.configuration = new Configuration(log, this.namesrvConfig, this.nettyServerConfig);
            this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
        }

        public boolean initialize() {
            // 通过 KVConfigManager，从 /${user.home}/namesrv/kvConfig.json 中加载 NameServer 的配置信息，KVConfigManager 将配置信息存储在 configTable 中
            this.kvConfigManager.load();

            // 创建 NettyServer 网络处理对象，remotingServer 是 NameServer 用于对外提供连接服务的
            this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);
    
            this.remotingExecutor = Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));
    
            // 注册 NameServer 服务接受请求的处理类，默认采用 DefaultRequestProcessor，在 DefaultRequestProcessor 中，根据
            // 发送过来的消息的 RequestCode，来调用具体的方法对请求进行处理
            this.registerProcessor();
    
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    // 开启定时任务，NameServer 每隔 10s 扫描一次 Broker，移除掉处于不激活状态的 Broker
                    NamesrvController.this.routeInfoManager.scanNotActiveBroker();
                }
            }, 5, 10, TimeUnit.SECONDS);
    
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    // NameServer 每隔 10 分钟打印一次 KV 配置
                    NamesrvController.this.kvConfigManager.printAllPeriodically();
                }
            }, 1, 10, TimeUnit.MINUTES);
    
            return true;
        }

        private void registerProcessor() {
            if (namesrvConfig.isClusterTest()) {
                this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()),
                    this.remotingExecutor);
            } else {
                // 注册默认的处理类 DefaultRequestProcessor,所有的请求均由该处理类的 processRequest 方法来处理
                this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.remotingExecutor);
            }
        }

        public void start() throws Exception {
            // 使 NettyRemotingServer 监听特定端口，开始处理网络请求
            this.remotingServer.start();
        }

    }

    public class QueueData implements Comparable<QueueData> {
        private String brokerName;
        private int readQueueNums;
        private int writeQueueNums;
        private int perm;
        private int topicSynFlag;
    }

    public class BrokerData implements Comparable<BrokerData> {
        private String cluster;
        private String brokerName;
        private HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs;
    }

    class BrokerLiveInfo {
        // NameServer 上次收到心跳包的时间
        private long lastUpdateTimestamp;
        private DataVersion dataVersion;
        // NameServer 与 Broker 之间的 Channel 连接
        private Channel channel;
        private String haServerAddr;
    }

    public class RouteInfoManager {

        private final static long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;

        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        // topic 消息队列路由信息，消息发送时根据路由表进行负载均衡
        // 同一个消费组的消息消费者使用集群模式的话，会将订阅主题下的消息队列按照一定的策略分依次分发给消息消费者。使得每个消息队列在同一时刻只有一个
        // 消息消费者使用，一个消息消费者可以使用多个消息队列
        // topicQueueTable 记录了一个 topic 对应的消息队列信息，比如 topic 对应的消息队列位于哪些 Broker 上，
        // 以及在这些 Broker 上有多少个读队列和写队列。
        // topicQueueTable 的 value 是一个 QueueData 队列，队列的长度等于这个 topic 数据存储的 Master Broker 的个数，
        // QueueData 里面存储着 Broker 的名称，读写 queue 的数量，同步标识
        private final HashMap<String/* topic */, List<QueueData>> topicQueueTable;

        // broker 基础信息，包含 broker 所属集群名称，brokerName，以及主备 Broker 地址
        // 相同名称的 Broker 可能存在多台机器，一个 Master 和多个 Slave
        private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;

        // Broker 集群信息，存储集群中所有 Broker 名称
        private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;

        // Broker 状态信息，包括上次收到 Broker 发送过来心跳包的时间以及和 Broker 的连接 Channel, NameServer 每次收到心跳包时会替换该信息 lastUpdateTimestamp
        // key 是 brokerAddr，也就是对应着一台机器，brokerLiveTable 存储的内容是这台 Broker 机器的实时状态，
        // 包括上次更新状态的时间戳，NameServer 会定期检查这个时间戳，超时没有更新就认为这个 Broker 无效了，
        // 将其从 Broker 列表里清除。
        private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;

        // Broker 上的 FilterServer 列表，用于类模式消息过滤
        private final HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

        /**
         * NameServer 与 Broker 保持长连接，Broker 状态存储在 brokerLiveTable 中，NameServer 每收到一个心跳包，将更新 brokerLiveTable 中关于 Broker 
         * 的状态信息以及路由表（ topicQueueTable、brokerAddrTable、brokerLiveTabl、filterServerTable）。更新上述路由表（HashTable）使用了锁粒度较少的读写锁，
         * 允许多个消息发送者（producer）并发读，保证消息发送时的高并发，但同一时刻 NameServer 只处理一个 Broker 心跳包，多个心跳
         * 包请求串行执行 这也是读写锁经典使用场景
         */
        public RegisterBrokerResult registerBroker(final String clusterName, final String brokerAddr, final String brokerName, 
                final long brokerId, final String haServerAddr, final TopicConfigSerializeWrapper topicConfigWrapper, final List<String> filterServerList,
                final Channel channel) {

            RegisterBrokerResult result = new RegisterBrokerResult();

            try {
                try {
                    // 路由注册需要加写锁，防止并发修改 RouteInfoManager 中的路由表
                    this.lock.writeLock().lockInterruptibly();

                    /** 更新 clusterAddrTable 对象 */

                    // 首先判断 Broker 所属的集群是否存在，如果不存在，则创建，然后将 Broker 名称加入到集群 Broker 集合中
                    Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
                    if (null == brokerNames) {
                        brokerNames = new HashSet<String>();
                        this.clusterAddrTable.put(clusterName, brokerNames);
                    }
                    brokerNames.add(brokerName);

                    boolean registerFirst = false;
                    
                    /** 更新 brokerAddrTable 对象 */

                    // 维护 BrokerData 信息，首先从 brokerAddrTable 根据 BrokerName 尝试获取 Broker 信息，
                    // 如果不存在，新建 BrokerData 并放入到 brokerAddrTable，registerFirst 设置为 true；
                    // 如果存在，直接替换原先的 Broker 地址信息，registerFirst 设置为 false，表示非第一次注册；
                    BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                    if (null == brokerData) {
                        registerFirst = true;
                        brokerData = new BrokerData(clusterName, brokerName, new HashMap<Long, String>());
                        this.brokerAddrTable.put(brokerName, brokerData);
                    }
                    String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
                    registerFirst = registerFirst || (null == oldAddr);

                    /** 更新 topicQueueTable 对象 */

                    // 如果这个 Broker 是 Master，并且满足以下两个条件时，会创建当前 Broker 的 QueueData 信息：
                    // 1.这个心跳信息是该 Broker 的第一次心跳
                    // 2.Broker 的 topic 信息发生了变化，这时会用新的 QueueData 对象去取代旧的 QueueData 对象
                    if (null != topicConfigWrapper && MixAll.MASTER_ID == brokerId) {
                        if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.getDataVersion()) || registerFirst) {
                            ConcurrentMap<String, TopicConfig> tcTable = topicConfigWrapper.getTopicConfigTable();
                            if (tcTable != null) {
                                for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                                    // 在下面这个方法中会创建或者更新 topicQueueTable 对象
                                    this.createAndUpdateQueueData(brokerName, entry.getValue());
                                }
                            }
                        }
                    }

                    /** 更新 brokerLiveTable 对象 */

                    // 更新 BrokerLiveInfo，也就是存活的 Broker 信息表，BrokerLiveInfo 是执行路由删除的重要依据
                    BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr, new BrokerLiveInfo(
                            System.currentTimeMillis(), topicConfigWrapper.getDataVersion(), channel, haServerAddr));

                    if (null == prevBrokerLiveInfo) {
                        log.info("new broker registered, {} HAServer: {}", brokerAddr, haServerAddr);
                    }

                    // 注册 Broker 的 Filter Server 地址列表，一个 Broker 上会关联多个 FilterServer 消息过滤服务器，
                    if (filterServerList != null) {
                        if (filterServerList.isEmpty()) {
                            this.filterServerTable.remove(brokerAddr);
                        } else {
                            this.filterServerTable.put(brokerAddr, filterServerList);
                        }
                    }

                    // 如果此 Broker 为从节点，则获取到 Master Broker 的地址，然后通过 masterAddr 的地址获取到 HaServerAddress 的地址
                    // 并且将其设置到 result 中再返回，让 Broker 从节点可以知道到哪个地址去进行主从同步
                    if (MixAll.MASTER_ID != brokerId) {
                        String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                        if (masterAddr != null) {
                            BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(masterAddr);
                            if (brokerLiveInfo != null) {
                                // 设置 Master 的 HaServerAddress 到结果中
                                result.setHaServerAddr(brokerLiveInfo.getHaServerAddr());
                                result.setMasterAddr(masterAddr);
                            }
                        }
                    }
                } finally {
                    this.lock.writeLock().unlock();
                }
            } catch (Exception e) {
                log.error("registerBroker Exception", e);
            }

            return result;
        }

        private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {

            QueueData queueData = new QueueData();
            queueData.setBrokerName(brokerName);
            queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
            queueData.setReadQueueNums(topicConfig.getReadQueueNums());
            queueData.setPerm(topicConfig.getPerm());
            queueData.setTopicSynFlag(topicConfig.getTopicSysFlag());
    
            List<QueueData> queueDataList = this.topicQueueTable.get(topicConfig.getTopicName());
            if (null == queueDataList) {
                queueDataList = new LinkedList<QueueData>();
                queueDataList.add(queueData);
                this.topicQueueTable.put(topicConfig.getTopicName(), queueDataList);
                log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);
            } else {
                boolean addNewOne = true;
    
                Iterator<QueueData> it = queueDataList.iterator();
                while (it.hasNext()) {
                    QueueData qd = it.next();
                    if (qd.getBrokerName().equals(brokerName)) {
                        if (qd.equals(queueData)) {
                            addNewOne = false;
                        } else {
                            log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), qd, queueData);
                            it.remove();
                        }
                    }
                }
    
                if (addNewOne) {
                    queueDataList.add(queueData);
                }
            }
        }

        /**
         * RocktMQ 有两个触发点来触发路由删除：
         * 1) NameServer 定时扫描 brokerLiveTable 检测上次心跳包与当前系统时间的时间差，如果时间戳大于 120s ，则需要移除该 Broker 信息
         * 2) Broker 在正常被关闭的情况下（执行 BrokerController#shutdown 方法），会执行 unregisterBroker 指令。
         * 由于不管是何种方式触发的路由删除，路由删除的方法都是一样的，就是从 topicQueueTable、brokerAddrTable、brokerLiveTable、filterServerTable 
         * 删除与该 Broker 相关的信息，但 RocketMQ 这两种方式维护路由信息时会抽取公共代码，接下来将以第一种方式展开分析
         */
        // RouteInfoManager#scanNotActiveBroker
        public void scanNotActiveBroker() {
            Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, BrokerLiveInfo> next = it.next();
                long last = next.getValue().getLastUpdateTimestamp();
                // 判断现在距离上次收到 Broker 发送的心跳包的时间是否已经过去了 120s
                if ((last + BROKER_CHANNEL_EXPIRED_TIME) < System.currentTimeMillis()) {
                    // 如果超时了，则关闭掉和 Broker 的连接
                    RemotingUtil.closeChannel(next.getValue().getChannel());
                    it.remove();
                    log.warn("The broker channel expired, {} {}ms", next.getKey(), BROKER_CHANNEL_EXPIRED_TIME);
                    this.onChannelDestroy(next.getKey(), next.getValue().getChannel());
                }
            }
        }

        // 执行具体的删除逻辑，也就是从
        // brokerLiveTable、filterServerTable、clusterAddrTable、topicQueueTable、brokerAddrTable
        // 中移除掉已经超时的 Broker，维护上述 table 的一致性
        public void onChannelDestroy(String remoteAddr, Channel channel) {
            String brokerAddrFound = null;
            if (channel != null) {
                try {
                    try {
                        // 获取写锁
                        this.lock.readLock().lockInterruptibly();
                        Iterator<Entry<String, BrokerLiveInfo>> itBrokerLiveTable = this.brokerLiveTable.entrySet().iterator();
                        // 遍历 brokerLiveTable，获取到和此 channel 对应的 broker 地址
                        while (itBrokerLiveTable.hasNext()) {
                            Entry<String, BrokerLiveInfo> entry = itBrokerLiveTable.next();
                            if (entry.getValue().getChannel() == channel) {
                                brokerAddrFound = entry.getKey();
                                break;
                            }
                        }
                    } finally {
                        this.lock.readLock().unlock();
                    }
                } catch (Exception e) {
                    log.error("onChannelDestroy Exception", e);
                }
            }

            if (null == brokerAddrFound) {
                brokerAddrFound = remoteAddr;
            } else {
                log.info("the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
            }

            if (brokerAddrFound != null && brokerAddrFound.length() > 0) {
                try {
                    try {
                        this.lock.writeLock().lockInterruptibly();
                        // 从 brokerLiveTable 中删除此 broker 地址的 BrokerLiveInfo
                        this.brokerLiveTable.remove(brokerAddrFound);
                        // 从 filterServerTable 中删除此 broker 地址中的 FilterServer
                        this.filterServerTable.remove(brokerAddrFound);
                        String brokerNameFound = null;
                        boolean removeBrokerName = false;
                        Iterator<Entry<String, BrokerData>> itBrokerAddrTable = this.brokerAddrTable.entrySet().iterator();

                        // 维护 brokerAddrTable。遍历 HashMap<String /* brokerName */, BrokerData> brokerAddrTable，
                        // 从 BrokerData 的 HashMap<Long/* brokerId */， String /*broker address */> brokerAddr 中，找到具体的 Broker ，
                        // 从 BrokerData 中移除，如果移除后在 BrokerData 不再包含其 Broker，则 brokerAddrTable 中移除该
                        // brokerName 对应的 Broker 条目
                        while (itBrokerAddrTable.hasNext() && (null == brokerNameFound)) {
                            BrokerData brokerData = itBrokerAddrTable.next().getValue();

                            Iterator<Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();

                            while (it.hasNext()) {
                                Entry<Long, String> entry = it.next();
                                Long brokerId = entry.getKey();
                                String brokerAddr = entry.getValue();
                                if (brokerAddr.equals(brokerAddrFound)) {
                                    brokerNameFound = brokerData.getBrokerName();
                                    it.remove();
                                    log.info("remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed");
                                    break;
                                }
                            }

                            if (brokerData.getBrokerAddrs().isEmpty()) {
                                removeBrokerName = true;
                                itBrokerAddrTable.remove();
                                log.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed");
                            }
                        }

                        // 根据 BrokerName ，从 clusterAddrTable 中找到 Broker 并从集群中移除。如果移除后，集群中不包含任何
                        // Broker，则将该集群从 clusterAddrTable 中移除
                        if (brokerNameFound != null && removeBrokerName) {
                            Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                            while (it.hasNext()) {
                                Entry<String, Set<String>> entry = it.next();
                                String clusterName = entry.getKey();
                                Set<String> brokerNames = entry.getValue();
                                boolean removed = brokerNames.remove(brokerNameFound);
                                if (removed) {
                                    log.info("remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed");
                                    if (brokerNames.isEmpty()) {
                                        log.info("remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster");
                                        it.remove();
                                    }
                                    break;
                                }
                            }
                        }

                        // 根据 brokerName ，遍历所有主题的队列，如果队列中包含了当前 Broker 的队列，则移除，如果 topic 只包含待移除 Broker
                        // 的队列的话，从路由表中删除该 topic
                        if (removeBrokerName) {
                            Iterator<Entry<String, List<QueueData>>> itTopicQueueTable = this.topicQueueTable.entrySet().iterator();
                            while (itTopicQueueTable.hasNext()) {
                                Entry<String, List<QueueData>> entry = itTopicQueueTable.next();
                                String topic = entry.getKey();
                                List<QueueData> queueDataList = entry.getValue();

                                Iterator<QueueData> itQueueData = queueDataList.iterator();
                                while (itQueueData.hasNext()) {
                                    QueueData queueData = itQueueData.next();
                                    if (queueData.getBrokerName().equals(brokerNameFound)) {
                                        itQueueData.remove();
                                        log.info("remove topic[{} {}], from topicQueueTable, because channel destroyed");
                                    }
                                }

                                if (queueDataList.isEmpty()) {
                                    itTopicQueueTable.remove();
                                    log.info("remove topic[{}] all queue, from topicQueueTable, because channel destroyed");
                                }
                            }
                        }
                    } finally {
                        // 释放锁，完成了路由删除
                        this.lock.writeLock().unlock();
                    }
                } catch (Exception e) {
                    log.error("onChannelDestroy Exception", e);
                }
            }
        }

        public TopicRouteData pickupTopicRouteData(final String topic) {
            TopicRouteData topicRouteData = new TopicRouteData();
            boolean foundQueueData = false;
            boolean foundBrokerData = false;
            Set<String> brokerNameSet = new HashSet<String>();
            List<BrokerData> brokerDataList = new LinkedList<BrokerData>();
            topicRouteData.setBrokerDatas(brokerDataList);
    
            HashMap<String, List<String>> filterServerMap = new HashMap<String, List<String>>();
            topicRouteData.setFilterServerTable(filterServerMap);
    
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    List<QueueData> queueDataList = this.topicQueueTable.get(topic);
                    if (queueDataList != null) {
                        topicRouteData.setQueueDatas(queueDataList);
                        foundQueueData = true;
    
                        Iterator<QueueData> it = queueDataList.iterator();
                        while (it.hasNext()) {
                            QueueData qd = it.next();
                            brokerNameSet.add(qd.getBrokerName());
                        }
    
                        for (String brokerName : brokerNameSet) {
                            BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                            if (null != brokerData) {
                                BrokerData brokerDataClone = new BrokerData(brokerData.getCluster(), brokerData.getBrokerName(), (HashMap<Long, String>) brokerData
                                    .getBrokerAddrs().clone());
                                brokerDataList.add(brokerDataClone);
                                foundBrokerData = true;
                                for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                                    List<String> filterServerList = this.filterServerTable.get(brokerAddr);
                                    filterServerMap.put(brokerAddr, filterServerList);
                                }
                            }
                        }
                    }
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("pickupTopicRouteData Exception", e);
            }
    
            // 省略代码
    
            return null;
        }
    }

    public static class BrokerStartup {
        public static void main(String[] args) {
            start(createBrokerController(args));
        }

        public static BrokerController start(BrokerController controller) {
            try {
    
                controller.start();
    
                String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                    + controller.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
    
                if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                    tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
                }
    
                log.info(tip);
                return controller;
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(-1);
            }
    
            return null;
        }

        public static BrokerController createBrokerController(String[] args) {
            System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
    
            if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
                NettySystemConfig.socketSndbufSize = 131072;
            }
    
            if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
                NettySystemConfig.socketRcvbufSize = 131072;
            }
    
            try {
                //PackageConflictDetect.detectFastjson();
                Options options = ServerUtil.buildCommandlineOptions(new Options());
                commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options), new PosixParser());
                if (null == commandLine) {
                    System.exit(-1);
                }
    
                // 先创建 4 个配置文件对象：BrokerConfig、NettyServerConfig、NettyClientConfig、MessageStoreConfig，里面设置了默认值
                final BrokerConfig brokerConfig = new BrokerConfig();
                final NettyServerConfig nettyServerConfig = new NettyServerConfig();
                final NettyClientConfig nettyClientConfig = new NettyClientConfig();
    
                nettyClientConfig.setUseTLS(Boolean.parseBoolean(System.getProperty(TLS_ENABLE, String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING))));
                nettyServerConfig.setListenPort(10911);

                final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
    
                if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                    int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                    messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
                }
    
                // 如果用户使用了 -c 指定了配置文件的话，broker 同时会去加载我们配置好的配置文件，
                // 配置文件中配置的属性会覆盖对应的这4个配置对象中的配置。比如brokerName，brokerId等都需通过配置文件去配置覆盖
                if (commandLine.hasOption('c')) {
                    String file = commandLine.getOptionValue('c');
                    if (file != null) {
                        configFile = file;
                        InputStream in = new BufferedInputStream(new FileInputStream(file));
                        properties = new Properties();
                        properties.load(in);
    
                        properties2SystemEnv(properties);
                        // 分别使用配置文件中的属性覆盖掉 BrokerConfig、NettyServerConfig、NettyClientConfig、MessageStoreConfig 中默认的属性
                        MixAll.properties2Object(properties, brokerConfig);
                        MixAll.properties2Object(properties, nettyServerConfig);
                        MixAll.properties2Object(properties, nettyClientConfig);
                        MixAll.properties2Object(properties, messageStoreConfig);
    
                        BrokerPathConfigHelper.setBrokerConfigPath(file);
                        in.close();
                    }
                }
    
                MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);
    
                if (null == brokerConfig.getRocketmqHome()) {
                    System.out.printf("Please set the " + MixAll.ROCKETMQ_HOME_ENV
                        + " variable in your environment to match the location of the RocketMQ installation");
                    System.exit(-2);
                }
    
                String namesrvAddr = brokerConfig.getNamesrvAddr();
                if (null != namesrvAddr) {
                    try {
                        String[] addrArray = namesrvAddr.split(";");
                        for (String addr : addrArray) {
                            RemotingUtil.string2SocketAddress(addr);
                        }
                    } catch (Exception e) {
                        System.out.printf("The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n");
                        System.exit(-3);
                    }
                }
    
                switch (messageStoreConfig.getBrokerRole()) {
                    case ASYNC_MASTER:
                    case SYNC_MASTER:
                        brokerConfig.setBrokerId(MixAll.MASTER_ID);
                        break;
                    case SLAVE:
                        if (brokerConfig.getBrokerId() <= 0) {
                            System.out.printf("Slave's brokerId must be > 0");
                            System.exit(-3);
                        }
                        break;
                    default:
                        break;
                }
    
                messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
                LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
                JoranConfigurator configurator = new JoranConfigurator();
                configurator.setContext(lc);
                lc.reset();
                configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");
    
                // 如果有参数 -p 的话，会打印配置信息，省略代码
    
                // 将以下 4 个配置文件对象当做参数传入 BrokerController 的构造函数，
                // 这里真正创建了BrokerController，brokerController的构造方法中也初始化了非常多的broker所必备的组件
                // 例如 ConsumerOffsetManager（消费集群消费进度组件）、topicConfigManager、ConsumerFilterManager、ConsumerManager、
                // ProducerManager、PullRequestHoldService、slaveSynchronize 等等
                final BrokerController controller = new BrokerController(
                    brokerConfig,
                    nettyServerConfig,
                    nettyClientConfig,
                    messageStoreConfig);

                // remember all configs to prevent discard
                controller.getConfiguration().registerConfig(properties);
    
                boolean initResult = controller.initialize();
                if (!initResult) {
                    controller.shutdown();
                    System.exit(-3);
                }
    
                // 优雅停机，在 JVM 关闭之前，会通过调用 controller.shutdown 方法关闭掉 controller 
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    private volatile boolean hasShutdown = false;
                    private AtomicInteger shutdownTimes = new AtomicInteger(0);
                    @Override
                    public void run() {
                        synchronized (this) {
                            log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                            if (!this.hasShutdown) {
                                this.hasShutdown = true;
                                long beginTime = System.currentTimeMillis();
                                controller.shutdown();
                                long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                                log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                            }
                        }
                    }
                }, "ShutdownHook"));
    
                return controller;
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(-1);
            }
    
            return null;
        }
    }

    public class BrokerController{

        public void start() throws Exception {
            if (this.messageStore != null) {
                this.messageStore.start();
            }
    
            if (this.remotingServer != null) {
                this.remotingServer.start();
            }
    
            if (this.fastRemotingServer != null) {
                this.fastRemotingServer.start();
            }
    
            if (this.brokerOuterAPI != null) {
                this.brokerOuterAPI.start();
            }
    
            if (this.pullRequestHoldService != null) {
                this.pullRequestHoldService.start();
            }
    
            if (this.clientHousekeepingService != null) {
                this.clientHousekeepingService.start();
            }
    
            if (this.filterServerManager != null) {
                this.filterServerManager.start();
            }
    
            // 将此 Broker 的信息注册到所有的 NameServer 上
            this.registerBrokerAll(true, false);
    
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        // Broker 每隔 30s 向【所有的】 NameServer 发送心跳包
                        BrokerController.this.registerBrokerAll(true, false);
                    } catch (Throwable e) {
                        log.error("registerBrokerAll Exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 30, TimeUnit.MILLISECONDS);
    
            if (this.brokerStatsManager != null) {
                this.brokerStatsManager.start();
            }
    
            if (this.brokerFastFailure != null) {
                this.brokerFastFailure.start();
            }
        }

        public void shutdown() {
            // ignore code

            this.unregisterBrokerAll();

            // ignore code
        }

        private void unregisterBrokerAll() {
            this.brokerOuterAPI.unregisterBrokerAll(this.brokerConfig.getBrokerClusterName(), this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(), this.brokerConfig.getBrokerId());
        }

        public boolean initialize() throws CloneNotSupportedException {
            // 将存储在服务器上面的 topic 配置信息（topicConfigManager），消费者的消费进度（consumerOffsetManager），
            // 消费者的订阅信息（subscriptionGroupManager）等等加载进来。这些配置信息可为空， 因为本身这部分信息就是 broker 在运行的过程中自动持久化到服务器里的，
            // 我们这里去读取只是恢复 broker 在关闭之前的各配置的情况。如果都为空意味着是一个新创建的 broker。
            boolean result = this.topicConfigManager.load();
            result = result && this.consumerOffsetManager.load();
            result = result && this.subscriptionGroupManager.load();
            result = result && this.consumerFilterManager.load();

            if (result) {
                try {
                    // 创建 MessageStore 核心组件，MessageStore 组件是 Broker 消息存储的最核心的组件
                    // 在其构造方法中，主要创建了 CommitLog、allocateMappedFileService（用于创建新的 MappedFile 对象，也就是真正在磁盘上分配空间，创建新文件）
                    // indexService（创建 IndexFile 文件），haService（用于主从消息同步），reputMessageService（用于将 CommitLog 中的消息转发到 IndexFile 和
                    // ConsumeQueue 文件中），ScheduleMessageService（延迟消息服务线程）等等.
                    this.messageStore = new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener,
                            this.brokerConfig);
                    this.brokerStats = new BrokerStats((DefaultMessageStore) this.messageStore);
                    //load plugin
                    MessageStorePluginContext context = new MessageStorePluginContext(messageStoreConfig, brokerStatsManager, messageArrivingListener, brokerConfig);
                    this.messageStore = MessageStoreFactory.build(context, this.messageStore);
                    this.messageStore.getDispatcherList().addFirst(new CommitLogDispatcherCalcBitMap(this.brokerConfig, this.consumerFilterManager));
                } catch (IOException e) {
                    result = false;
                    log.error("Failed to initialize", e);
                }
            }

            // MessageStore 创建好之后就需要调用对应的 load 方法，加载和恢复 CommitLog，ConsumeQueue 等文件
            result = result && this.messageStore.load();

            // 
            if (result) {
                this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
                NettyServerConfig fastConfig = (NettyServerConfig) this.nettyServerConfig.clone();
                fastConfig.setListenPort(nettyServerConfig.getListenPort() - 2);
                this.fastRemotingServer = new NettyRemotingServer(fastConfig, this.clientHousekeepingService);

                // 创建各种线程池，省略代码

                // 将各种 Processor 注册到 remotingServer 和 fastRemotingServer 上
                this.registerProcessor();

                if (this.brokerConfig.getNamesrvAddr() != null) {
                    this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
                    log.info("Set user specified name server address: {}", this.brokerConfig.getNamesrvAddr());
                } else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
                    this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                            } catch (Throwable e) {
                                log.error("ScheduledTask fetchNameServerAddr exception", e);
                            }
                        }
                    }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
                }

                // 这个 Broker 是 SLAVE 的话
                if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                    /**
                     * 如果用户自己在配置文件中配置了 haMasterAddress 属性，并且合法的话
                     * 就将此 haMasterAddress 最终设置成为 HAClient 中的 masterAddress，并且不会从 NameServer 上获取 haMasterAddress 的地址。
                     * 在 HAClient 启动的时候，就直接连接此 masterAddress 的地址，然后每隔 5s，此 SLAVE Broker 就会发送一个心跳包给对应的 MASTER，
                     * 其中包含一个 slave offset，表示 SLAVE 已经接收到的消息的偏移量
                     */
                    if (this.messageStoreConfig.getHaMasterAddress() != null && this.messageStoreConfig.getHaMasterAddress().length() >= 6) {
                        this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                        // updateMasterHAServerAddrPeriodically 为 false，表示不会从 NameServer 上获得 haMasterAddress 的信息
                        this.updateMasterHAServerAddrPeriodically = false;
                    } else {
                        this.updateMasterHAServerAddrPeriodically = true;
                    }
    
                    this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                BrokerController.this.slaveSynchronize.syncAll();
                            } catch (Throwable e) {
                                log.error("ScheduledTask syncAll slave exception", e);
                            }
                        }
                    }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
                } else {
                    this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
    
                        @Override
                        public void run() {
                            try {
                                BrokerController.this.printMasterAndSlaveDiff();
                            } catch (Throwable e) {
                                log.error("schedule printMasterAndSlaveDiff error.", e);
                            }
                        }
                    }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
                }

                
            }
        }

        // 省略了 fastRemotingServer 注册 processor 的过程，不过也和 remotingServer 类似
        // 一共注册了 7 个 Processor，SendMessageProcessor、PullMessageProcessor、QueryMessageProcessor、ClientManageProcessor
        // ConsumerManageProcessor、EndTransactionProcessor、AdminBrokerProcessor。这些 Processor 注册到 remotingServer 中，
        // 并且一个 RequestCode 对应一个 Processor，所以当消息到达 remotingServer 时，会根据消息头中的 RequestCode，来选择具体
        // Processor 进行处理（一个 RequestCode 对应一个 Processor，同时一个 Processor 可以处理多个 RequestCode）
        public void registerProcessor() {
            /**
             * SendMessageProcessor
             */
            SendMessageProcessor sendProcessor = new SendMessageProcessor(this);
            sendProcessor.registerSendMessageHook(sendMessageHookList);
            sendProcessor.registerConsumeMessageHook(consumeMessageHookList);
            this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
            this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, this.sendMessageExecutor);
            this.remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendProcessor, this.sendMessageExecutor);
            this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, this.sendMessageExecutor);
            
            /**
             * PullMessageProcessor
             */
            this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);
            this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);
    
            /**
             * QueryMessageProcessor
             */
            NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
            this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
            this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);
    
            /**
             * ClientManageProcessor
             */
            ClientManageProcessor clientProcessor = new ClientManageProcessor(this);
            this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.clientManageExecutor);
            this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
            this.remotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientProcessor, this.clientManageExecutor);
    
            /**
             * ConsumerManageProcessor
             */
            ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(this);
            this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
            this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
            this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
    
            /**
             * EndTransactionProcessor
             */
            this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.sendMessageExecutor);
    
            /**
             * Default
             */
            AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
            this.remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
        }

        public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway) {

            TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();
    
            if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {

                ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
                for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                    TopicConfig tmp = new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                            this.brokerConfig.getBrokerPermission());
                    topicConfigTable.put(topicConfig.getTopicName(), tmp);
                }
                topicConfigWrapper.setTopicConfigTable(topicConfigTable);
            }
    
            RegisterBrokerResult registerBrokerResult = this.brokerOuterAPI.registerBrokerAll(
                this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId(),
                // 将此 Broker 的 haMasterAddress 地址也注册到 NameServer 上
                this.getHAServerAddr(),
                topicConfigWrapper,
                this.filterServerManager.buildNewFilterServerList(),
                oneway,
                this.brokerConfig.getRegisterBrokerTimeoutMills());
    
            if (registerBrokerResult != null) {
                // 1.如果此 Broker 是 MASTER 的话，updateMasterHAServerAddrPeriodically 就为 false
                // 2.如果此 Broker 是 SLAVE 的话，
                //     i.如果配置了 haMasterAddress 的话，updateMasterHAServerAddrPeriodically 为 false，表明不会从 NameServer
                //     上获取 haMasterAdress 地址
                //     ii.如果没有配置 haMasterAddress 的话，就会从 NameServer 上获取到 haMasterAddress 地址，并且将其通过
                //     messageStore.updateHaMasterAddress 保存到 HAClient 中
                //
                // 这里需要注意一下，haMasterAddress 并不简单等于 MasterAddress，而是等于  MasterAddress 中的 port + 1，
                // 一个 ip 地址为 192.168.1.6:2000 的 borker 在启动之后会监听 3 个端口，分别是 listenPort:2000，fastRemotingServer:1998,
                // haListenPort:2001，haListenPort 是在 HAService 中的 AcceptSocketService 进行监听，用于进行主从同步。其中这 3 个端口只有
                // listenPort 可以进行配置，其余两个都是 rocketmq 自动进行生成。
                //
                // 所以总结如下：
                // 1.如果 Broker 是 Master 的话，updateMasterHAServerAddrPeriodically 会一直为 false，也就是不会从 NameServer 上获取到
                // haMasterAddress。此 Master 会监听 3 个端口，和这里有关的是 2 个，一个是 listenPort，一个是 haListenPort，其中 haListenPort = listenPort + 1
                // listenPort 用来监听 Producer、Consumer 的请求，而 haListenPort 专门用来监听 Slave Broker 的请求，用来进行主从同步。
                // 2.如果 Broker 是 Slave，并且配置了 haMasterAddress 的话，就使用这个地址来进行主从同步，不会去 NameServer 上获取 haServerAddress，
                // 并且随后 HAClient 也使用这个地址来进行连接
                // 3.如果 Broker 是 Slave，并且没有配置 haMasterAddress 的话，就必须到 NameServer 上获取 haMasterAddress，然后通过 messageStore.updateHaMasterAddress
                // 将 haMasterAddress 设置到 HAClient#masterAddress 中，随后使用这个地址进行连接
                // 
                // 在 HAService#AcceptSocketService 中，socketAddressListen 是 Master 用来监听 Slave 的主从同步请求，socketAddressListen 等于 haListenPort，
                // 不能进行配置，而 haListenPort 由 rocketmq 自动进行设置（在 BrokerController#initialize 方法中），等于 listenPort + 1。
                // 在 HAService#HAClient 中，masterAddress 是由用户自己配置的 haMasterAddress 或者从 NameServer 上获取到的地址进行设置
                if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) {
                    this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
                }
    
                this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());
    
                if (checkOrderConfig) {
                    this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
                }
            }
        }

    }

    public class BrokerOuterAPI {

        public RegisterBrokerResult registerBrokerAll(final String clusterName, final String brokerAddr, final String brokerName, 
                final long brokerId, final String haServerAddr, final TopicConfigSerializeWrapper topicConfigWrapper, 
                final List<String> filterServerList, final boolean oneway, final int timeoutMills) {

            RegisterBrokerResult registerBrokerResult = null;

            List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
            if (nameServerAddressList != null) {
                // 遍历 NameServer 的列表，Broker 依次向 NameServer 发送心跳包
                for (String namesrvAddr : nameServerAddressList) {
                    try {
                        RegisterBrokerResult result = this.registerBroker(namesrvAddr, clusterName, brokerAddr, brokerName, brokerId, 
                                haServerAddr, topicConfigWrapper, filterServerList, oneway, timeoutMills);

                        if (result != null) {
                            registerBrokerResult = result;
                        }

                        log.info("register broker to name server {} OK", namesrvAddr);
                    } catch (Exception e) {
                        log.warn("registerBroker Exception, {}", namesrvAddr, e);
                    }
                }
            }

            return registerBrokerResult;
        }

        public void unregisterBrokerAll(final String clusterName, final String brokerAddr, final String brokerName, final long brokerId) {
            List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
            // 遍历 NameServer 列表，然后依次向其发送 unregister 请求
            if (nameServerAddressList != null) {
                for (String namesrvAddr : nameServerAddressList) {
                    try {
                        this.unregisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName, brokerId);
                        log.info("unregisterBroker OK, NamesrvAddr: {}", namesrvAddr);
                    } catch (Exception e) {
                        log.warn("unregisterBroker Exception, {}", namesrvAddr, e);
                    }
                }
            }
        }

        // Broker 向 NameServer 发送心跳包的具体逻辑
        // RocketMQ 网络传输基于 Netty。每一个请求，RocketMQ 都会定义一个 RequestCode，然后在服务端会对应相应的网络处理器（processor 包中）
        // 只需整库搜索 questCode 即可找到相应的处理逻辑。
        // 这里发送心跳包的 RequestCode 为 REGISTER_BROKER，由 NameSever 的 DefaultMessageProcessor 进行处理
        private RegisterBrokerResult registerBroker(final String namesrvAddr, final String clusterName,
                final String brokerAddr, final String brokerName, final long brokerId, final String haServerAddr,
                final TopicConfigSerializeWrapper topicConfigWrapper, final List<String> filterServerList,
                final boolean oneway, final int timeoutMills) throws Exception {
                    
            // 封装请求包头（Header）
            RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
            requestHeader.setBrokerAddr(brokerAddr);
            requestHeader.setBrokerId(brokerId);
            requestHeader.setBrokerName(brokerName);
            requestHeader.setClusterName(clusterName);
            requestHeader.setHaServerAddr(haServerAddr);

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, requestHeader);

            // 封装请求体
            RegisterBrokerBody requestBody = new RegisterBrokerBody();
            requestBody.setTopicConfigSerializeWrapper(topicConfigWrapper);
            requestBody.setFilterServerList(filterServerList);
            request.setBody(requestBody.encode());

            if (oneway) {
                try {
                    this.remotingClient.invokeOneway(namesrvAddr, request, timeoutMills);
                } catch (RemotingTooMuchRequestException e) {
                    // do nothing
                }
                return null;
            }

            RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMills);
            assert response != null;

            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    // ignore code
                }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark());
        }

    }

    /**
     * NameServer 默认注册的是 DefaultRequestProcessor 处理器。如果设置了 NamesrvConfig.clusterTest=true，则会注册 ClusterTestRequestProcessor 处理器。
     * ClusterTestRequestProcessor 继承 DefaultRequestProcessor；ClusterTestRequestProcessor 仅仅重写了 getRouteInfoByTopic() 方法。
     * 判断如果获取不到 topicRouteData 数据，则会去其它的 NameServer 上查找该数据并返回。DefaultRequestProcessor 通过 processRequest() 
     * 方法来处理客户端发过来的请求。该方法通过 request 的 code 值来判断是属于哪种类型的操作。接收到的所有请求操作的数据都保存在 RouteInfoManager 类中，
     * 所有的操作都是对 RouteInfoManager 类的操作。
     */
    public class DefaultRequestProcessor implements NettyRequestProcessor {

        // DefaultRequestProcessor#processRequest
        // 根据 processRequest() 方法分析源码，发现接收到的所有请求操作的数据都保存在 KVConfigManager 和 RouteInfoManager 类中，
        // 所有的操作都是对 KVConfigManager 和 RouteInfoManager 类的操作。
        public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
            if (log.isDebugEnabled()) {
                log.debug("receive request, {} {} {}", request.getCode(),
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()), request);
            }

            switch (request.getCode()) {

                // ignore code

                case RequestCode.REGISTER_BROKER:
                    Version brokerVersion = MQVersion.value2Version(request.getVersion());
                    if (brokerVersion.ordinal() >= MQVersion.Version.V3_0_11.ordinal()) {
                        return this.registerBrokerWithFilterServer(ctx, request);
                    } else {
                        // org.apache.rocketmq.namesrv processor.DefaultRequestProcessor 网络处理器解析请求类型，
                        // 如果请求类型为 RequestCode REGISTER_BROKER ，则请求最终转发到 RoutelnfoManager#registerBroker
                        return this.registerBroker(ctx, request);
                    }
                case RequestCode.UNREGISTER_BROKER:
                    return this.unregisterBroker(ctx, request);
            
                // ignore code

                case RequestCode.GET_NAMESRV_CONFIG:
                    return this.getConfig(ctx, request);
                default:
                    break;
            }
            return null;
        }

        public RemotingCommand registerBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {

            final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
            final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();
            final RegisterBrokerRequestHeader requestHeader = (RegisterBrokerRequestHeader) request
                    .decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

            TopicConfigSerializeWrapper topicConfigWrapper;
            if (request.getBody() != null) {
                topicConfigWrapper = TopicConfigSerializeWrapper.decode(request.getBody(), TopicConfigSerializeWrapper.class);
            } else {
                topicConfigWrapper = new TopicConfigSerializeWrapper();
                topicConfigWrapper.getDataVersion().setCounter(new AtomicLong(0));
                topicConfigWrapper.getDataVersion().setTimestamp(0);
            }

            RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(
                    requestHeader.getClusterName(), requestHeader.getBrokerAddr(), requestHeader.getBrokerName(),
                    requestHeader.getBrokerId(), requestHeader.getHaServerAddr(), topicConfigWrapper, null,
                    ctx.channel());

            responseHeader.setHaServerAddr(result.getHaServerAddr());
            responseHeader.setMasterAddr(result.getMasterAddr());

            byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
            response.setBody(jsonValue);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        /**
         * RocketMQ 路由发现是非实时的，当 Topic 路由出现变化后，NameServer 不主动推送给客户端。而是由客户端定时拉取主题最新的路由。根据主题名
         * 称拉取路由信息的命令编码为：GET_ROUTEINTO_BY_TOPIC
         */
        public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
            
            final RemotingCommand response = RemotingCommand.createResponseCommand(null);

            final GetRouteInfoRequestHeader requestHeader = (GetRouteInfoRequestHeader) request
                    .decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

            // 调用 RouteInfoManager 的方法，从路由表 topicQueueTable、brokerAddrTable、filterServerTable 中分别填充 TopicRouteData 中的
            // List<QueueData>、List<BrokerData> 和 FilterServer 地址表
            TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(requestHeader.getTopic());

            if (topicRouteData != null) {
                // 如果找到主题对应的路由信息并且该主题为顺序消息，那么从 NameServer KVconfig 中获取关于顺序消息相关的配置填充路由信息
                if (this.namesrvController.getNamesrvConfig().isOrderMessageEnable()) {
                    String orderTopicConf = this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, requestHeader.getTopic());
                    topicRouteData.setOrderTopicConf(orderTopicConf);
                }

                byte[] content = topicRouteData.encode();
                response.setBody(content);
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            }

            // 如果没找到对应的路由信息，则 CODE 使用 TOPIC_NOT_EXISTS，表示没有找到对应的路由信息
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic() + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
            return response;

        }

    }

    public class TopicRouteData extends RemotingSerializable {

        private String orderTopicConf;
        private List<QueueData> queueDatas;
        private List<BrokerData> brokerDatas;
        private HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    }

    // 当 NameServer 和 Broker 的长连接断掉之后，onChannelDestroy 函数会被调用，把这个 Broker 的信息清理出去
    // RocketMQ 使用 BrokerHouseKeepingService 来处理 broker 是否存活. 如果 broker 失效, 异常或者关闭, 则将 broker 从 RouteInfoManager 路由信息中移除, 
    // 同时将与该 broker 相关的 topic 信息也一起删除. Netty 服务端专门启动了一个线程用于监听连接的失效, 异常或者关闭等的事件队列, 
    // 当事件队列里面有新事件时, 则取出事件并判断事件的类型, 然后调用 BrokerHouseKeepingService 对应的方法来处理该事件
    public class BrokerHousekeepingService implements ChannelEventListener {
        private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
        private final NamesrvController namesrvController;
    
        public BrokerHousekeepingService(NamesrvController namesrvController) {
            this.namesrvController = namesrvController;
        }
    
        @Override
        public void onChannelConnect(String remoteAddr, Channel channel) {
        }
    
        @Override
        public void onChannelClose(String remoteAddr, Channel channel) {
            this.namesrvController.getRouteInfoManager().onChannelDestroy(remoteAddr, channel);
        }
    
        @Override
        public void onChannelException(String remoteAddr, Channel channel) {
            this.namesrvController.getRouteInfoManager().onChannelDestroy(remoteAddr, channel);
        }
    
        @Override
        public void onChannelIdle(String remoteAddr, Channel channel) {
            this.namesrvController.getRouteInfoManager().onChannelDestroy(remoteAddr, channel);
        }
    }

}