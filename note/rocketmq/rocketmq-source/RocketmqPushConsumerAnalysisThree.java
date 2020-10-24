public class RocketmqPushConsumerAnalysisThree{

    /**
     * 消费者消息重试
     * 
     * Consumer 在启动的时候，会执行一个函数 copySubscription()，当用户注册的消息模型为集群模式的时候，会根据用户指定的组创建重试组话题并放入到注册信息中。
     * 
     * 假设用户的消费组名称为 "ORDER"，那么重试话题则为 "%RETRY%ORDER"，即前面加上了 "%RETRY%" 这个字符串。Consumer在一开始启动的时候，
     * 就为用户自动注册了订阅组的重试话题。即用户不单单只接受这个组的话题的消息，也接受这个组的重试话题的消息。这样一来，就为接下来用户如何重试接受消息奠定了基础。
     * 
     * 当 Consumer 客户端在消费消息的时候，抛出了异常、返回了非正确消费的状态等错误的时候，这个时候 ConsumeMessageConcurrentlyService 会收集所有失败的消息，
     * 然后将每一条消息封装进 CONSUMER_SEND_MSG_BACK 的请求中，并将其发送到 Broker 服务器。
     * 
     * 当消费失败的消息重新发送到服务器后，Broker 会为其指定新的话题重试 topic（就是 %RETRY% + ConsumerGroup），以及新的重试队列 id，并根据当前这条消息的已有的重试次数来选择定时级别，
     * 即将这条消息变成定时消息投放到重试话题消息队列中。可见消息消费失败后并不是立即进行新的投递，而是有一定的延迟时间的。延迟时间随着重试次数的增加而增加，
     * 也即投递的时间的间隔也越来越长。
     * 
     * 当然，消息如果一直消费不成功，那也不会一直无限次的尝试重新投递的。当重试次数大于最大重试次数 (默认为 16 次) 的时候，该消息将会被送往死信话题队列。
     * 
     */


    /**
     * 定时消息
     * 
     * 定时消息是指消息发送到 Broker 后，并不立即被消费者消费而是要等到特定的时间后才能被消费， RocketMQ 并不支持任意的时间精度， 
     * 如果要支持任意时间精度的定时调度，不可避免地需要在 Broker 层做消息排序，再加上持久化方面的考量，将不可避免地带来具大的性能消耗，
     * 所以 RocketMQ 只支持特定级别的延迟消息。
     * 
     * 说到定时任务，上文提到的消息重试正是借助定时任务实现的，在将消息存入 commitlog 文件之前需要判断消息的重试次数 ，如果大于 0，
     * 则会将消息的主题设置 SCHEDULE_TOPIC_XXXX，RocketMQ 定时消息 实现类为 org.apache.rocketmq.store.schedule.ScheduleMessageService。
     * 该类的实例在 DefaultMessageStore 中创建，具体的调用链如下：
     * 
     * BrokerStartup
     * |-main
     *      |-start
     *          |-createBrokerController
     *              |-BrokerController.initialize()    
     *              |-controller.start()
     *                  |-DefaultMessageStore.start()
     *                      |-new ScheduleMessageService(this)
     *                      |-scheduleMessageService.start()
     * 
     * 本文我们完整的对RocketMQ的定时消息实现方式进行了分析，我们总结一下它的完整流程：
     * 
     * 1.消息发送方发送消息，设置 delayLevel。
     * 2.如果 delayLevel 大于 0，表明是一条延时消息，broker 处理该消息，将消息的原始主题、队列原始 id 进行备份后，改变消息的主题为 SCHEDULE_TOPIC_XXXX，
     * 队列 id = 延迟级别 - 1，将消息持久化。
     * 3.通过定时任务 ScheduleMessageService 对定时消息进行处理，每隔 1s 从上次拉取偏移量取出所有的消息进行处理
     * 4.从消费队列中解析出消息的物理偏移量，从而从 commitLog 中取出消息
     * 5.根据消息的属性重建消息，恢复消息的 topic、原队列 id，将消息的延迟级别属性 delayLevel 清除掉，再次保存到 commitLog 中，此时消费者可以对该消息进行消费
     * 
     * 综上，也就是说，rocketmq 对于延时消息是进行单独处理，如果一个消息的 delayLevel 大于 0，那么表明这个消息是延时消息，会将其暂存到延时队列中。其中，每一个
     * 延时级别 DelayLevel 对应一个延时队列。同时，对于每一个延迟级别，都会开启一个任务 DeliverDelayedMessageTimerTask 判断其延时队列中的消息是否到期，如果到期，
     * 则恢复其原始的主题 topic 和队列 id，并且将其重新写入到 commitLog 中，供消费者消费
     */

    /**
     * 长轮询
     * 
     * Push 模式：
     * Push 即服务端主动发送数据给客户端。在服务端收到消息之后立即推送给客户端。
     * Push 模型最大的好处就是实时性。因为服务端可以做到只要有消息就立即推送，所以消息的消费没有"额外"的延迟。
     * 但是 Push 模式在消息中间件的场景中会面临以下一些问题：
     * 1.在 Broker 端需要维护 Consumer 的状态，不利于 Broker 去支持大量的 Consumer 的场景
     * 2.Consumer 的消费速度是不一致的，由 Broker 进行推送难以处理不同的 Consumer 的状况
     * 3.Broker 难以处理 Consumer 无法消费消息的情况（Broker 无法确定 Consumer 的故障是短暂的还是永久的）
     * 4.大量的推送消息会加重 Consumer 的负载或者冲垮 Consumer
     * 
     * Pull 模式：
     * Broker不再需要维护Consumer的状态（每一次pull都包含了其实偏移量等必要的信息）
     * 状态维护在Consumer，所以Consumer可以很容易的根据自身的负载等状态来决定从Broker获取消息的频率
     * 但是，和Push模式正好相反，Pull就面临了实时性的问题。因为由Consumer主动来Pull消息，所以实时性和Pull的周期相关，这里就产生了“额外”延迟。
     * 如果为了降低延迟来提升 Pull 的执行频率，可能在没有消息的时候产生大量的Pull请求，也就是空轮询（消息中间件是完全解耦的，Broker 和 Consumer 无法预测下一条消息在什么时候产生）;
     * 如果频率低了，那延迟自然就大了。
     * 
     * 
     * 通过研究源码可知，RocketMQ的消费方式都是基于拉模式拉取消息的，而在这其中有一种长轮询机制（对普通轮询的一种优化），来平衡上面Push/Pull模型的各自缺点。
     * 
     * 基本设计思路是：消费者如果第一次尝试 Pull 消息失败（比如：Broker 端没有可以消费的消息），Broker 并不立即给消费者客户端返回 Response 的响应，而是先 hold 住并且挂起请求
     * （将请求保存至 pullRequestTable 本地缓存变量中），然后 Broker 端的后台独立线程 — PullRequestHoldService 会每隔 5s 去检查是否有新的消息到达。
     * Broker 在一直有新消息到达的情况下，长轮询就变为执行时间间隔为 0 的 pull 模式
     * Broker 在一直没有新消息到达的情况下，请求阻塞在了 Broker，在下一条新消息到达或者长轮询等待时间超时的时候响应请求给 Consumer
     * 
     * RocketMQ 消息 Pull 的长轮询机制的关键在于 Broker 端的 PullRequestHoldService 和 ReputMessageService 两个后台线程
     */

    /**
     * 从使用上可以推断顺序消息需要从发送到消费整个过程中保证有序，所以顺序消息具体表现为:
     * 
     * i.发送消息是顺序的
     * ii.broker 存储消息是顺序的
     * iii.consumer 消费是顺序的
     * 
     * 1.发送消息是顺序的
     * 
     * 因为 broker 存储消息有序的前提是 producer 发送消息是有序的，所以这两个结合在一起说。消息发布是有序的含义：producer 发送消息应该是依次发送的，
     * 所以要求发送消息的时候保证：
     * 
     * i.消息不能异步发送，同步发送的时候才能保证 broker 收到是有序的。
     * ii.每次发送选择的是同一个 MessageQueue
     * 
     * producer 发送消息的时候是同步发送的。同步发送表示，producer 发送消息之后不会立即返回，会等待 broker 的 response。
     * broker 收到 producer 的请求之后虽然是启动线程处理的，但是在线程中将消息写入 commitLog 中以后会发送 response 给 producer，producer 在收到 broker 的 response
     * 并且是处理成功之后才算是消息发送成功。
     * 
     * 2.保证 Broker 存储消息是顺序的
     * 
     * 为了保证 broker 收到消息也是顺序的，所以 producer 只能向其中一个队列发送消息。因为只有是同一个队列才能保证消息是发往同一个 broker，
     * 只有同一个 broker 处理发来的消息才能保证顺序。所以发送顺序消息的时候需要用户指定 MessageQueue
     * 
     * 3.保证 consumer 消息消费时顺序的
     * 
     * 保证了 Broker 中物理存储的消息是顺序的，只要保证消息消费是顺序的就能保证整个过程是顺序消息了。顺序消费和普通消费的 Listener 是不一样的，
     * 顺序消费需要实现的是下面这个接口：org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly
     * 
     * 在 consumer 启动的时候会根据 listener 的类型判断应该使用哪一个 service 来消费，如果是顺序消息，就使用 ConsumeMessageOrderlyService
     * consumer 拉取消息是按照 offset 拉取的，所以 consumer 能保证拉取到 consumer 的消息是连续有序的，但是 consumer 拉取到消息后又启动了线程池去处理消息，
     * ，所以线程执行的顺序又不确定了，那么 consumer 消费就变成无序的了吗？
     * 
     * 这里要额外提一下 ProcessQueue 这个关键的数据结构。一个 MessageQueue 对应一个 ProcessQueue，这是一个有序队列，该队列记录一个 queueId 下所有从 broker 拉取回来的消息，
     * 如果消费成功了就会从队列中删除。ProcessQueue 有序的原因是维护了一个 TreeMap。msgTreeMap：里面维护了从broker 拉取回来的所有消息，TreeMap 是有序的，
     * key 是 Long 类型的，没有指定 comparator，因为key是当前消息的offset，而Long实现了Comparable接口，所以msgTreeMap里面的消息是按照offset排序的。
     * 所以是ProcessQueue保证了拉取回来的消息是有序的。
     * 
     * 但是，如果一个 processQueue 在同一时刻有多个线程去消费，那么还是不能保证消息消费的顺序性。这里就要引入锁的概念。Consumer 在严格顺序消费时，通过 3 把锁保证严格顺序消费。
     * Broker 消息队列锁（分布式锁） ：
     *      集群模式下，Consumer 从 Broker 获得该锁后，才能进行消息拉取、消费。
     *      广播模式下，Consumer 无需该锁。
     * Consumer 消息队列锁（本地锁） ：Consumer 获得该锁才能操作消息队列。
     * Consumer 消息处理队列消费锁（本地锁） ：Consumer 获得该锁才能消费消息队列。
     * 
     * 锁的逻辑是这样，在最初通过 RebalanceImpl#updateProcessQueueTableInRebalance 方法给消费者 Consumer 分配消息队列时，如果有新的消息队列分配给 
     * Consumer，如果是顺序消息，通过 lock 方法会向 Broker 发起锁定消息队列的请求，然后该方法会返回被当前消费者成功锁定的消息队列集合。接着就会将这些
     * 消息队列对应的 ProcessQueue 设置为锁定状态。这些锁有过期期限，默认为 60s，所以 ConsumeMessageOrderlyService 的 start 方法中会每隔 20s 锁定
     * 这个 consumer 对应的 processQueue。
     * 
     * 在进行消息消费时，要首先获取到这个 MessageQueue 对应的锁（其实就是一个对象，每个 mq 都对应一个），然后还要获取到这个 processQueue 对应的消费锁。
     * 这就保证了一个消息队列在某一个时刻只能允许一个线程进行访问。
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
        // 延迟级别，将 "1s 5s 10s 30s 1m 2m 3m 4m Sm 6m 7m 8m 9m 10m 20m 30m lh 2h" 字符串解析成 delayLevelTable，
        // 转换后的数据结构类似 {1: 1000 ,2 :5000 30000, ...}
        private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable = new ConcurrentHashMap<Integer, Long>(32);
        // offsetTable,延迟级别对应的消费进度，key=延迟级别，value=对应延迟级别下的消费进度
        private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable = new ConcurrentHashMap<Integer, Long>(32);

        public boolean load() {
            // 延迟消息消费队列消息进度的加载，迟队列消息消费进度默认存储路径为 ${ROCKET HOME}/store/config/delayOffset.json
            boolean result = super.load();
            // delayLevelTable 数据的构造
            result = result && this.parseDelayLevel();
            return result;
        }

        public void start() {
            // 对不同的延迟级别创建对应的定时任务
            // 遍历延迟级别，根据延迟级别 level 从 offsetTable 中获取到消费队列的消费进度，如果不存在，则使用 0，
            // 也就是说，每一个延迟级别对应一个消息消费队列，也就是每一个消息队列有自己的消费进度 offset
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }
    
                // 然后创建定时任务，每一个时任务第一次启动时默认延迟 ls 先执行一次定时任务，第二次调度开始才使用相应的延迟时间。
                // 延迟级别与消息消费队列的映射关系为：消息队列 ID = 延迟级别 - 1
                if (timeDelay != null) {
                    this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
                }
            }
    
            this.timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        // 创建定时任务，每隔 10s 持久化一次延迟队列的消息消费进度（延迟消息调进度），也就是持久化 offsetTable 到磁盘的 delayOffset.json 上，
                        // 持久化频率可以通过 flushDelayOffsetInterval 配置属性进行设置
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
            // queueId = delayLevel - 1
            ConsumeQueue cq = ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(SCHEDULE_TOPIC, delayLevel2QueueId(delayLevel));

            long failScheduleOffset = offset;

            if (cq != null) {
                // 从消息消费队列 ConsumeQueue 中获取偏移量为 offset 的数据，包含多条消息。
                // 如果未找到，更新一下延迟队列定时拉取进度并创建定时任务待下一次继续尝试
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
                            // 这里的 tagsCode 是消息到期的时间戳
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            // 定时任务每次执行到这里都进行时间比较，计算延迟时间与当前时间的差值，如果延迟时间-当前时间<=0说明该延迟消息应当被处理，使其能够被消费者消费
                            long countdown = deliverTimestamp - now;

                            if (countdown <= 0) {
                                // 根据消息物理偏移量与消息大小从 commitlog 文件中查找消息。如果未找到消息，打印错误日志，根据延迟时间创建下一个定时器
                                MessageExt msgExt = ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);

                                if (msgExt != null) {
                                    try {
                                        // messageTimeup 执行消息的恢复操作，这里会清除掉消息的 delayLevel 属性，复原先的队列以及消息 topic，确保在保存到 commitLog 时，
                                        // 不会被再次放入到延迟队列。
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                        // 对消息执行重新存储操作，将消息重新持久化到 commitLog 中，此时的消息已经能够被消费者拉取到
                                        PutMessageResult putMessageResult = ScheduleMessageService.this.defaultMessageStore.putMessage(msgInner);
                                        
                                        // 如果写成功了，则继续写下一条消息
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
                                // 延迟时间还没到，所以等待 countdown 时间之后再次进行执行
                                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset), countdown);
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for
                        // offset 是一个逻辑值，而 i 表示消费的真实字节数
                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        // 继续处理延迟级别 delayLevel 的消息队列中的延迟消息，其实也就是 delayLevel - 1 的
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        // 更新当前延迟队列的消息拉取进度
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {
                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {
                    // 如果根据 offset 未取到 SelectMappedBufferResult，则纠正下次定时任务的 offset 为当前定时任务队列的最小值
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
            // 创建一个新的 MessageExtBrokerInner 对象
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            // ignore code

            msgInner.setWaitStoreMsgOK(false);
            // 删除掉 msgInner 中的 delayLevel 属性
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
            // 恢复 msgInner 中的原始 topic
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
            // 恢复 msgInner 中的原始队列 id
            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }

        // 计算消息的到期时间，也就是消息的存储时间 + 延迟时间
        public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
            Long time = this.delayLevelTable.get(delayLevel);
            if (time != null) {
                return time + storeTimestamp;
            }
    
            return storeTimestamp + 1000;
        }

    }

    /**
     * RocketMQ 支持局部消息顺序消费，可以确保同一个消息消费队列中的消息被顺序消费，如果需要做到全局顺序消费则可以将主题配置成一个队列。
     * 消息消费包含如下 4 个步骤：消息队列负载、消息拉取、消息消费、消息消费进度存储。
     * 
     * 每个 DefaultMQPushConsumerImpl 都持有一个单独的 RebalanceImpl 对象，该方法主要是遍历订阅信息对每个主题的队列进行重新的负载均衡，
     * 在调用 DefaultMQPushConsumerImpl#subscribe 方法时会对 RebalanceImpl 中的 subTable 属性进行填充
     */
    public abstract class RebalanceImpl{

        /**
         * RocketMQ 首先需要通过 RebalanceService 线程实现消息队列的负载， 集群模式下同一个消费组内的消费者共同承担其订阅主题下消息队列的消费，
         * 同一个消息消费队列在同一时刻只会被消费组内一个消费者消费，一个消费者同一时刻可以分配多个消费队列
         */
        private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet, final boolean isOrder) {

            // 省略代码

            List<PullRequest> pullRequestList = new ArrayList<PullRequest>();

            /**
             * 如果经过消息队列重新负载（分配）后，分配到新的消息队列时，首先需要尝试向 Broker 发起锁定该消息队列的请求，
             * 如果返回加锁成功则创建该消息队列的拉取任务，否则将跳过，等待其他消费者释放该消息队列的锁，然后在下一次队列
             * 重新负载时再尝试加锁。
             * 
             * 顺序消息消费与并发消息消费的第一个关键区别：顺序消息在创建消息队列拉取任务时需要在 Broker 服务器锁定该消息队列。
             */
            for (MessageQueue mq : mqSet) {
                if (!this.processQueueTable.containsKey(mq)) {
                    // 尝试向 Broker 发起锁定该消息队列的请求
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
                            // 把 pullRequest 加入到 pullRequestList 中，从而可以让 PullMessageService 不断地向  Broker 发起请求
                            pullRequestList.add(pullRequest);
                            changed = true;
                        }
                    } else {
                        log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                    }
                }
            }

            // 将 pullRequestList 中的 pullRequest 向 Broker 发起请求
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
            // 将消息队列按照 Broker 组织成 Map<String, Set<MessageQueue>>，方便下一步向 Broker 发送锁定请求消息队列的请求
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
                        // 向 Broker ( Master 主节点) 发送锁定消息队列的请求，该方法返回成功被当前消费者锁定的消息消费队列
                        Set<MessageQueue> lockOKMQSet = this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
    
                        // 将成功锁定的消息消费队列相对应的处理队列 ProcessQueue 设置为锁定状态，同时更新加锁时间
                        for (MessageQueue mq : lockOKMQSet) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                if (!processQueue.isLocked()) {
                                    log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                                }
    
                                // 将 pq 设定成锁定状态
                                processQueue.setLocked(true);
                                // 更新 pq 的锁定时间
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

        // 
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
                    // 发送请求从 Broker 中获取该 topic 下消费组内所有的消费者客户端 id
                    // 某个主题 topic 的队列可能分布在多个 Broker 上，请求该发送给哪个 Broker 呢？RocketeMQ 会从主题的路由信息表中随机选择一个 Broker，
                    // 为什么呢？因为消费者在启动的时候，会向 MQClientInstance 中注册消费者，然后 MQClientInstance 会向所有的 Broker 发送心跳包，而这个
                    // 心跳包中包含了 MQClientInstance 的消费者信息
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
                        // 将 MQ 和 cid 都排好序，这个很重要，因为要确保同一个消费队列不会被分配给多个消费者
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
         * 从以上的代码可以看出，rebalanceImpl 每次都会检查分配到的 queue 列表，如果发现有新的 queue 加入，就会给这个 queue 初始化一个缓存队列，
         * 然后新发起一个 PullRequest 给 PullMessageService 执行。
         * 
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
                    // 不再消费这个 MessageQueue 的消息，也就是说经过负载均衡的分配策略之后，分配给这个 consumer 的消息队列发生了变化
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
                // 如果是新加入的 MessageQueue，也就是说新分配给这个 consumer 的 MessageQueue
                if (!this.processQueueTable.containsKey(mq)) {
                    // 如果是顺序消息，对于新分配的消息队列，首先尝试向 Broker 发起锁定该消息队列的请求
                    // 如果返回加锁成功则创建该消息队列的拉取请求，否则直接跳过。等待其他消费者释放该消息队列的锁，然后在下一次队列重新负载均衡的时候
                    //  再尝试重新加锁
                    if (isOrder && !this.lock(mq)) {
                        log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                        continue;
                    }

                    // 从 OffsetStore 中移除过时数据
                    this.removeDirtyOffset(mq);
                    // 为新的 MessageQueue 初始化一个 ProcessQueue，用来缓存收到的消息
                    ProcessQueue pq = new ProcessQueue();
                    // 获取起始消费的 Offset
                    long nextOffset = this.computePullFromWhere(mq);
                    if (nextOffset >= 0) {
                        ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                        if (pre != null) {
                            log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                        } else {
                            // 对于新加的 MessageQueue，初始化一个 PullRequest，并且将其加入到 pullRequestList 中
                            // 在一个 JVM 进程中，同一个消费组中同一个队列只会存在一个 PullRequest 对象
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

        
        public void start() {
            if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                    // 如果消费模式为集群模式，启动定时任务，默认每隔 20s 执行一次锁定分配给自己的消息消费队列。
                    // 通过 -Drocketmq.client.rebalance.locklnterval = 20000 设置间隔，该值建议与一次消息负载频率设置相同。
                    // 从之前 RebalanceImpl#updateProcessQueueTableInRebalance 方法中，集群模式下顺序消息消费对于新分配到的 mq，
                    // 在创建拉取任务和 processQueue 时并未将 ProcessQueue 的 locked 状态设置为 true，在未锁定消息队列之前无法执行消息拉取任务。
                    // 
                    // ConsumeMessageOrderlyService 以每秒 20s 频率对分配给自己的消息队列进行自动锁操作，从而消费加锁成功的消息消费队列
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

        public void submitConsumeRequest(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue, 
                    final boolean dispathToConsume) {
            // 构建消费任务，并且提交到消费线程池中
            // 从这里可以看出，顺序消息的 ConsumeRequest 消费任务不会直接消费本次拉取的消息 msgs，也就是构建 ConsumeRequest 对象时，
            // msgs 完全被忽略了。而事实上，是在消息消费时从处理队列 processQueue 中拉取的消息
            if (dispathToConsume) {
                ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
                this.consumeExecutor.submit(consumeRequest);
            }
        }

        /**
         * 顺序消费消息结果 (ConsumeOrderlyStatus) 有四种情况:
         * 
         * SUCCESS：消费成功并且提交。
         * ROLLBACK：消费失败，消费回滚。
         * COMMIT：消费成功提交并且提交。
         * SUSPEND_CURRENT_QUEUE_A_MOMENT：消费失败，挂起消费队列一会，稍后继续消费。
         * 
         * 考虑到 ROLLBACK 、COMMIT 暂时只使用在 MySQL binlog 场景，官方将这两状态标记为 @Deprecated。当然，相应的实现逻辑依然保留。
         * 在并发消费场景时，如果消费失败，Consumer 会将消费失败消息发回到 Broker 重试队列，跳过当前消息，等待下次拉取该消息再进行消费。
         * 但是在完全严格顺序消费消费时，这样做显然不行，有可能会破坏掉消息消费的顺序性。也因此，消费失败的消息，会挂起队列一会会，稍后继续消费。
         * 不过消费失败的消息一直失败，也不可能一直消费。当超过消费重试上限时，Consumer 会将消费失败超过上限的消息发回到 Broker 死信队列。
         */
        public boolean processConsumeResult(final List<MessageExt> msgs, final ConsumeOrderlyStatus status, final ConsumeOrderlyContext context, final ConsumeRequest consumeRequest) {
            boolean continueConsume = true;
            long commitOffset = -1L;
            if (context.isAutoCommit()) {
                switch (status) {
                    case COMMIT:
                    case ROLLBACK:
                        log.warn("the message queue consume result is illegal, we think you want to ack these message {}", consumeRequest.getMessageQueue());
                    case SUCCESS:
                        // 将这批消息从 processQueue 中移除，同时维护 processQueue 中的状态信息
                        commitOffset = consumeRequest.getProcessQueue().commit();
                        this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                        break;
                    case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                        this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                        if (checkReconsumeTimes(msgs)) {
                            // 消息消费重试，先将这批消息重新放入到 processQueue 的 msgTree 中，同时从 consumingMsgOrderlyTreeMap 中移除掉，
                            consumeRequest.getProcessQueue().makeMessageToCosumeAgain(msgs);
                            this.submitConsumeRequestLater(consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue(), context.getSuspendCurrentQueueTimeMillis());
                            continueConsume = false;
                        } else {
                            commitOffset = consumeRequest.getProcessQueue().commit();
                        }   
                        break;
                    default:
                        break;
                }
            } else {
                // ignore code
            }

            if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
                this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), commitOffset, false);
            }

            return continueConsume;
        }

        // 如果 msgs 中的任何一条消息 msg 的重试次数小于最大重试次数，或者超过最大重试次数但是 sendMessageBack 失败的话，checkReconsumeTimes
        // 都会返回 true，在 processConsumeResult 方法中执行消息的重新消费工作
        private boolean checkReconsumeTimes(List<MessageExt> msgs) {
            boolean suspend = false;
            if (msgs != null && !msgs.isEmpty()) {
                for (MessageExt msg : msgs) {
                    // 检查消息 msg 的最大重试次数，如果大于或者等于允许的最大重试次数，那么就将该消息发送到 Broker 端，
                    // 该消息在消息服务端最终会进入到 DLQ 队列。
                    if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
                        MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));
                        // 如果消息成功进入到 DLQ 队列，sendMessageBack 返回 true
                        if (!sendMessageBack(msg)) {
                            suspend = true;
                            msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        }
                    } else {
                        suspend = true;
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    }
                }
            }
            return suspend;
        }

        public boolean sendMessageBack(final MessageExt msg) {
            try {
                // max reconsume times exceeded then send to dead letter queue.
                Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
                String originMsgId = MessageAccessor.getOriginMessageId(msg);
                MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
                newMsg.setFlag(msg.getFlag());
                MessageAccessor.setProperties(newMsg, msg.getProperties());
                MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
                MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes()));
                MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
                newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());
    
                this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getDefaultMQProducer().send(newMsg);
                return true;
            } catch (Exception e) {
                log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
            }
    
            return false;
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
                        
                        // 再次检查 processQueue 是否被锁定，如果没有，这延迟该消息队列的消费
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && !this.processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        // 同样，再次检查 processQueue 的锁是否超时，如果超时，也延迟该消息队列的消费
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && this.processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        long interval = System.currentTimeMillis() - beginTime;
                        // 顺序消息的消费的处理逻辑，每一个 ConsumeRequest 消费任务不是以消费消息条数来计算的，而是根据消费时间，
                        // 默认当消费时长超过 MAX_TIME_CONSUME_CONTINUOUSLY 之后，会延迟 10ms 再进行消息队列的消费，默认情况下，每消费 1 分钟休息 10ms
                        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            // 延迟 10ms 之后再消费
                            ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                            break;
                        }

                        // 每次从处理队列中按顺序取出 consumeBatchSize 消息，如果未取到消息，也就是 msgs 为空，则设置 continueConsume 为 false ，本次消费任务结束。
                        // 顺序消息消费时，从 ProceessQueue 取出的消息，会临时存储在 ProceeQueue 的 consumingMsgOrderlyTreeMap 属性中
                        final int consumeBatchSize = ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
                        
                        // 注意这里和并发处理消息不同，并发消费请求在 ConsumeRequest 创建时，已经设置好消费哪些消息
                        List<MessageExt> msgs = this.processQueue.takeMessags(consumeBatchSize);

                        if (!msgs.isEmpty()) {
                            final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);
                            ConsumeOrderlyStatus status = null;
                            ConsumeMessageContext consumeMessageContext = null;

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                // ignore code
                                // 执行消息消费钩子函数（消息消费之前 before 方法）
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                            }

                            long beginTimestamp = System.currentTimeMillis();
                            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                            boolean hasException = false;
                            try {
                                // 申请消息消费锁，然后执行消息消费监听器，调用业务方具体消息监听器执行真正的消息消费处理逻辑，并通知 RocketMQ 消息消费结果
                                this.processQueue.getLockConsume().lock();
                                if (this.processQueue.isDropped()) {
                                    log.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
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
         * 提交，就是将该批消息从 ProceeQueue 中移除，维护 msgCount (消息处理队列中消息条数) 并获取消息消费的偏移量 offset，
         * 然后将该批消息从 msgTreeMapTemp 中移除，并返回待保存的消息消费进度 (offset+ 1)，从中可以看出 offset 表示消息消费队列的逻辑偏移似于数组的下标，
         * 代表第 n 个 ConsumeQueue 条目
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
                    // 将该批消息从 msgTreeMapTemp 中移除
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


    public class DefaultMessageStore extends MessageStore{

        public ConsumeQueue findConsumeQueue(String topic, int queueId) {

            ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
            if (null == map) {
                ConcurrentMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
                ConcurrentMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
                if (oldMap != null) {
                    map = oldMap;
                } else {
                    map = newMap;
                }
            }
    
            ConsumeQueue logic = map.get(queueId);
            if (null == logic) {
                ConsumeQueue newLogic = new ConsumeQueue(topic, queueId, StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                    this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(), this);
                ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
                if (oldLogic != null) {
                    logic = oldLogic;
                } else {
                    logic = newLogic;
                }
            }
    
            return logic;
        }

        class ReputMessageService extends ServiceThread {

            @Override
            public void run() {
                DefaultMessageStore.log.info(this.getServiceName() + " service started");

                while (!this.isStopped()) {
                    try {
                        Thread.sleep(1);
                        this.doReput();
                    } catch (Exception e) {
                        DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                    }
                }

                DefaultMessageStore.log.info(this.getServiceName() + " service end");
            }

            private void doReput() {
                for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {
    
                    if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable() && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
                        break;
                    }
    
                    SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                    if (result != null) {
                        try {
                            this.reputFromOffset = result.getStartOffset();
    
                            for (int readSize = 0; readSize < result.getSize() && doNext; ) {
                                DispatchRequest dispatchRequest = DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                                int size = dispatchRequest.getMsgSize();
    
                                if (dispatchRequest.isSuccess()) {
                                    if (size > 0) {
                                        DefaultMessageStore.this.doDispatch(dispatchRequest);
                                        // 如果开启了长轮询，并且 Broker 的角色为主节点的话，则通知有新消息到达，执行 NotifyMessageArrivingListener 代码，
                                        // 最终调用 pullRequestHoldService 的 notifyMessageArriving 方法，进行一次消息拉取
                                        if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                            && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()) {
                                            DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                                dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                                                dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                                                dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
                                        }
    
                                        // ignore code
                                    } else if (size == 0) {
                                        this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                        readSize = result.getSize();
                                    }
                                } else if (!dispatchRequest.isSuccess()) {

                                    // ignore code

                                }
                            }
                        } finally {
                            result.release();
                        }
                    } else {
                        doNext = false;
                    }
                }
            }

        }

    }
    


}