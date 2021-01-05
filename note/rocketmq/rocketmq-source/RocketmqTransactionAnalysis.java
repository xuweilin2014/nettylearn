public class RocketmqTransactionAnalysis{

    /**
     * 事务消息
     * 
     * 事务消息的大致分为两个流程：正常事务消息的发送及提交、事务消息的补偿流程。
     * 
     * 1.事务消息发送及提交
     *     1).发送消息(half 消息)
     *     2).服务端响应消息写入结果
     *     3).根据发送结果执行本地事务（如果写入失败，此时 half 消息对业务不可见，本地逻辑不执行）
     *     4).根据本地事务状态执行 Commit 或者 Rollback（Commit 操作生成消息索引，消息对消费者可见）
     * 
     * 2.事务补偿
     *     1).对没有 Commit/Rollback 的事务消息（pending 状态的消息），从服务端发起一次"回查"
     *     2).Producer 收到回查消息，检查回查消息对应的本地事务的状态
     *     3).根据本地事务状态，重新 Commit 或者 Rollback
     * 
     * 其中，补偿阶段用于解决消息 Commit 或者 Rollback 发生超时或者失败的情况。
     * 
     * 事务的消息状态，事务消息共有三种状态，提交状态、回滚状态、中间状态：
     * 1.TransactionStatus.CommitTransaction:提交事务，它允许消费者消费此消息。
     * 2.TransactionStatus.RollbackTransaction:回滚事务，它代表该消息将被删除，不允许被消费。
     * 3.TransactionStatus.Unknown:中间状态，它代表需要检查消息队列来确定状态。
     * 
     * 事务消息存储在消息服务器时主题被替换为 RMQ_SYS_TRANS_HALF_TOPIC，执行完本地事务返回本地事务状态为 UNKNOW 时，
     * 结束事务时将不做任何处理，而是通过事务状态的定时回查以期得到发送端明确的事务操作。
     * 
     * RocketMQ 通过 TransactionalMessageCheckService 线程来定时去检测 RMQ_SYS_TRANS_HALF_TOPIC 主题
     * 中的消息，回查消息的事务状态。TransactionalMessageCheckService 的检测频率默认为 1 分钟。
     */

    public interface TransactionListener {
        /**
         * When send transactional prepare(half) message succeed, this method will be invoked to execute local transaction.
         * 执行本地事务
         */
        LocalTransactionState executeLocalTransaction(final Message msg, final Object arg);

        /**
         * When no response to prepare(half) message. broker will send check message to check the transaction status, and this
         * method will be invoked to get local transaction status.
         * 回查事务状态
         */
        LocalTransactionState checkLocalTransaction(final MessageExt msg);
    }

    public class TransactionMQProducer extends DefaultMQProducer {
        // 事务监听器，主要定义实现本地事务执行、事务状态回查两个接口
        private TransactionListener transactionListener;
        // 事务状态回查异步执行线程
        private ExecutorService executorService;

        @Override
        // TransactionMQProducer#sendMessageInTransaction
        public TransactionSendResult sendMessageInTransaction(final Message msg, final Object arg) throws MQClientException {
            if (null == this.transactionListener) {
                throw new MQClientException("TransactionListener is null", null);
            }

            return this.defaultMQProducerImpl.sendMessageInTransaction(msg, transactionListener, arg);
        }
    }

    public class DefaultMQProducerImpl implements MQProducerInner {

        // DefaultMQProducerImpl#sendMessageInTransaction
        public TransactionSendResult sendMessageInTransaction(final Message msg, final TransactionListener tranExecuter, final Object arg) throws MQClientException {
            // 如果事件监听器为空，那么就直接返回异常，
            if (null == tranExecuter) {
                throw new MQClientException("tranExecutor is null", null);
            }
            Validators.checkMessage(msg, this.defaultMQProducer);

            SendResult sendResult = null;
            // 在消息属性中，添加两个属性：TRAN_MSG，其值为 true，表示为事务消息; PGROUP：消息所属发送者组，然后以同步方式发送消息
            // 设置消息生产组的目的是在查询事务消息本地事务状态的时候，从该生产者组中随机选择一个消息生产者即可
            // PROPERTY_TRANSACTION_PREPARED 的值为 TRAN_MSG
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
            // PROPERTY_PRODUCER_GROUP 的值为 PGROUP
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, this.defaultMQProducer.getProducerGroup());
            try {
                sendResult = this.send(msg);
            } catch (Exception e) {
                throw new MQClientException("send message Exception", e);
            }

            LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
            Throwable localException = null;
            // sendResult 为服务端响应 prepare 消息的写入结果
            // 如果 prepare 成功写入服务端，那么就直接执行本地事务
            // 如果 prepare 消息写入失败，那么就直接回滚事务，ROLLBACK
            switch (sendResult.getSendStatus()) {
            case SEND_OK: {
                try {
                    if (sendResult.getTransactionId() != null) {
                        msg.putUserProperty("__transactionId__", sendResult.getTransactionId());
                    }
                    String transactionId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                    if (null != transactionId && !"".equals(transactionId)) {
                        msg.setTransactionId(transactionId);
                    }
                    // 如果消息发送成功，则执行 TransactionListener#executeLocalTransaction 方法
                    // 并且该方法与业务代码处于同一事务，与业务事务要么一起成功，要么一起失败
                    // 该方法的作用是记录事务消息的本地事务状态，这里是事务消息设计的关键理念之一，为后续的事务状态回查提供唯一依据
                    localTransactionState = tranExecuter.executeLocalTransaction(msg, arg);
                    // executeLocalTransaction 返回 null，则相当于 localTransactionState 为 UNKNOW
                    if (null == localTransactionState) {
                        localTransactionState = LocalTransactionState.UNKNOW;
                    }

                    if (localTransactionState != LocalTransactionState.COMMIT_MESSAGE) {
                        log.info("executeLocalTransactionBranch return {}", localTransactionState);
                        log.info(msg.toString());
                    }
                } catch (Throwable e) {
                    log.info("executeLocalTransactionBranch exception", e);
                    log.info(msg.toString());
                    localException = e;
                }
            }
                break;
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case SLAVE_NOT_AVAILABLE:
                // 如果消息发送失败，则设置本次事务状态为 LocalTransactionState.ROLLBACK_MESSAGE
                localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
                break;
            default:
                break;
            }

            try {
                // 结束事务，根据第二步返回的事务状态执行，向 Broker 发送提交（COMMIT）、回滚（ROLLBACK）或者暂时不处理（UNKNOW）事务
                // LocalTransactionState.COMMIT_MESSAGE:提交事务
                // LocalTransactionState.ROLLBACK_MESSAGE:回滚事务
                // LocalTransactionState.UNKNOW:结束事务，但是不做任何处理
                this.endTransaction(sendResult, localTransactionState, localException);
            } catch (Exception e) {
                log.warn("local transaction execute " + localTransactionState + ", but end broker transaction failed", e);
            }

            TransactionSendResult transactionSendResult = new TransactionSendResult();
            transactionSendResult.setSendStatus(sendResult.getSendStatus());
            transactionSendResult.setMessageQueue(sendResult.getMessageQueue());
            transactionSendResult.setMsgId(sendResult.getMsgId());
            transactionSendResult.setQueueOffset(sendResult.getQueueOffset());
            transactionSendResult.setTransactionId(sendResult.getTransactionId());
            transactionSendResult.setLocalTransactionState(localTransactionState);
            return transactionSendResult;
        }

        // DefaultMQPushConsumerImpl#endTransaction
        public void endTransaction(final SendResult sendResult, final LocalTransactionState localTransactionState, final Throwable localException) throws Exception {
            final MessageId id;
            if (sendResult.getOffsetMsgId() != null) {
                id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
            } else {
                id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
            }

            String transactionId = sendResult.getTransactionId();
            final String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(sendResult.getMessageQueue().getBrokerName());
            EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
            requestHeader.setTransactionId(transactionId);
            requestHeader.setCommitLogOffset(id.getOffset());

            switch (localTransactionState) {
            case COMMIT_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                break;
            case ROLLBACK_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                break;
            case UNKNOW:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                break;
            default:
                break;
            }

            requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
            requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
            requestHeader.setMsgId(sendResult.getMsgId());
            String remark = localException != null ? ("executeLocalTransactionBranch exception: " + localException.toString()) : null;
            // 根据消息所属的消息队列获取 Broker 的 IP 与端口信息，然后发送结束事务命令，其关键就是根据本地执行事务的状态分别提交、
            // 回滚或者"不作为"的命令
            this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, requestHeader, remark, this.defaultMQProducer.getSendMsgTimeout());
        }

        /**
         * DEFAULT SYNC -------------------------------------------------------
         */
        public SendResult send(Message msg) throws Exception {
            return send(msg, this.defaultMQProducer.getSendMsgTimeout());
        }

    }

    public class TransactionalMessageServiceImpl implements TransactionalMessageService {

        @Override
        // TransactionalMessageServiceImpl#prepareMessage
        public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
            return transactionalMessageBridge.putHalfMessage(messageInner);
        }

        @Override
        // TransactionalMessageServiceImpl#check
        public void check(long transactionTimeout, int transactionCheckMax, AbstractTransactionalMessageCheckListener listener) {
            try {
                String topic = MixAll.RMQ_SYS_TRANS_HALF_TOPIC;
                // 获取主题 RMQ_SYS_TRANS_HALF_TOPIC 下的所有消息队列，然后依次处理
                Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
                if (msgQueues == null || msgQueues.size() == 0) {
                    log.warn("The queue of topic is empty :");
                    return;
                }
                log.info("Check topic={}, queues={}");
                for (MessageQueue messageQueue : msgQueues) {
                    long startTime = System.currentTimeMillis();
                    // 根据事务消息队列获取与之对应的主题为 RMQ_SYS_TRANS_OP_HALF_TOPIC 消息队列，其实就是获取已经被处理过的（Commit or Rollback）
                    // 消息的消息队列
                    MessageQueue opQueue = getOpQueue(messageQueue);
                    long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);
                    long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                    log.info("Before check, the queue={} msgOffset={} opOffset={}");
                    if (halfOffset < 0 || opOffset < 0) {
                        log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue");
                        continue;
                    }

                    List<Long> doneOpOffset = new ArrayList<>();
                    HashMap<Long, Long> removeMap = new HashMap<>();
                    // fillOpRemoveMap 主要的作用是根据当前的处理进度依次从已经处理队列中拉取 32 条消息，方便判断当前
                    // 消息是否已经处理过（Commit or Rollback），如果处理过则无须再次发送发送事务状态回查请求，
                    // 避免重复发送事务回查请求，在事务消息的处理过程中，涉及如下两个主题:
                    // RMQ_SYS_TRANS_HALF_TOPIC:prepare 消息的主题，事务消息首先进入到该主题
                    // RMQ_SYS_TRANS_OP_HALF_TOPIC:当消息服务器收到事务消息的提交或者回滚请求后，会将消息存储在该主题下
                    // removeMap 中的消息表示已经被处理过的消息
                    // Read op message, parse op message, and fill removeMap
                    PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                    if (null == pullResult) {
                        log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null");
                        continue;
                    }
                    // single thread
                    // 获取空消息的次数
                    int getMessageNullCount = 1;
                    // 当前处理 RMQ_SYS_TRANS_HALF_TOPIC#queueId 的最新进度
                    long newOffset = halfOffset;
                    // i 当前处理消息队列的偏移量，其主题依然为 RMQ_SYS_TRANS_HALF_TOPIC
                    long i = halfOffset; // @1
                    while (true) {
                        // RocketMQ 为待检测主题 RMQ_SYS_TRANS_HALF_TOPIC 的每个队列做事务状态回查，一次最多不超过 60 秒
                        if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) { // @2
                            log.info("Queue={} process time reach max={}");
                            break;
                        }

                        // 如果该消息已经被处理（Commit or Rollback），则继续处理下一条消息
                        if (removeMap.containsKey(i)) {  // @3
                            log.info("Half offset {} has been committed/rolled back");
                            removeMap.remove(i);
                        } else {
                            // 根据消息队列偏移量 i 从消费队列中获取消息
                            GetResult getResult = getHalfMsg(messageQueue, i); // @4
                            MessageExt msgExt = getResult.getMsg();
                            // 从待处理任务队列中拉取消息，如果没有拉取到消息，则根据允许重复次数进行操作，默认重试一次
                            if (msgExt == null) {  // @5
                                // 如果超过重试次数，直接跳出，结束该消息队列的事务状态回查，MAX_RETRY_COUNT_WHEN_HALF_NULL 的值为 1
                                if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                    break;
                                }
                                // 如果是由于没有新的消息而返回空，则结束该消息队列的事务状态回查
                                if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                    log.info("No new msg, the miss offset={} in={}, continue check={}, pull result={}");
                                    break;
                                // 重新拉取
                                } else {
                                    log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}");
                                    i = getResult.getPullResult().getNextBeginOffset();
                                    newOffset = i;
                                    continue;
                                }
                            }

                            // 判断该消息是否需要丢弃 discard 或者跳过
                            // 超过最大重试次数，则丢弃消息；如果事务消息超过文件的过期时间，则跳过消息
                            if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {  // @6
                                listener.resolveDiscardMsg(msgExt);
                                newOffset = i + 1;
                                i++;
                                continue;
                            }

                            if (msgExt.getStoreTimestamp() >= startTime) {
                                log.info("Fresh stored. the miss offset={}, check it later, store={}");
                                break;
                            }

                            // 消息已经存储的时间，当前系统时间减去消息存储的时间戳
                            long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();  // @7
                            // checkImmunityTime 变量的意义是，应用程序在发送事务消息之后，事务不会马上提交，会执行一段时间，在这段时间内，
                            // RocketMQ 事务没有提交，故不应该在这个时间段内向应用程序发送回查请求
                            long checkImmunityTime = transactionTimeout;
                            String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                            if (null != checkImmunityTimeStr) {  // @8
                                // 如果用户配置了上面的 PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS 属性的话，
                                // 那么 checkImmunityTime = checkImmunityTimeStr * 1000，否则 checkImmunityTime = transactionTimeout
                                checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                                // 只有超过了 checkImmunityTime 时间，才会发送回查消息进行回查请求
                                if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                    if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt, checkImmunityTime)) {
                                        newOffset = i + 1;
                                        i++;
                                        continue;
                                    }
                                }
                            } else {  // @9
                                // 如果当前消息的 valueOfCurrentMinusBorn 小于 checkImmunityTime，那就说明回查的时间还没到，
                                // 并且消息队列之后的消息回查的时间也还没有到，因为之后的消息 getBornTimestamp 更大，算出来的 
                                // valueOfCurrentMinusBorn 值只会更小
                                if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                    log.info("New arrived, the miss offset={}, check it later checkImmunity={}, born={}");
                                    break;
                                }
                            }
                            List<MessageExt> opMsg = pullResult.getMsgFoundList();
                            // 判断是否需要发送事务回查消息，具体逻辑如下：
                            // 1.如果操作队列（RMQ_SYS_TRANS_OP_HALF_TOPIC）中已经没有已处理的消息，并且已经超过了 checkImmunityTime
                            // 2.如果操作队列不为空，并且最后一条消息的存储时间已经超过了 transactionTimeout 的值，如果最后一条消息的存储时间
                            // 都已经超过 transactionTimeout 的话，那么其中的所有消息的存储时间都超过了 transactionTimeout
                            boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)
                                    || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout))
                                    || (valueOfCurrentMinusBorn <= -1);  // @10

                            if (isNeedCheck) {
                                // 如果需要发送事务状态回查消息，则先将消息再次发送到 RMQ_SYS_TRANS_HALF_TOPIC 主题中，
                                // 发送成功则返回 true，否则返回 false

                                /**
                                 * putBackHalfMsgQueue 方法将消息再次发送到 RMQ_SYS_TRANS_HALF_TOPIC 主题的消息队列中，
                                 * resolveHalfMsg 则发送具体的事务回查命令，使用线程池来异步发送回查消息，为了回查消费进度
                                 * 保存的简化，只要发送了回查消息，当前回查进度会向前推动，如果回查失败，上一步骤 putBackHalfMsgQueue
                                 * 新增的消息将可以再次发送回查消息，那如果回查消息发送成功，会不会下一次又重复发送回查消息呢？
                                 * 这个可以根据 OP 队列中的消息来判断是否重复，如果回查消息发送成功并且消息服务器完成提交或者回滚
                                 * 操作，这条消息会发送到 OP 队列，然后首先会通过 fillOpRemoveMap 根据处理进度获取一批已处理的消息，
                                 * 来与消息判断是否重复，由于 fillOpRemoveMap 一次只拉 32 条消息，那又如何保证一定只能拉取到与
                                 * 当前消息的处理记录呢？其实就是通过代码 @10，如果此批消息最后一条未超过事务延迟消息，则继续
                                 * 拉取更多消息进行判断 @12 和 @14，OP 队列也会随着回查进度的推进而推进
                                 */

                                if (!putBackHalfMsgQueue(msgExt, i)) {
                                    continue; // @11
                                }
                                listener.resolveHalfMsg(msgExt);
                            } else {
                                // 如果无法判断是否发送回查消息，则加载更多的已处理消息进行筛选
                                pullResult = fillOpRemoveMap(removeMap, opQueue, pullResult.getNextBeginOffset(), halfOffset, doneOpOffset); // @12
                                log.info("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}");
                                continue;
                            }
                        }
                        newOffset = i + 1;
                        i++;
                    }
                    // 保存 prepare 消息队列的回查进度
                    if (newOffset != halfOffset) {   // @13
                        transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                    }
                    // 保存处理队列 op 的进度
                    long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                    if (newOpOffset != opOffset) {  // @14
                        transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Check error", e);
            }

        }

        private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
            long checkImmunityTime;
    
            checkImmunityTime = getLong(checkImmunityTimeStr);
            if (-1 == checkImmunityTime) {
                checkImmunityTime = transactionTimeout;
            } else {
                checkImmunityTime *= 1000;
            }
            return checkImmunityTime;
        }

        private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
            String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
            int checkTime = 1;
            if (null != checkTimes) {
                checkTime = getInt(checkTimes);
                // 如果该消息回查的次数超过允许的最大回查次数，则该消息将被丢弃，即事务消息提交失败
                if (checkTime >= transactionCheckMax) {
                    return true;
                // 每回查一次，消息属性 PROPERTY_TRANSACTION_CHECK_TIMES 中增 1，默认最大回查次数为 5 次
                } else {
                    checkTime++;
                }
            }
            msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
            return false;
        }

        private boolean needSkip(MessageExt msgExt) {
            long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
            // 如果事务消息超过文件的过期时间，默认为 72 小时，则跳过该消息
            if (valueOfCurrentMinusBorn > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
                * 3600L * 1000) {
                log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}");
                return true;
            }
            return false;
        }
    

    }

    public class TransactionalMessageUtil {
        public static final String REMOVETAG = "d";
        public static Charset charset = Charset.forName("utf-8");
    }

    public class TransactionalMessageBridge {
        // TransactionalMessageBridge#putHalfMessage
        public PutMessageResult putHalfMessage(MessageExtBrokerInner messageInner) {
            return store.putMessage(parseHalfMessageInner(messageInner));
        }

        // TransactionalMessageBridge#parseHalfMessageInner
        private MessageExtBrokerInner parseHalfMessageInner(MessageExtBrokerInner msgInner) {
            // 这里是事务消息与非事务消息的主要区别，如果是事务消息，则备份消息的原主题与原消息队列 id，然后将主题变更为
            // RMQ_SYS_TRANS_HALF_TOPIC，消息队列设置为 0，然后消息按照普通消息存储在 CommitLog 文件，从而转发到 RMQ_SYS_TRANS_HALF_TOPIC
            // 主题对应的消息消费队列。也就是说，事务消息在未提交之前不会存入消息原有主题的消息队列，自然也就不会被消费者消费。
            // 既然变更了主题，RocketMQ 通常会采用定时任务（单独的线程）去消费该主题，然后将该消息在满足特定条件下恢复消息主题，
            // 进而被消费者消费。
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC, msgInner.getTopic());
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msgInner.getQueueId()));
            msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), MessageSysFlag.TRANSACTION_NOT_TYPE));
            msgInner.setTopic(TransactionalMessageUtil.buildHalfTopic());
            msgInner.setQueueId(0);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            return msgInner;
        }
    }

    /**
     * EndTransaction processor: process commit and rollback message
     */
    public class EndTransactionProcessor implements NettyRequestProcessor {

        @Override
        public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
            final RemotingCommand response = RemotingCommand.createResponseCommand(null);
            final EndTransactionRequestHeader requestHeader = (EndTransactionRequestHeader) request.decodeCommandCustomHeader(EndTransactionRequestHeader.class);
            LOGGER.info("Transaction request:{}", requestHeader);
            if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
                response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
                LOGGER.warn("Message store is slave mode, so end transaction is forbidden. ");
                return response;
            }

            // EndTransactionProcessor#processRequest
            if (requestHeader.getFromTransactionCheck()) {
                switch (requestHeader.getCommitOrRollback()) {
                // 可以看出如果本地事务的处理结果为 UNKNOW 时，不做任何处理，而是直接返回
                // 后面会通过事务状态的定时回查以期得到发送端明确的事务操作（提交事务或者回滚事务）
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, but it's pending status.");
                    return null;
                }

                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer commit the message.");
                    break;
                }

                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer rollback the message.");
                    break;
                }
                default:
                    return null;
                }
            } else {
                switch (requestHeader.getCommitOrRollback()) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message,  and it's pending status.");
                    return null;
                }

                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    break;
                }

                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message, rollback the message.");
                    break;
                }
                default:
                    return null;
                }
            }

            // EndTransactionProcessor#processRequest
            OperationResult result = new OperationResult();
            // 如果结束事务动作为提交事务，则执行提交事务逻辑
            if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == requestHeader.getCommitOrRollback()) {
                // 1.首先从结束事务请求命令中获取消息的物理偏移量（commitlogOffset）
                result = this.brokerController.getTransactionalMessageService().commitMessage(requestHeader);
                if (result.getResponseCode() == ResponseCode.SUCCESS) {
                    RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                    if (res.getCode() == ResponseCode.SUCCESS) {
                        // 2.恢复消息的主题、消费队列，构建新的消息对象
                        MessageExtBrokerInner msgInner = endMessageTransaction(result.getPrepareMessage());
                        msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), requestHeader.getCommitOrRollback()));
                        msgInner.setQueueOffset(requestHeader.getTranStateTableOffset());
                        msgInner.setPreparedTransactionOffset(requestHeader.getCommitLogOffset());
                        msgInner.setStoreTimestamp(result.getPrepareMessage().getStoreTimestamp());
                        // 3.然后将消息再次存储在 commitlog 文件中，此时的消息主题则为业务方发送的消息，被转发到对应的消息消费队列，
                        // 供消息消费者消费
                        RemotingCommand sendResult = sendFinalMessage(msgInner);
                        if (sendResult.getCode() == ResponseCode.SUCCESS) {
                            // 4.消息存储后，删除 prepare 消息，其实现方法并不是真正的删除，而是将 prepare 消息存储到 RMQ_SYS_TRANS_OP_HALF_TOPIC
                            // 主题消息队列中，表示该事务消息（prepare 状态的消息）已经处理过（提交或者回滚），为未处理的事务进行事务回查提供查找依据
                            this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                        }
                        return sendResult;
                    }
                    return res;
                }
            } else if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == requestHeader.getCommitOrRollback()) {
                // 事务回滚与提交的唯一差别是无须将消息恢复原主题，直接删除 prepare 消息即可，同样是将预处理消息存储在 RMQ_SYS_TRANS_OP_HALF_TOPIC
                // 主题中，表示已经处理过该消息
                result = this.brokerController.getTransactionalMessageService().rollbackMessage(requestHeader);
                if (result.getResponseCode() == ResponseCode.SUCCESS) {
                    RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                    if (res.getCode() == ResponseCode.SUCCESS) {
                        this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                    }
                    return res;
                }
            }
            response.setCode(result.getResponseCode());
            response.setRemark(result.getResponseRemark());
            return response;
        }

    }

    public class TransactionalMessageCheckService extends ServiceThread {
        @Override
        public void run() {
            log.info("Start transaction check service thread!");
            long checkInterval = brokerController.getBrokerConfig().getTransactionCheckInterval();
            while (!this.isStopped()) {
                this.waitForRunning(checkInterval);
            }
            log.info("End transaction check service thread!");
        }

        @Override
        // TransactionalMessageCheckService#onWaitEnd
        protected void onWaitEnd() {
            // 事务的过期时间，只有当消息的存储时间加上过期时间大于系统当前时间时，才对消息执行事务状态回查，否则在下一次周期中
            // 执行事务回查操作
            long timeout = brokerController.getBrokerConfig().getTransactionTimeOut();
            // 事务回查最大检测次数，如果超过最大检测次数还是无法获知消息的事务状态，RocketMQ 将不会继续对消息进行事务状态回查，
            // 而是直接丢弃即相当于回滚事务
            int checkMax = brokerController.getBrokerConfig().getTransactionCheckMax();
            long begin = System.currentTimeMillis();
            log.info("Begin to check prepare message, begin time:{}", begin);
            this.brokerController.getTransactionalMessageService().check(timeout, checkMax, this.brokerController.getTransactionalMessageCheckListener());
            log.info("End to check prepare message, consumed time:{}", System.currentTimeMillis() - begin);
        }
    }

    public class ClientRemotingProcessor implements NettyRequestProcessor {

        @Override
        public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
            switch (request.getCode()) {
            case RequestCode.CHECK_TRANSACTION_STATE:
                return this.checkTransactionState(ctx, request);
            case RequestCode.NOTIFY_CONSUMER_IDS_CHANGED:
                return this.notifyConsumerIdsChanged(ctx, request);
            case RequestCode.RESET_CONSUMER_CLIENT_OFFSET:
                return this.resetOffset(ctx, request);
            case RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT:
                return this.getConsumeStatus(ctx, request);

            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return this.getConsumerRunningInfo(ctx, request);

            case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                return this.consumeMessageDirectly(ctx, request);
            default:
                break;
            }
            return null;
        }

        public RemotingCommand checkTransactionState(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
            final CheckTransactionStateRequestHeader requestHeader = (CheckTransactionStateRequestHeader) request.decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class);
            final ByteBuffer byteBuffer = ByteBuffer.wrap(request.getBody());
            final MessageExt messageExt = MessageDecoder.decode(byteBuffer);
            if (messageExt != null) {
                String transactionId = messageExt.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                if (null != transactionId && !"".equals(transactionId)) {
                    messageExt.setTransactionId(transactionId);
                }
                final String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
                if (group != null) {
                    MQProducerInner producer = this.mqClientFactory.selectProducer(group);
                    if (producer != null) {
                        final String addr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                        producer.checkTransactionState(addr, messageExt, requestHeader);
                    } else {
                        log.debug("checkTransactionState, pick producer by group[{}] failed", group);
                    }
                } else {
                    log.warn("checkTransactionState, pick producer group failed");
                }
            } else {
                log.warn("checkTransactionState, decode message failed");
            }

            return null;
        }

    }

    public abstract class AbstractTransactionalMessageCheckListener {

        public void resolveHalfMsg(final MessageExt msgExt) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        // 异步方式发送消息回查的实现过程
                        sendCheckMessage(msgExt);
                    } catch (Exception e) {
                        LOGGER.error("Send check message error!", e);
                    }
                }
            });
        }

        public void sendCheckMessage(MessageExt msgExt) throws Exception {
            CheckTransactionStateRequestHeader checkTransactionStateRequestHeader = new CheckTransactionStateRequestHeader();
            checkTransactionStateRequestHeader.setCommitLogOffset(msgExt.getCommitLogOffset());
            checkTransactionStateRequestHeader.setOffsetMsgId(msgExt.getMsgId());
            checkTransactionStateRequestHeader.setMsgId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
            checkTransactionStateRequestHeader.setTransactionId(checkTransactionStateRequestHeader.getMsgId());
            checkTransactionStateRequestHeader.setTranStateTableOffset(msgExt.getQueueOffset());
            msgExt.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
            msgExt.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
            msgExt.setStoreSize(0);
            String groupId = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            Channel channel = brokerController.getProducerManager().getAvaliableChannel(groupId);
            if (channel != null) {
                // 向 Producer 发送回查消息
                brokerController.getBroker2Client().checkProducerTransactionState(groupId, channel, checkTransactionStateRequestHeader, msgExt);
            } else {
                LOGGER.warn("Check transaction failed, channel is null. groupId={}", groupId);
            }
        }
    }

}