public class RocketmqMessageStoreAnalysisTwo{

    public class ConsumeQueue{

        /**
         * ConsumeQueue 文件
         * 
         * RocketMQ 基于主题订阅模式实现消息消费，消费者关心的是一个主题下的所有消息，但由于同一主题的消息不连续地存储在 commitlog 文件中，
         * 试想一下如果消息消费者直接从消息存储文件（commitlog）中去遍历查找订阅主题下的消息，效率将极其低下，RocketMQ 为了适应消息消费的检索需求，
         * 设计了消息消费队列文件（Consumequeue），该文件可以看成是 Commitlog 关于消息消费的“索引”文件， consumequeue 的第一级目录为消息主题，
         * 第二级目录为主题的消息队列。
         * 
         * 为了加速 ConsumeQueue 消息条目的检索速度与节省磁盘空间，每一个 Consumequeue 条目不会存储消息的全量信息，其存储的信息为：
         * 
         * commitlog offset(8个字节) | size(4个字节) | tag hashcode(8个字节)
         * 
         * 单个 ConsumeQueue 文件中默认包含 30 万个条目，单个文件的长度为 30w 20 字节，单个 ConsumeQ eue 文件可以看出是一个 ConsumeQueue 条目的数组，
         * 其下标为 ConsumeQueue 的逻辑偏移量，消息消费进度存储的偏移量，即逻辑偏移量。 ConsumeQueue 即为 Commitlog 文件的索引文件， 其构建机制是当消息到达 
         * Commitlog 文件后，由专门的线程产生消息转发任务，从而构建消息消费队列文件与索引文件。
         */

        /**
         * IndexFile 文件
         * 
         * 消息消费队列 ConsumeQueue 是 rocketmq 专门为消息订阅构建的索引文件，提高根据【主题】与【消息队列】检索消息的速度。除此之外，rcoketmq 
         * 引入了 Hash 索引机制专门为【消息】建立索引，也就是 IndexFile。IndexFile 的结构和 HashMap 比较像，也包括两个基本点，Hash 槽与 Hash 
         * 冲突的链表结构。
         * 
         * IndexFile 的结构分为以下 3 个部分：
         * 
         * IndexHead（40 个字节）、hash 槽（500万个）、index 条目（2000万个）
         * 
         * IndexHead：包含 40 个字节，记录该 IndexFile 的统计信息，包含的具体信息在 IndexHead 对象属性中
         * hash 槽列表：一个 IndexFile 默认包含 500 万个 Hash 槽，每个 Hash 槽存储的是落在该 Hash 槽的最新的 Index 的索引（落在同一个 hash 槽的 Index 索引会形成
         * 链表结构）。每个 hash 槽是大概 4 个字节
         * Index 条目列表：默认一个索引文件包含 2000 万个条目，每一个 Index 条目（总共 20 个字节）结构如下：
         * hashcode：key 的 hashcode
         * phyoffset：消息对应的物理偏移量
         * timedif：该消息存储时间与第一条消息的时间戳的差值，小于 0 该消息无效
         * prelndexNo：该条目的前一条记录的 Index 索引，当出现 hash 冲突时，构建链表结构
         */

        /**
         * checkpoint 的作用是记录 Commitlog、ConsumeQueue、Index 文件的刷盘时间点，固定长度为 4k ，其中只用该文件的前面的 24 个字节
         * physicMsgTimestamp: commitlog 文件刷盘时间点
         * logicsMsgTimestamp: ConsumeQueue 文件刷盘时间点
         * indexMsgTimestamp: 索引文件刷盘时间点
         */

        public class ConsumeQueue {
            // CQ_STORE_UNIT_SIZE 表示在一个 ConsumeQueue 文件中，每个条目的大小固定为 20 字节
            public static final int CQ_STORE_UNIT_SIZE = 20;
            // 一个 ConsumeQueue 文件的大小
            private final int mappedFileSize;

            // 根据 startIndex 来获取消息消费队列的条目
            public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
                int mappedFileSize = this.mappedFileSize;
                // startIndex * 20 得到在 ConsumeQueue 文件中的逻辑偏移量
                long offset = startIndex * CQ_STORE_UNIT_SIZE;

                // 如果该 offset 小于 minLogicOffset，则返回 null，说明该消息已被删除；如果大于 minLogicOffset，则根据偏移定位到具体的物理文件，
                // 然后通过 offset 与物理文件大小取模获取在该文件内的偏移，从而从偏移量开始连续读取 20 个字节即可
                if (offset >= this.getMinLogicOffset()) {
                    MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
                    if (mappedFile != null) {
                        SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
                        return result;
                    }
                }
                return null;
            }
        }

        public class IndexHeader {
            public static final int INDEX_HEADER_SIZE = 40;
            private static int beginTimestampIndex = 0;
            private static int endTimestampIndex = 8;
            private static int beginPhyoffsetIndex = 16;
            private static int endPhyoffsetIndex = 24;
            private static int hashSlotcountIndex = 32;
            private static int indexCountIndex = 36;
            private final ByteBuffer byteBuffer;
            private AtomicLong beginTimestamp = new AtomicLong(0);
            private AtomicLong endTimestamp = new AtomicLong(0);
            private AtomicLong beginPhyOffset = new AtomicLong(0);
            private AtomicLong endPhyOffset = new AtomicLong(0);
            private AtomicInteger hashSlotCount = new AtomicInteger(0);
            private AtomicInteger indexCount = new AtomicInteger(1);
        }

        public class IndexFile {

            private static int hashSlotSize = 4;
            private static int indexSize = 20;
            private static int invalidIndex = 0;
            private final int hashSlotNum;

            // IndexFile 最多允许的索引条目个数
            private final int indexNum;
            private final MappedFile mappedFile;
            private final FileChannel fileChannel;
            private final MappedByteBuffer mappedByteBuffer;
            private final IndexHeader indexHeader;

            public IndexFile(final String fileName, final int hashSlotNum, final int indexNum, final long endPhyOffset,
                    final long endTimestamp) throws IOException {

                int fileTotalSize = IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
                this.mappedFile = new MappedFile(fileName, fileTotalSize);
                this.fileChannel = this.mappedFile.getFileChannel();
                this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
                this.hashSlotNum = hashSlotNum;
                this.indexNum = indexNum;

                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                this.indexHeader = new IndexHeader(byteBuffer);

                if (endPhyOffset > 0) {
                    this.indexHeader.setBeginPhyOffset(endPhyOffset);
                    this.indexHeader.setEndPhyOffset(endPhyOffset);
                }

                if (endTimestamp > 0) {
                    this.indexHeader.setBeginTimestamp(endTimestamp);
                    this.indexHeader.setEndTimestamp(endTimestamp);
                }
            }

            public int indexKeyHashMethod(final String key) {
                int keyHash = key.hashCode();
                int keyHashPositive = Math.abs(keyHash);
                if (keyHashPositive < 0)
                    keyHashPositive = 0;
                return keyHashPositive;
            }

            /**
             * 
             * @param key 发送的消息中的 key 值
             * @param phyOffset 消息在 commitlog 中的物理偏移量
             * @param storeTimestamp 
             * @return
             */
            public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
                // IndexHeader 对象中的 indexCount 表示已经使用的索引条目个数，如果大于 IndexFile 最多允许的索引条目个数，就直接返回 false
                if (this.indexHeader.getIndexCount() < this.indexNum) {
                    // 获取到 key 对应的 hashcode 值
                    int keyHash = indexKeyHashMethod(key);
                    // 根据 keyHash 取模得到对应的 hash 槽的下标
                    int slotPos = keyHash % this.hashSlotNum;
                    // 计算出这个新添加的条目在 hash 槽对应的偏移量，也就是：IndexHeader 头部（40 字节） + hash 槽的下标 * hash 槽的大小（4 字节）
                    // hash 槽中存储的值为最新的 key 对应的 Index 条目在 Index 条目列表中的下标值
                    int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;
        
                    FileLock fileLock = null;
        
                    try {
                        // 读取 hash 槽中存储的数据值，每个 hash 槽的大小刚好是 4 个字节，
                        int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                        if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                            slotValue = invalidIndex;
                        }
        

                        long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                        timeDiff = timeDiff / 1000;
                        if (this.indexHeader.getBeginTimestamp() <= 0) {
                            timeDiff = 0;
                        } else if (timeDiff > Integer.MAX_VALUE) {
                            timeDiff = Integer.MAX_VALUE;
                        } else if (timeDiff < 0) {
                            timeDiff = 0;
                        }

                        /**
                         * 这里是 Hash 冲突链式解决方案的关键实现， Hash 槽中存储的是在这个槽中的所对应的最新的 Index 条目的下标，新的 Index 条目的最后 4 个字节
                         * 存储的是相同 hash 槽中上一个条目的 Index 下标。如果 Hash 槽中存储的值为 0 或大于当前 IndexFile 最大条目数，表示该 Hash 槽当前并没有与之对应的 Index 
                         * 条目。值得关注的是 IndexFile 条目中存储的不是消息的原始 key 值而是消息属性 key 的 HashCode，在根据 key 查找时需要根据消息物理偏移量找到消息
                         * 进而再验证消息 key 的值，之所以只存储 HashCode 不存储具体的 key 是为了将 Index 条目设计为定长结构，才能方便地检索与定位条目。
                         */

                        // 计算这个新添加的条目在 Index 条目列表中的偏移量，用来往里面存储 Index 的条目值
                        int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize + this.indexHeader.getIndexCount() * indexSize;
                        // 依次将消息中 key 值计算出来的 hashcode，消息物理偏移量，时间差值，以及当前的 hash 槽的值存入 MappedByteBuffer 中
                        this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                        this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                        this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                        this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                        // 将当前条目在 Index 条目列表中的下标存入到 hash 槽中
                        this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
        
                        if (this.indexHeader.getIndexCount() <= 1) {
                            this.indexHeader.setBeginPhyOffset(phyOffset);
                            this.indexHeader.setBeginTimestamp(storeTimestamp);
                        }
        
                        this.indexHeader.incHashSlotCount();
                        this.indexHeader.incIndexCount();
                        this.indexHeader.setEndPhyOffset(phyOffset);
                        this.indexHeader.setEndTimestamp(storeTimestamp);
        
                        return true;
                    } catch (Exception e) {
                        log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
                    } finally {
                        if (fileLock != null) {
                            try {
                                fileLock.release();
                            } catch (IOException e) {
                                log.error("Failed to release the lock", e);
                            }
                        }
                    }
                } else {
                    log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount() + "; index max num = " + this.indexNum);
                }
        
                return false;
            }

        }

        /**
         * 根据索引 key 查找查找消息
         * @param phyOffsets 查找到的消息物理偏移量
         * @param key 索引 key
         * @param maxNum 本次查找最大消息条数
         * @param begin
         * @param end
         * @param lock
         */
        public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum, final long begin, final long end, boolean lock) {

            if (this.mappedFile.hold()) {
                // 根据 key 算出 key 的 hashcode，然后 keyHash 对 hash 槽数量取余定位到 hashcode 对应的 hash 槽下标，hashcode 对应的 hash 槽的偏移量为
                // IndexHeader 头部（40字节）加上下标乘以每个 hash 槽的大小（4字节）
                int keyHash = indexKeyHashMethod(key);
                int slotPos = keyHash % this.hashSlotNum;
                int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

                FileLock fileLock = null;
                try {
                    if (lock) {
                    }

                    // slotValue 为 slotPos 这个 hash 槽中保存的最新的一个 Item 条目在 Item 条目索引的下标值
                    int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

                    if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount() || this.indexHeader.getIndexCount() <= 1) {
                    } else {
                        // 由于会存在 hash 冲突，根据 slotValue 定位该 hash 槽最新的一个 Item 条目在 Item 条目列表中的下标，将存储的物理偏移量加入到
                        // phyOffsets 中，然后继续验证 Item 条目中存储的下一个 Index 的下标
                        for (int nextIndexToRead = slotValue;;) {
                            if (phyOffsets.size() >= maxNum) {
                                break;
                            }

                            int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize + nextIndexToRead * indexSize;
                            int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                            long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                            long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                            int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                            if (timeDiff < 0) {
                                break;
                            }

                            timeDiff *= 1000L;

                            long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                            boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                            if (keyHash == keyHashRead && timeMatched) {
                                phyOffsets.add(phyOffsetRead);
                            }

                            if (prevIndexRead <= invalidIndex || prevIndexRead > this.indexHeader.getIndexCount()
                                    || prevIndexRead == nextIndexToRead || timeRead < begin) {
                                break;
                            }

                            nextIndexToRead = prevIndexRead;
                        }
                    }
                } catch (Exception e) {
                    log.error("selectPhyOffset exception ", e);
                } finally {
                    if (fileLock != null) {
                        try {
                            fileLock.release();
                        } catch (IOException e) {
                            log.error("Failed to release the lock", e);
                        }
                    }

                    this.mappedFile.release();
                }
            }
        }

    }

}