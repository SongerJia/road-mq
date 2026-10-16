> **模块边界**：RocketMQ / RabbitMQ 本体 → 各自模块；Spring Cloud Stream 的 Binder → Spring Cloud 模块。本模块覆盖 Kafka 的存储、生产消费、副本一致性、KRaft 与生态。

**① Kafka 总览与定位**

1. 是什么、特点
2. 与 RocketMQ / RabbitMQ 对比、延迟定位（非低延迟最优解）
3. 核心概念（Producer / Consumer / Broker / Topic / Partition / Offset / Group / Replica）
4. 应用场景（日志采集 / 流处理 / 事件驱动 / CDC）
5. 版本演进（KRaft 3.3 生产可用、4.0 移除 ZooKeeper）
6. 配额（quota）与安全（SASL / SSL / ACL）

**② 架构与存储** 7. Broker 与 Controller、Controller 选举与分区 Leader 选举 8. Topic / Partition / Replica、副本因子与最小 ISR 的组合约束、分区数不可减少及影响 9. Leader / Follower / ISR、ISR 的收缩与扩展（replica.lag.time.max.ms） 10. 消息格式演进（v0 / v1 → v2 RecordBatch） 11. Segment 文件与日志段滚动（log.segment.bytes） 12. 稀疏索引 13. 数据保留与磁盘管理（retention.ms / retention.bytes）

**③ 高性能原理** 14. 顺序写与页缓存 15. 零拷贝（sendfile） 16. 批量与压缩 17. 请求处理模型（网络线程 + IO 线程） 18. 分区并行

**④ 生产者** 19. 发送流程（拦截器 → 序列化 → 分区器 → 累加器 → Sender） 20. 分区策略、key 与分区的关系 21. 消息累加器与批量 22. acks 机制（0 / 1 / all） 23. 幂等与重试、幂等原理（PID + 序列号 + Broker 去重） 24. 事务与实现（TransactionCoordinator + 事务日志 + 两阶段提交） 25. 压缩（gzip / snappy / lz4 / zstd）

**⑤ 消费者与消费组** 26. 消费组与再平衡 27. 分区分配策略（Range / RoundRobin / Sticky / CooperativeSticky） 28. Rebalance 协议演进（Eager STW vs Cooperative 增量） 29. offset 提交（自动 / 手动、同步 / 异步）与重复消费 30. `__consumer_offsets` 与 GroupCoordinator 31. `auto.offset.reset` 32. 静态成员

**⑥ 副本与一致性** 33. ISR 34. Leader 选举 35. HW 与 LEO 的区别与关系 36. acks=all + min.insync.replicas 的联合约束（acks=all 不等于不丢） 37. unclean.leader.election 与数据丢失取舍

**⑦ KRaft** 38. ZooKeeper 的作用 39. KRaft 模式与 Controller quorum

**⑧ 消息可靠性** 40. 消息丢失 41. 消息重复与幂等 42. 消息顺序（仅分区内有序） 43. Exactly-once 语义

**⑨ 运维与监控** 44. 常用命令 45. 监控指标（Lag / ISR / UnderReplicated） 46. 日志清理（delete / compact） 47. 扩容、分区再均衡与 leader 迁移（kafka-reassign-partitions） 48. 消息积压与 Lag 处理（扩分区 / 扩消费者 / 批量消费 / 跳过）

**⑩ 生态、集成与规范** 49. Kafka Connect 与 Debezium CDC 50. Kafka Streams 51. 与 Flink / Spark 的流处理集成 52. MirrorMaker 53. Schema Registry 与 Avro 54. spring-kafka（@KafkaListener / KafkaTemplate） 55. 常见坑清单（分区数不可减 / Rebalance 风暴 / 消费者数超分区闲置 / acks=1 丢数据 / 自动提交重复 / 单分区热点） 56. MQ 三件套选型矩阵（RocketMQ / RabbitMQ / Kafka 的取舍对照）