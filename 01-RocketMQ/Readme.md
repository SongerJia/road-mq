> **模块边界**：Kafka / RabbitMQ 本体 → 各自模块；消息驱动微服务（Spring Cloud Stream）→ Spring Cloud 模块；分布式事务的通用理论 → 分布式模块（此处只讲 RocketMQ 事务消息的实现）。本模块覆盖 RocketMQ 的存储、投递、特殊消息与运维。

**① RocketMQ 总览与选型**

1. 是什么、特点
2. 与 Kafka / RabbitMQ 对比（事务消息 / 延时消息 / 消息回溯的差异）
3. 核心概念（Producer / Consumer / Broker / NameServer / Topic / MessageQueue / Tag）
4. 应用场景（异步 / 解耦 / 削峰 / 顺序 / 事务 / 延时）
5. 版本演进（4.5 DLedger、5.x 自动切换 + Proxy + 任意延时）

**② 架构** 6. NameServer（无状态、路由注册） 7. Broker 主从、brokerId 与集群部署形态（多主多从 / 双主） 8. Producer / Consumer 组 9. Topic / MessageQueue 10. 消息存储结构概览 11. **Proxy（gRPC）与 Pop 消费（无状态消费者，5.x）**

**③ 消息发送** 12. 发送方式（同步 / 异步 / 单向） 13. 发送流程 14. 队列选择 15. 批量发送 16. 生产者重试

**④ 消息存储** 17. CommitLog（顺序写、默认 1G 文件滚动） 18. ConsumeQueue（20 字节定长索引） 19. IndexFile（哈希槽结构）与按 key 查询 20. 页缓存与 mmap、MappedFile / MappedFileQueue 21. 刷盘机制（同步 / 异步）与复制的可靠性组合 22. 主从复制（同步 / 异步） 23. 零拷贝与顺序写的性能加成

**⑤ 消息消费** 24. PushConsumer 本质：拉 + 长轮询 25. 消费模式（集群 / 广播） 26. 消费位点（本地 offset 文件 vs Broker 管理） 27. 并发消费与顺序消费、顺序消费的队列锁 28. 消息重试与死信队列（重试 16 次 → %DLQ%） 29. 消息过滤：Tag 与 SQL92 30. 消费限流与失败退避

**⑥ 顺序消息** 31. 全局顺序 vs 分区顺序 32. 顺序消费实现

**⑦ 事务消息** 33. 事务消息原理：半消息、两阶段提交 34. 回查机制（次数与间隔）与事务状态（提交 / 回滚 / 未知）

**⑧ 延时消息** 35. 延时级别（4.x 固定 18 级） 36. 任意延时（5.x）与定时消息

**⑨ 可靠性与问题处理** 37. 消息丢失：生产 / 存储 / 消费三环节与对策 38. 消息重复消费与幂等 39. 消息积压的定位与解决

**⑩ 高可用与运维** 40. 主从与故障切换 41. DLedger / Raft 42. 消息轨迹 43. ACL 权限

**⑪ 性能与线程模型** 44. Netty 网络层与业务线程池（SendMessageProcessor / PullMessageProcessor） 45. 消息序列化与压缩

**⑫ 集成与规范** 46. 与 Spring Boot 集成（rocketmq-spring-boot-starter）、常见坑清单（重复 / 顺序失效 / 延时精度 / 异步刷盘丢消息 / Tag 过滤失效 / 订阅不一致）