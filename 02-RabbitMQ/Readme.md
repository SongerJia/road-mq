> **模块边界**：Kafka / RocketMQ 本体 → 各自模块；Spring Cloud Stream 的 Binder → Spring Cloud 模块。本模块覆盖 RabbitMQ 的 AMQP 模型、可靠性、集群、死信延时与运维。

**① RabbitMQ 总览**

1. 是什么、特点（Erlang / AMQP）
2. 与 Kafka / RocketMQ 对比、单队列单节点瓶颈
3. 核心概念
4. 应用场景
5. 版本演进（3.9 镜像队列弃用、4.0 移除项与默认 Quorum）

**② 核心概念与 AMQP 模型** 6. Exchange 7. Queue 8. Binding（routing key / binding key） 9. Virtual Host 的隔离与权限模型（configure / write / read） 10. Connection vs Channel（复用、线程安全、Channel 级异常） 11. Connection 心跳机制 12. 消息流转（Producer → Exchange → Binding → Queue → Consumer） 13. 消息顺序性（单队列单消费者才有序）

**③ Exchange 类型** 14. direct 15. topic（`*` 与 `#` 通配符） 16. fanout 17. headers 18. 默认 exchange

**④ 生产者可靠性** 19. Publisher Confirm（同步 / 异步两种模式） 20. 事务机制 21. mandatory / immediate 与 Return 回调 22. 消息持久化三层（交换机 / 队列 / 消息，缺一不可） 23. 备用交换机（alternate exchange）

**⑤ 消费者可靠性** 24. 消费确认（ack / nack） 25. 拒绝与重回队列 26. 消费限流（prefetch / Qos 机制） 27. 幂等

**⑥ Broker 可靠性与消息丢失** 28. 持久化（存储结构：消息索引 / 消息存储 / 队列索引） 29. 镜像队列与 Quorum 队列的对比（镜像队列弃用） 30. Quorum 队列（3.8+ / Raft / 至少 3 节点 / 特性限制） 31. 惰性队列 32. 内存与磁盘告警、流控（flow control） 33. **消息丢失三环节汇总（生产 / Broker / 消费）与对策** 34. **消息堆积与内存告警的处置（扩消费者 / 惰性队列 / 限流）**

**⑦ 高可用与集群** 35. 集群模式与 Erlang 运行模型（进程 / 调度器 / 内存敏感） 36. 节点类型（磁盘 / 内存） 37. 网络分区与脑裂：ignore / pause_minority / autoheal 策略取舍 38. 负载均衡 39. 插件生态：Shovel 与 Federation

**⑧ 死信与延时** 40. 死信队列与产生死信的三种条件 41. TTL：消息 TTL vs 队列 TTL 42. 延时队列三方案对比（TTL + DLX 的队头阻塞 / 延时插件 / 定时扫描）

**⑨ 其他特性** 43. 优先级队列 44. 单活动消费者 45. Streams（3.9+）

**⑩ 运维、集成与规范** 46. 管理界面与 CLI 及监控 47. 性能调优与高性能实践（连接复用 / 批量 ack / 大消息拆分） 48. Spring AMQP 集成 49. 常见坑清单（自动 ack 丢消息 / prefetch 过大倾斜 / 队头阻塞 / 单队列瓶颈 / 脑裂配置错误 / 镜像队列误用）