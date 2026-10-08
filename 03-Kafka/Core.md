## 核心概念
Kafka是什么->五大概念->Broker/Topic/Partition->消息流转->与RabbitMQ/RocketMQ对比->面试高频题
### Kafka是什么
Kafka：Apache开源的分布式流平台，高吞吐、可持久化、支持百万级消息/秒，主要用在大数据和日志流。
特性
```
// ① 高吞吐（百万级/秒）
// ② 持久化（消息落盘，可重复消费）
// ③ 分区（横向扩展）
// ④ 副本（高可用）
// ⑤ 消息回溯（按 offset 重新消费）
// ⑥ 支持流处理（Kafka Streams）
```
定位
```
// RabbitMQ/RocketMQ：消息中间件（解耦）
// Kafka：分布式流平台（日志、事件、大数据）

// 典型场景：
// ① 日志收集（海量日志 → 分析）
// ② 用户行为埋点
// ③ 大数据管道（Kafka → Flink/Spark）
// ④ 事件流
```
### 五大核心概念
| 概念                 | 说明              |
| ------------------ | --------------- |
| **Broker**         | Kafka 服务器节点     |
| **Topic**          | 消息的分类（逻辑主题）     |
| **Partition**      | Topic 的分区（物理分片） |
| **Producer**       | 生产者（发消息）        |
| **Consumer**       | 消费者（收消息）        |
| **Consumer Group** | 消费组（负载均衡）       |
### Broker
```
// Kafka 集群由多个 Broker 组成
// 每个 Broker 是一个 Kafka 服务进程（节点）

// 集群：3+ 个 Broker（生产）
// 数据分布在各 Broker 上
```
角色
```
// ① Controller（控制器）：
//    集群中的一个 Broker 担任
//    管理分区分配、副本选举、故障处理

// ② Leader/Follower（副本角色）：
//    每个分区的副本有 Leader 和 Follower
//    Leader 处理读写，Follower 备份

// ③ 元数据存储：
//    老版本用 ZooKeeper
//    新版本用 KRaft（自管理，2.8+）
```
### Topic和Partition
Topic
```
// 消息的逻辑分类
// 例：order-events、user-behavior、logs

// 一个 Topic 分成多个 Partition（分区）
// 类似 RocketMQ 的 Topic + Queue
```
Partition
```
// Topic 的物理分片（存储单元）
// ① 每个分区独立顺序写日志
// ② 分区是"并行度"的单元
// ③ 分区内消息有序（追加写）
// ④ 消息在分区内有序，跨分区无序

// 例：
// order-events（Topic）
// ├── partition-0（在 Broker1）
// ├── partition-1（在 Broker2）
// └── partition-2（在 Broker3）
```
为什么分区
```
// ① 并行：多个分区可并行读写（吞吐）
// ② 扩展：分区分布在不同 Broker（容量）
// ③ 有序：分区内顺序（局部有序）

// 分区数 = 并行度
// 分区多了好（并行），但太多也有管理开销
```
消息在分区内的位置
```
// 每条消息在分区内有一个 offset（偏移量）
// 类似"数组下标"，从 0 递增

// partition-0：
// [msg0][msg1][msg2][msg3]...
//  offset 0    1    2    3

// 消费者记录 offset → 知道消费到哪了
```
### 消息流转
```
Producer 发送消息
    │
    ├── ① 计算目标分区（key 哈希 / 轮询）
    │
    ├── ② 发送到分区的 Leader（Broker）
    │
    ├── ③ Leader 写入日志 + 同步给 Follower
    │
    ├── ④ 返回确认（acks 级别）
    │
    └── Consumer 消费
         ├── ① 从分区 Leader 拉取（pull）
         ├── ② 记录 offset（消费进度）
         └── ③ 处理消息
```
和RabbitMQ区别
```
// RabbitMQ：Push（长轮询）
// Kafka：Pull（消费者主动拉取）

// 拉取的好处：
// ① 消费者自己控制速度（不会压垮）
// ② 批量拉取（高效）
// ③ 重复消费容易控制（offset）
```
### 与RabbitMQ和RocketMQ对比
| 对比       | Kafka       | RocketMQ  | RabbitMQ |
| -------- | ----------- | --------- | -------- |
| **吞吐**   | 百万级 ✅       | 10 万+     | 万级       |
| **存储**   | 日志分区        | CommitLog | 队列       |
| **消费方式** | Pull ✅      | 长轮询       | Push/长轮询 |
| **顺序**   | 分区内 ✅       | 分区顺序      | 弱        |
| **重复消费** | 可重放（offset） | 可回溯       | 一般       |
| **延迟**   | 中           | 低         | 最低       |
| **适用**   | 大数据/日志 ✅    | 业务消息      | 轻量通用     |
### 面试高频题
#### 题目1：Kafka核心概念
```
// Broker、Topic、Partition、Producer、Consumer/Group
```
#### 题目2：Topic和Partition的关系
```
// Topic 逻辑分类，Partition 物理分片
// 分区是并行度和有序的单位
```
#### 题目3：为什么Kafka吞吐高
```
// 顺序写 + 分区并行 + 批量 + 零拷贝
```
#### 题目4：消息在分区里怎么定位
```
// offset（偏移量），从 0 递增
// 消费者记录 offset 消费进度
```
#### 题目5：消息是Push还是Pull
```
// Pull！消费者主动拉取
// 控制速度、批量、可回溯
```
