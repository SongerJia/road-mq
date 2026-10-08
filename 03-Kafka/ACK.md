## 投递语义
三种语义->acks级别->幂等生产者->事务->端到端Exactly-once->面试高频题
### 三种投递语义
|语义|含义|消息结果|
|---|---|---|
|**At-most-once**|最多一次|可能丢，不会重复|
|**At-least-once**|至少一次|不丢，可能重复|
|**Exactly-once**|精确一次|不丢不重|
```
// 类比：
// 最多一次：快递丢件（丢了不管）
// 至少一次：重发到收到（可能重复）
// 精确一次：不丢不重（理想）
```
### 三种语义的实现
At-most-once 最多一次
```
// 实现：消费完立即提交 offset，不管成功与否

// 流程：
// ① 拉取消息
// ② 提交 offset（先提交）
// ③ 处理消息（可能失败）
// 处理失败 → offset 已提交 → 消息不再重发 → 丢了

// 适用：能容忍丢失（日志、统计）
```
At-least-once 最少一次 默认
```
// 实现：处理成功后再提交 offset

// 流程：
// ① 拉取消息
// ② 处理（成功）
// ③ 提交 offset
// 处理失败 → offset 未提交 → 重发 → 不丢（但可能重复）

// 适用：业务消息（配合幂等）
// 是 Kafka 默认语义
```
Exactly-once 精确一次
```
// 理想：不丢不重
// 实现：事务（幂等生产者 + 事务消费者）
// 0.11+ 支持

// 适用：支付、扣款（不能重复）
```
### acks级别 生产者确认
| acks         | 含义        | 可靠性         | 性能  |
| ------------ | --------- | ----------- | --- |
| **acks=0**   | 不等确认      | 可能丢         | 最快  |
| **acks=1**   | Leader 确认 | Leader 挂可能丢 | 中   |
| **acks=all** | ISR 全部确认  | 不丢          | 最慢  |
```
// acks=0：
// 发完不管（fire-and-forget）
// 网络断了/Leader 挂了 → 丢

// acks=1：
// Leader 写入成功即返回
// 但 Leader 挂了没同步 → 丢

// acks=all（-1）：
// ISR 中所有副本都写入才返回
// 不丢（配合 min.insync.replicas）

// 生产配置：
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.RETRIES_CONFIG, 3);       // 重试
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  // 幂等
```
### 幂等生产者
```
// 幂等生产者（enable.idempotence=true，0.11+）
// 解决"重试导致的重复"

// 问题：
// 生产者发送超时 → 重试 → 消息可能写两次 → 重复！

// 幂等解决：
// ① 每条消息带序号（sequence）
// ② Broker 检测重复序号 → 去重
// ③ 网络重试时不会重复写入

// 条件：
// ① acks=all
// ② enable.idempotence=true

// 注意：幂等只解决"生产端重试重复"
// 不解决"消费端重复消费"（要事务/幂等消费）
```
原理
```
// 每个分区：Producer 维护单调递增的序号
// Broker 记录已处理的序号
// 序号重复 → 拒绝（去重）
// 断连重连后序号校验（PID 变化要处理）
```
### 事务
事务消息
```
// 0.11+ 支持跨分区事务
// ① 初始化事务
producer.initTransactions();

// ② 开启事务
producer.beginTransaction();

try {
    // ③ 发送多条消息（多个分区）
    producer.send(new ProducerRecord<>("topic1", "a"));
    producer.send(new ProducerRecord<>("topic2", "b"));

    // ④ 提交
    producer.commitTransaction();
} catch (Exception e) {
    // ⑤ 回滚
    producer.abortTransaction();
}

// 效果：跨分区要么全成功要么全失败
// 类似"跨分区原子写"
```
事务实现原理
```
// ① 事务协调器（Transaction Coordinator）
// ② __transaction_state 主题（事务状态）
// ③ 两阶段提交（precommit → commit）
// ④ 事务标记写入消息

// 代价：性能损耗（协调 + 状态记录）
```
### 端到端Exactly-once 完整链路
```
// 生产端幂等：解决"生产重复"
// 消费端还需要：解决"消费重复"

// 消费端重复的场景：
// ① 处理成功 → 提交 offset 前崩溃 → 重发 → 重复
// ② Rebalance → 重新消费
```
消费端幂等的两种方案
```
// 方案一：业务幂等（常用）
// 处理消息前先查"是否已处理"（去重表）
// 简单、通用

// 方案二：消费事务（Kafka 事务）
// 消费 + 处理 + 提交 offset 在一个事务里
// "读已提交"（read_committed）隔离

// 方案二配置：
props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
// 只能读已提交事务的消息
// 配合事务消费实现端到端精确一次
```
**Q:** "Kafka 怎么实现 Exactly-once？" 
**A:** "分三层：① 生产者幂等（enable.idempotence=true）+ acks=all，解决生产重试重复；② 生产者事务（跨分区原子写）；③ 消费端配合事务（read_committed 隔离 + 消费事务），实现端到端精确一次。但注意：Exactly-once 有性能代价，一般业务用 At-least-once + 业务幂等就够了。"
### 面试高频题
#### 题目1：三种投递语义
```
// 最多一次（可能丢）、至少一次（可能重）、精确一次（不丢不重）
```
#### 题目2：acks的三个级别
```
// 0（不管）、1（Leader）、all（ISR 全部）
// all 不丢
```
#### 题目3：幂等生产者解决了什么
```
// 生产重试导致重复
// enable.idempotence + acks=all
```
#### 题目4：幂等和事务区别
```
// 幂等：单分区去重（重试）
// 事务：跨分区原子（全部或全不）
```
#### 题目5：消费端怎么防重复
```
// 业务幂等（去重表）
// 或消费事务（read_committed）
```
#### 题目6：生产用什么配置
```
// acks=all + retries + enable.idempotence
```


