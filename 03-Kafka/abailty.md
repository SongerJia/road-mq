## 可靠性三要素
三要素总览->acks->min.insync.replicas->retries->完整配置->不丢vs不重->面试高频题
### 三要素总览
```
// Kafka 生产端消息不丢的三个关键配置：
// ① acks：确认级别（等谁确认）
// ② min.insync.replicas：最小同步副本数（ISR 下限）
// ③ retries：发送重试次数

// 三者配合：acks=all + min.insync + retries
// → 消息不丢
```
### acks确认级别
| acks        | 等谁确认      | 可靠性  |
| ----------- | --------- | ---- |
| **0**       | 不等（发了就完）  | 最低   |
| **1**       | Leader 确认 | 中    |
| **all（-1）** | ISR 全部确认  | ✅ 最高 |
```
// 生产推荐：
props.put(ProducerConfig.ACKS_CONFIG, "all");
// 只有 ISR 中所有副本都写入，才返回成功
// 配合 min.insync 保证"至少 N 个副本有数据"
```
为什么acks=all还不够
```
// 只有 acks=all：
// ISR = {Leader}（只有一个）也能满足
// Leader 确认就返回（其他副本没同步）
// Leader 挂了 → 丢！

// 所以还需要 min.insync.replicas 兜底
```
### min.insync.replicas 最小同步副本数
```
// Broker 配置：分区最少要有 N 个 ISR 副本
// ISR < N → 拒绝写入（宁可失败，不丢）

// 配置：
// broker 参数：min.insync.replicas=2

// 作用：
// ① 保证写入时有至少 2 个副本同步
// ② ISR 缩到 < 2 → 写入失败（生产者重试/报错）
// ③ Leader 挂了 → 还有 1 个副本有数据（不丢）
```
和生产配合
```
// 副本因子 3 + min.insync.replicas=2：
// ① 正常：3 副本在 ISR，acks=all 等 3 个确认
// ② 1 个副本挂了：ISR=2，还能写（2 >= min 2）
// ③ 2 个副本挂了：ISR=1 < 2 → 拒绝写（不丢！）

// 效果：最多容忍 1 个副本故障（3 副本）

// min.insync.replicas 不能大于副本因子
// 值越大越可靠，但可用性越低（副本少时拒绝写）
// 生产常见：副本 3 + min.insync 2
```
### retries 重试
```
// 发送失败时重试次数
// 网络抖动、瞬时故障 → 重试成功

props.put(ProducerConfig.RETRIES_CONFIG, 3);       // 重试 3 次
props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);  // 间隔 100ms
```
重试的坑
```
// ① 重试可能重复（消息写两次）
//    → 配合幂等生产者（enable.idempotence=true）解决
// ② 重试导致乱序？
//    → 幂等 + 单个请求（max.in.flight=1 或幂等模式）
// ③ 重试次数太多 → 延迟大

// 生产配置：
props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);  // 无限重试（配合幂等）
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);    // 幂等
```
### 完整可靠配置
```java
// 生产者（不丢消息标准配置）
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
props.put(ProducerConfig.ACKS_CONFIG, "all");                    // ① ISR 确认
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);       // ② 幂等（防重复）
props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);     // ③ 重试
props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);  // 幂等模式下可大于1
```
```properties
# Broker 配置（server.properties）
default.replication.factor=3        # 副本因子 3
min.insync.replicas=2              # 最小同步副本 2
unclean.leader.election.enable=false  # 不允许非 ISR 当 Leader（防丢）
```
完整不丢链路
```
生产端：acks=all + 幂等 + 重试 + min.insync=2
    ↓
Broker：副本 3 + min.insync 2 + 不允许非 ISR 选举
    ↓
消费端：手动提交 offset + 幂等处理
→ 消息不丢（最多容忍 1 副本故障）
```
### 不丢 vs 不重
| 目标       | 配置                              | 代价            |
| -------- | ------------------------------- | ------------- |
| **不丢**   | acks=all + min.insync + retries | 延迟高、可用性略降     |
| **不重**   | 幂等 + 事务 + 消费幂等                  | 性能开销          |
| **又快又稳** | acks=1 + 重试                     | 可能丢 Leader 数据 |
| **最快**   | acks=0                          | 可能丢           |
选型
```
// 金融/订单：acks=all + min.insync=2 + 幂等（不丢）
// 一般业务：acks=1 + 重试（性能可靠平衡）
// 日志/统计：acks=0（最快，容忍丢）

// 面试一句话：
// "消息不丢三要素：acks=all（ISR 全部确认）、
// min.insync.replicas（最少同步副本，ISR 缩到下限拒绝写）、
// retries + 幂等（失败重试且不重复）。
// 配合副本因子 3 + 不允许非 ISR 选举，最多容忍一个副本故障不丢。"
```
### 面试高频题
#### 题目1：消息不丢三要素
```
// acks=all + min.insync.replicas + retries/幂等
```
#### 题目2：min.insync.replicas是什么
```
// 最小同步副本数
// ISR < 阈值 → 拒绝写（宁可失败不丢）
```
#### 题目3：acks=all还不够吗
```
// 不够！ISR 只有 Leader 时 all=Leader 确认
// 要 min.insync 保证至少 N 副本
```
#### 题目4：重试会不会重复
```
// 会！配合幂等生产者解决
```
#### 题目5：生产者怎么配置
```
// acks=all + 幂等 + 无限重试 + min.insync=2
```
#### 题目6：不丢和不重哪个难
```
// 不丢容易（配置够）
// 不重难（幂等 + 事务 + 消费幂等）
```