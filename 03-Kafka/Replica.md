## 副本
副本是什么->Leader/Follower->同步流程->LEO/HW->ISR机制->故障恢复->与RocketMQ对比->面试高频题
### 副本是什么
Replica：分区的数据冗余副本，存在多个Broker上。Leader处理读写，Follwer同步备份，保证可用性。
partition-0（3 副本，分布在 3 个 Broker）：
```
┌──────────────┐
│ Broker1：副本0（Leader）│ ← 读写都走这里
├──────────────┤
│ Broker2：副本1（Follower）│ ← 备份
├──────────────┤
│ Broker3：副本2（Follower）│ ← 备份
└──────────────┘
```
### Leader和Follower
| 角色           | 职责                 |
| ------------ | ------------------ |
| **Leader**   | 处理所有读写请求（唯一入口）     |
| **Follower** | 同步 Leader 数据（备份）   |
| **ISR**      | 与 Leader 保持同步的副本集合 |
为什么只有Leader读写
```
// ① 数据一致性：只有一个写入点（不会冲突）
// ② 顺序性：单 Leader 保证有序
// ③ 简化：不需要处理并发写

// 对比 RocketMQ：
// 类似主从（Master 写，Slave 读/备份）
```
### 同步流程
```
// ① Producer 发消息给 Leader
// ② Leader 写入本地日志（追加）
// ③ Follower 从 Leader 拉取（pull，类似消费者）
// ④ Follower 写入自己的日志
// ⑤ 更新自己的 LEO（日志末端偏移量）

// 注意：Follower 用 pull（和消费者一样）
// 不是 Leader 推
```
### LEO和HW
LEO log End Offset
```
// LEO：日志末端偏移量（下一条要写入的位置）
// 每个副本有自己的 LEO

// Leader：LEO = 已写入的消息数
// Follower：LEO = 已同步的消息数

// partition-0：
// [msg0][msg1][msg2]  →  LEO = 3（下一个偏移量是 3）
```
HW High Watermark
```
// HW：高水位（所有 ISR 副本都同步到的位置）
// = 最小的 LEO（ISR 中落后的那个）

// 含义：
// ① HW 之前的消息 = 已被所有 ISR 副本同步（安全）
// ② HW 之后的消息 = 还没完全同步（可能丢）

// 消费者只能读到 HW 之前的消息！
// 保证：消费者读到的消息不会在故障后丢失
```
图解
```
Leader 日志：[0][1][2][3][4][5]
Follower1：  [0][1][2][3]      LEO=4
Follower2：  [0][1][2][3][4]   LEO=5

ISR = {Leader, Follower1, Follower2}
HW = 4（Follower1 的 LEO，最落后的）

消费者能读到 0~3（HW 之前）
4、5 虽然 Leader 有了，但消费者读不到（还没全同步）
```
为什么需要HW
```
// ① 保证消费者读到的消息"所有副本都有"（不丢）
// ② Leader 挂了 → 新 Leader 有 HW 之前的数据（不丢）
// ③ 一致性：消费者不会读到可能丢失的数据
```
### ISR机制
```
// ISR（In-Sync Replicas）：与 Leader 保持同步的副本集合
// 只有 ISR 中的副本，才有资格成为新 Leader

// 判定同步：
// ① Follower 在一定时间内（replica.lag.time.max，默认 30s）
// ② 跟上了 Leader 的 LEO（差距在允许范围）
// ③ 满足则留在 ISR，不满足移出
```
为什么需要ISR
```
// ① 排除"落后的副本"（不配当 Leader）
// ② 保证新 Leader 数据不丢（ISR 内都有同步）
// ③ 故障恢复更可靠

// 对比：
// 所有副本都能当 Leader？不行！
// 落后太多的副本当 Leader → 丢数据！
```
ISR收缩与恢复
```
// ① Follower 落后 → 从 ISR 移除（不在同步集合）
// ② Follower 追上 → 重新加入 ISR
// ③ ISR 不能空（至少 Leader 自己）
// ④ ISR 空了 → 可用 min.insync.replicas 兜底
```
### 故障恢复
Leader挂了
```
// ① 从 ISR 中选一个新 Leader
// ② 选"最新的"（LEO 最大的）
// ③ 其他 Follower 从新 Leader 同步

// 选举机制：Controller 协调（KRaft/旧 ZK）
```
数据不丢的条件
```
// ① 新 Leader 在 ISR 中（有 HW 之前数据）→ 不丢
// ② 生产者确认级别 acks 决定是否丢：
//    acks=0：不等确认（可能丢）
//    acks=1：Leader 确认（Leader 挂了可能丢）
//    acks=all：ISR 全部确认（不丢，但要等）

// 配合 min.insync.replicas（最小同步副本数）：
// ISR 小于阈值 → 拒绝写入（宁可失败不丢）
```
### 与RocketMQ对比
| 对比       | Kafka                  | RocketMQ       |
| -------- | ---------------------- | -------------- |
| **副本机制** | Leader + Follower（ISR） | Master + Slave |
| **同步集合** | ISR（动态）                | 主从（固定）         |
| **确认级别** | acks（0/1/all）          | 同步/异步复制        |
| **选举**   | 自动（ISR 中选）             | 手动/DLedger     |
| **HW**   | 有（高水位）                 | 无（类似偏移量）       |
**"Kafka 用 ISR（同步副本集合）保证可靠性：只有跟得上 Leader 的副本在 ISR 里，Leader 挂了从 ISR 选新 Leader（保证数据不丢）。LEO 是副本日志末端，HW 是 ISR 中最小的 LEO（消费者只能读到 HW 之前，保证不会读到会丢的数据）。生产配 acks=all + min.insync.replicas 不丢消息。"**
### 面试高频题
#### 题目1：副本机制是什么
```
// Leader 读写 + Follower 备份
// ISR 保证可靠选举
```
#### 题目2：LEO和HW
```
// LEO：副本日志末端（下一条写入位置）
// HW：ISR 最小 LEO（安全消费水位）
```
#### 题目3：消费者能读到哪个位置
```
// HW 之前（所有 ISR 都有的）
// 防止读到会丢的数据
```
#### 题目4：ISR是什么
```
// 与 Leader 保持同步的副本集合
// 只有 ISR 能当新 Leader
```
#### 题目5：Leader挂了怎么选
```
// 从 ISR 选 LEO 最大的
// 保证数据不丢
```
#### 题目6：怎么保证消息不丢
```
// acks=all + min.insync.replicas
// ISR 中多数确认
```
