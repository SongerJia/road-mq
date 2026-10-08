## 高可用
高可用总览->副本分布->Controller->故障恢复流程->元数据管理->面试高频题
### 高可用总览
```
// Kafka 高可用 = 多副本 + 自动故障转移
// ① 分区多副本（副本因子 3）
// ② Leader 挂了自动选新 Leader
// ③ Controller 协调
// ④ Broker 挂了不影响其他
```
核心保障
```
// ① 副本：数据冗余（不丢）
// ② 选举：Leader 故障自动切换（可用）
// ③ Controller：集群协调（管理）
// ④ 多 Broker：单点故障不影响整体
```
### 副本分布
分区副本怎么放
```
// 副本因子 3：每个分区 3 份副本
// 分布在不同 Broker（故障隔离）

// 例：3 个 Broker，Topic 3 分区，副本 3：
// p0：Leader(B1) + 副本(B2) + 副本(B3)
// p1：Leader(B2) + 副本(B1) + 副本(B3)
// p2：Leader(B3) + 副本(B1) + 副本(B2)

// 每个 Broker 都是某些分区的 Leader（负载均衡）
// 一个 Broker 挂了 → 它的 Leader 分区由其他副本接管
```
为什么副本要分散
```
// ① 单 Broker 挂 → 数据不丢（其他有副本）
// ② Leader 分散 → 负载均衡（不会单点压满）
// ③ 容忍单节点故障
```
### Controller控制器
```
// 集群中一个特殊的 Broker（Controller）
// 负责集群管理（元数据、选举、故障处理）

// 职责：
// ① 管理分区 Leader 选举
// ② 管理副本分配
// ③ 处理 Broker 上下线
// ④ 维护集群元数据
// ⑤ 通知其他 Broker（元数据变更）
```
Controller选举
```
// ① Controller 挂了 → 其他 Broker 竞选
// ② 谁先抢到（ZooKeeper 临时节点 / KRaft）
// ③ 当选的成为新 Controller
// ④ 新的 Controller 恢复集群管理

// 类似：注册中心的"领导者"
```
元数据管理
```
// 老版本：ZooKeeper（3.x 前）
// 新版本：KRaft（2.8+，去 ZK）
// 元数据：Topic、分区、副本、Leader 信息

// KRaft：
// ① 自管理（不需要 ZK）
// ② 基于 Raft 共识
// ③ 部署更简单
```
### 故障恢复流程
Leader挂了
```
① 某个 Broker 上的分区 Leader 挂了
    ↓
② 其他 Broker 心跳发现（Broker 失联）
    ↓
③ Controller 感知（元数据变更）
    ↓
④ Controller 从 ISR 中选新 Leader
    ↓
⑤ 更新元数据（通知所有 Broker）
    ↓
⑥ 生产者/消费者切换到新 Leader
    ↓
⑦ 其他副本从新 Leader 重新同步
```
选举细节
```
// 新 Leader 选择：
// ① 优先从 ISR 中选（保证数据不丢）
// ② 选 LEO 最大的（数据最新）
// ③ 默认不允许非 ISR 当选（unclean.leader.election=false）
//    → 防止数据丢失（但可能不可用）
// ④ 允许非 ISR：可用性优先（可能丢数据）
```
消费端感知
```
// 消费者怎么知道 Leader 变了？
// ① 拉取失败（Leader 挂了）
// ② 获取新的元数据（Controller 广播）
// ③ 找到新 Leader 继续消费

// 生产者同理：发送失败 → 重试 → 路由到新 Leader
```
### Broker故障的处理
场景：一个Broker挂了
```
// ① 该 Broker 上的分区 Leader 全部切换（其他副本）
// ② 该 Broker 上的副本（非 Leader）暂时缺失
// ③ ISR 缩小（少了该副本）
// ④ 生产继续（min.insync 满足即可）
// ⑤ Broker 恢复 → 重新加入 ISR（同步数据）
```
场景：多个Broker挂了
```
// 副本因子 3 + min.insync=2：
// ① 挂 1 个：ISR>=2，正常
// ② 挂 2 个：ISR=1 < 2，拒绝写（不丢但不可用）
// ③ 恢复后：重新同步
```
生产高可用配置
```
default.replication.factor=3      # 副本 3
min.insync.replicas=2            # 最小同步 2
unclean.leader.election.enable=false  # 不允许非 ISR 当选
auto.leader.rebalance.enable=true    # 自动平衡
```
### 面试高频题
#### 题目1：kafka高可用怎么保证
```
// 多副本 + ISR + 自动选举 + Controller
```
#### 题目2：Controller是什么
```
// 集群管理器（选举、元数据、故障处理）
// 挂了自动重新选举
```
#### 题目3：Leader挂了怎么恢复
```
// Controller 从 ISR 选新 Leader（LEO 最大）
// 更新元数据，客户端切换
```
#### 题目4：为什么优先从ISR选
```
// ISR 都是同步过的（数据不丢）
// 非 ISR 可能丢数据
```
#### 题目5：unclean.leader.election
```
// true：允许非 ISR 当选（可用优先，可能丢）
// false：不允许（可靠优先，可能短暂不可用）
// 生产默认 false
```
#### 题目6：zookeeper和KRaft
```
// 老：ZK 管理元数据
// 新：KRaft（2.8+，Raft 自管理）
```

