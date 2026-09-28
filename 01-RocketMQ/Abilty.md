## 高可用
高可用总览->NameServer高可用->Broker高可用->主从切换->客户端容错->面试高频题
### 高可用总览
那些组件要高可用
```
// ① NameServer：注册中心（多实例，无状态）
// ② Broker：消息服务器（主从部署）
// ③ Producer/Consumer：客户端（自带容错）

// 目标：任何一个组件挂了，系统仍可用
```
### NameServer高可用
```
// ① 多个 NameServer 独立部署（不通信、无状态）
// ② 每个都有完整路由信息
// ③ 客户端同时连接多个（随机选一个）
// ④ 一个挂了 → 用其他的

// 特点：
// ① 无状态 → 好扩展（加实例即可）
// ② 不通信 → 不互相依赖（更可靠）
// ③ 挂了不影响已连接的 Broker
```
客户端路由容错
```
// Producer/Consumer 配置多个 NameServer：
producer.setNamesrvAddr("192.168.1.1:9876;192.168.1.2:9876");
// 连不上第一个 → 自动换第二个

// 注意：NameServer 全挂 → 无法获取新路由
// 但已缓存的路由还能用（容灾）
```
### 面试高频

> **Q:** "NameServer 挂了一个会怎样？" 
> **A:** "不影响。NameServer 无状态集群，多个实例独立部署，客户端配置了多个地址会自动切换。但要注意：NameServer 全挂时，新发送的消息可能拿不到新路由（Broker 新增的 Topic 路由拿不到），已缓存的还能用。"
### Broker高可用
主从部署
```
// 每个 Broker 主节点配一个从节点：
// Master（写 + 读）
// Slave（读 + 备份）

// 作用：
// ① 数据冗余（主挂了从有数据）
// ② 读写分离（从可分担读）
// ③ 故障转移基础
```
主从通信
```
// ① 主节点写 CommitLog
// ② 主节点同步给从节点（异步/同步）
// ③ 从节点保持数据一致（实时）

// 配置：
brokerId=0     // Master
brokerId=1     // Slave
brokerRole=ASYNC_MASTER / SYNC_MASTER / SLAVE
```
### 主从切换
手动切换
```
// RocketMQ 默认不自动切换主从！
// 主节点挂了：
// ① 从节点还在（数据没丢）
// ② 但不会自动升级为主（不能写）
// ③ 需要手动把从节点变为主（改配置 + 重启）

// 对比 Kafka/Redis：RocketMQ 默认手动
// 生产一般配合 DLedger（Raft）自动切换
```
DLedger 自动切换
```
// DLedger：基于 Raft 的自动主从切换
// ① 多个 Broker 组成 Raft 组
// ② 自动选举 Leader（主）
// ③ Leader 挂了 → 自动选举新 Leader

// 配置：enableDLegerCommitLog=true

// 对比：
// 普通主从：手动切换（默认）
// DLedger：自动切换（Raft）
```
客户端容错
```
// 主节点挂了，客户端怎么办：
// ① Producer：发送失败 → 重试其他 Broker（有副本的）
// ② Consumer：从节点还可以读（保留数据）
// ③ 恢复后主从重新同步

// 所以：主挂不丢数据（从有），但不能写（等切换）
```
### Broker集群模式
多种部署形态
```
// ① 单主单从：1 主 1 从（最简）
// ② 多主多从（异步复制）：N 主 N 从（常用）
// ③ 多主多从（同步复制）：可靠性高
// ④ DLedger 多副本：Raft 自动切换

// 生产常见：多主多从（异步复制）
// 每个 Topic 的 Queue 分布在多个 Broker（并行）
```
数据分布
```
// Topic 的 Queue 分散到多个 Broker：
// order-topic：
// Queue 0, 1 → Broker-A（主）+ Broker-A'
// Queue 2, 3 → Broker-B（主）+ Broker-B'
// Queue 4, 5 → Broker-C（主）+ Broker-C'

// ① 并行（多个 Broker 分担）
// ② 冗余（每个都有从）
// ③ Broker 挂了 → 该 Broker 的 Queue 不可用（其他可用）
```
### 面试高频题
#### NameServer怎么高可用
```
// 多个无状态实例，客户端配置多个地址
// 一个挂不影响
```
#### Broker怎么高可用
```
// 主从部署（Master 写 + Slave 备份）
// 数据不丢（从有副本）
```
#### 主节点挂了怎么办
```
// 默认手动切换（改配置重启）
// DLedger 自动切换（Raft 选举）
```
#### 主从怎么保证不丢
```
// 同步复制：从确认才返回
// 异步复制：主写完返回（可能丢，但一般够）
```
客户端挂了怎么容错
```
// Producer 发送失败重试其他 Broker
// Consumer 从从节点读
```
和Kafka高可用区别
```
// Kafka：分区多副本自动选举（ISR）
// RocketMQ：主从（默认手动 / DLedger 自动）
```