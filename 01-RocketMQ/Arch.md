## 架构组件
RocketMQ是什么->四大组件->消息流转->核心概念->与其他MQ对比->面试高频题
### RocketMQ是什么
RocketMQ：阿里巴巴开源的分布式消息中间件，高吞吐、高可靠、支持事务消息。
特性
```
// ① 高吞吐（10万+/s）
// ② 高可靠（多副本 + 同步刷盘）
// ③ 事务消息（唯一原生支持较好）
// ④ 延迟消息（18 个级别）
// ⑤ 顺序消息
// ⑥ 消息回溯（按时间/offset）
```
### 四大组件
NameServer（注册中心）
```
// 作用：
// ① 管理 Broker 信息（地址、Topic、路由）
// ② 给生产者和消费者提供路由
// ③ 轻量（不是存储，无状态）

// 特点：
// ① 无状态、相互独立（不通信）
// ② 集群部署（多个）
// ③ Broker 启动时注册，定时心跳续约
// ④ 挂了不影响已连接的 Broker（只影响新路由）

// 类比：类似 Zookeeper/注册中心，但更轻
```
Broker（消息服务器）
```
// 作用：
// ① 存储消息（CommitLog）
// ② 转发消息给消费者
// ③ 处理生产/消费请求

// 结构：
// ① CommitLog：所有消息顺序写入（主存储）
// ② ConsumeQueue：消费队列（按 Topic/Queue 索引）
// ③ IndexFile：消息索引（按 key 查）
// ④ 主从：Master 写，Slave 读/备份

// 高可用：主从复制（异步/同步）
```
Producer（生产者）
```
// 作用：发送消息

// 特点：
// ① 从 NameServer 获取路由
// ② 负载均衡：选队列发送
// ③ 故障转移：Broker 挂了换一个
// ④ 支持同步/异步/单向三种发送
```
Consumer（消费者）
```
// 作用：消费消息

// 特点：
// ① 从 NameServer 获取路由
// ② 负载均衡：多个消费者分配队列
// ③ 两种模式：集群/广播
// ④ 支持重试、死信
```
### 消息流转
```
Producer 发送消息
    │
    ├── ① 从 NameServer 获取路由（哪个 Broker 有该 Topic）
    │
    ├── ② 选择队列（负载均衡）
    │
    ├── ③ 发送到 Broker
    │    └── Broker 写入 CommitLog + 建索引
    │
    ├── ④ 返回发送结果（同步确认）
    │
    └── Consumer 消费
         ├── ① 从 NameServer 获取路由
         ├── ② 拉取消息（长轮询）
         ├── ③ 消费处理
         └── ④ 确认（ack）
```
### 核心概念
Topic 主题
```
// 消息的分类（业务逻辑分组）
// 例：order-topic、stock-topic

// 一个 Topic 在多个 Broker 上有分布
// Producer 发消息到 Topic，Consumer 订阅 Topic
```
Queue 队列
```
// Topic 下的物理队列（分片）
// 一个 Topic 可以有多个 Queue（并行度）

// 作用：
// ① 负载均衡的最小单位
// ② 顺序消息的载体（一个 Queue 内有序）
// ③ 消费者并行消费的基础
```
Tag 标签
```
// Topic 下的二级分类（消息类型）
// 例：order-topic 下 tag=created / paid / shipped

// 作用：
// ① 消费者按 Tag 过滤（只消费关心的）
// ② 减少不必要消息传输
```
Message 消息
```
// 组成：
// ① Topic：所属主题
// ② Tag：标签
// ③ Key：业务唯一键（如订单号，用于索引查询）
// ④ Body：消息内容
// ⑤ 属性：自定义属性
```
概念关系图
```
Topic（订单主题）
 ├── Queue 0（队列0）
 ├── Queue 1（队列1）
 ├── Queue 2（队列2）
 └── Tag: created / paid / shipped（标签分类）
```
### 三种发送方式
同步发送
```
// 发消息 → 等 Broker 确认 → 返回成功/失败
// 保证可靠性（失败可以重试）

SendResult result = producer.send(msg);
System.out.println(result.getSendStatus());
// 适用：重要消息（订单、支付）
```
异步发送
```
// 发消息 → 不等待 → 回调处理结果
// 不阻塞（性能好）

producer.send(msg, new SendCallback() {
    @Override
    public void onSuccess(SendResult sendResult) {
        // 成功回调
    }

    @Override
    public void onException(Throwable e) {
        // 失败回调（可重试）
    }
});
// 适用：量大、可以异步确认的
```
单向发送
```
// 只发不管结果（fire-and-forget）
// 最快，但可能丢

producer.sendOneway(msg);
// 适用：日志、不重要的通知
```

|方式|可靠性|性能|适用|
|---|---|---|---|
|同步|高|低|重要消息|
|异步|高|中|量大|
|单向|低|高|日志|
#### 与其他MQ对比

|对比|RocketMQ|RabbitMQ|Kafka|
|---|---|---|---|
|**组件**|NameServer+Broker|Exchange+Queue|Broker+Partition|
|**存储**|CommitLog|队列文件|分区日志|
|**注册中心**|NameServer|无（直连）|ZK/KRaft|
|**事务消息**|✅ 强项|⚠️ 弱|✅ 0.11+|
|**吞吐**|10万+/s|万级|百万级|
|**延迟**|低|最低|中|
### 面试高频题
#### 题目1：RocketMQ的四大组件
```
// NameServer（路由）、Broker（存储）、
// Producer（生产）、Consumer（消费）
```
#### 题目2：NameServer的作用
```
// 管理 Broker 路由，给客户端提供路由
// 无状态、集群部署
```
#### 题目3：消息怎么流转
```
// Producer 拿路由 → 选队列 → 发 Broker → Consumer 拉取消费
```
#### 题目4：Topic和Queue的关系
```
// Topic 是逻辑分类，Queue 是物理队列
// 一个 Topic 多个 Queue（并行度）
```
#### 题目5：Tag有什么用
```
// 二级分类，消费者按 Tag 过滤
```
#### 题目6：三种发送方式
```
// 同步（可靠）、异步（量大）、单向（最快）
```
