## 核心概念
RabbitMQ是什么->核心组件->消息流转->与RocketMQ对比->面试高频题
### RabbitMQ是什么
RabbitMQ：Erlang开发的轻量级消息中间件，AMQP协议标准实现，功能全面，延迟低，安全可靠。
```
// ① 功能全面（Exchange 路由、死信、延迟）
// ② 延迟最低（微秒级）
// ③ 稳定成熟（老牌）
// ④ 管理界面友好（Web 控制台）
// ⑤ 支持多协议（AMQP、MQTT、STOMP）

// 定位：轻量可靠队列（不是大数据流）
```
### 核心组件

|组件|作用|
|---|---|
|**Producer**|消息生产者|
|**Consumer**|消息消费者|
|**Broker**|服务节点（接收+存储+转发）|
|**Queue**|消息队列（存储消息）|
|**Exchange**|交换机（路由分发）|
|**Binding**|绑定（Exchange 和 Queue 的关系）|
和RocketMQ组件对比
```
// RabbitMQ：Exchange（交换机）是灵魂！
// RocketMQ：没有 Exchange（直接 Topic）

// RocketMQ：Producer → Topic → Consumer
// RabbitMQ：Producer → Exchange →（路由）→ Queue → Consumer

// 多了一个 Exchange 层 → 路由更灵活
```
### 消息流转
```
Producer 发送消息
    │
    ├── ① 发到 Exchange（交换机，不存消息）
    │
    ├── ② Exchange 按路由规则（Binding）分发
    │
    ├── ③ 消息进入匹配的 Queue（存储）
    │
    └── ④ Consumer 从 Queue 消费
```
关键点
```
// ① Exchange 不存消息（只路由）
// ② Queue 才存消息
// ③ Exchange → Queue 靠 Binding 绑定
// ④ 没有匹配的 Queue → 消息丢失（或进死信）
```
### 核心概念详解
Queue 队列
```
// 消息的存储单元（FIFO）
// 特点：
// ① 消息按顺序消费（FIFO）
// ② 可持久化（重启不丢）
// ③ 可设置参数（TTL、长度限制）
// ④ 多个消费者竞争消费（一个消息一人）
```
Exchange 交换机
```
// 消息的路由分发器（不存储）
// 决定消息去哪个 Queue

// 四种类型：
// ① Direct：精确匹配 routingKey
// ② Topic：通配符匹配
// ③ Fanout：广播到所有绑定队列
// ④ Headers：按消息头匹配
```
Binding 绑定
```
// Exchange 和 Queue 的绑定关系
// 绑定 + routingKey（路由键）
// 决定消息按什么规则进哪个队列

// 例：
// exchange: order-exchange
// binding: order-exchange → order-queue  (routingKey=order.created)
// 消息 routingKey=order.created → 进 order-queue
```
Message 消息
```
// 组成：
// ① 属性（Properties）：路由键、TTL、优先级、持久化标志
// ② 消息体（Body）：业务数据
// ③ 头（Header）：自定义头

// routingKey 是路由的依据
```
### 三种交换方式
Direct 精确
```
Producer（routingKey=error）
    ↓
Exchange（direct）
    ├── error-queue（绑定 error）✅ 收到
    └── info-queue（绑定 info）❌ 不收
```
Topic 通配符
```
Producer（routingKey=order.created）
    ↓
Exchange（topic）
    ├── *.created（order.created）✅
    └── order.#（order.xxx.xxx）✅
```
Fanout 广播
```
Producer（任意 key）
    ↓
Exchange（fanout）
    ├── queue1 ✅ 都收到
    ├── queue2 ✅ 都收到
    └── queue3 ✅ 都收到
```
与RocketMQ对比

|对比|RabbitMQ|RocketMQ|
|---|---|---|
|**路由**|Exchange（灵活）|Topic（简单）|
|**吞吐**|万级|10 万+|
|**延迟**|最低（微秒）|低|
|**事务消息**|⚠️ 弱（需插件）|✅ 原生|
|**顺序消息**|⚠️ 弱|✅ 分区顺序|
|**管理界面**|✅ 好用|✅ 有|
|**生态**|老牌稳定|阿里系|
|**适用**|轻量通用|高吞吐/事务|
选型
```
// ① 简单通用、延迟敏感 → RabbitMQ
// ② 高吞吐、事务消息、阿里系 → RocketMQ
// ③ 大数据流 → Kafka
```
### 面试高频题
#### RabbitMQ核心组件
```
// Producer/Consumer/Broker/Queue/Exchange/Binding
```
#### Exchange的作用
```
// 路由分发（不存消息）
// 按 routingKey 把消息路由到 Queue
```
消息流转
```
// Producer → Exchange → Binding → Queue → Consumer
```
#### Queue和Exhange区别
```
// Exchange：路由不存储
// Queue：存储消息
```
#### routingKey是什么
```
// 路由键，Exchange 按它匹配 Queue
```
#### 和RocketMQ区别
```
// Exchange 路由 vs Topic 直接
// 延迟低 vs 吞吐高
```
