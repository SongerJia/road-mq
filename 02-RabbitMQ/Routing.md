## 消息路由
路由是什么->routingKey->绑定规则->通配符详解->实战场景->面试高频题
### 消息路由是什么
消息路由：Exchange根据路由键（routingKey）和绑定规则觉的消息进哪个队列。
```
Producer（带 routingKey）
    ↓
Exchange 检查绑定规则
    ↓ 匹配
Queue A / Queue B / 多个 / 无
```
路由三要素
```
// ① routingKey：消息携带的"地址标签"
// ② Binding：Queue 声明"我要什么 key"
// ③ Exchange 类型：决定怎么匹配
```
### routingKey（路由键）
```code
// 消息的属性之一，路由的依据
// 类似"地址"，Exchange 按它分发

// 命名习惯：用点分层
// "order.created" / "order.paid" / "log.error"
```
```java
// 发送时设置 routingKey
AMQP.BasicProperties props = ...;
channel.basicPublish("order-exchange", "order.created", props, body);
//                      交换机           路由键
```
路由键和绑定
```
// 队列绑定到交换机时指定"感兴趣的路由键"
channel.queueBind("order-queue", "order-exchange", "order.created");
// 只有 routingKey=order.created 的消息进 order-queue

channel.queueBind("all-queue", "order-exchange", "#");
// # 匹配所有 → 所有消息都进 all-queue
```
### 绑定规则
绑定=声明关系
```
// Exchange + routingKey + Queue → Binding
// 一个队列可以绑定多个 key
// 一个 key 可以绑多个队列

// 例：
// order-queue ← binding("order.created")
// order-queue ← binding("order.paid")   （多个 key 一个队列）
// log-queue  ← binding("log.*")
// alert-queue ← binding("log.error")     （一个 key 多个队列）
```
匹配流程
```
消息（routingKey=order.paid）
    ↓ 进入 order-exchange
    ↓ 检查所有绑定
    ├── order-queue (order.created) → 不匹配 ❌
    ├── order-queue (order.paid) → 匹配 ✅
    └── all-queue (#) → 匹配 ✅
    ↓
进 order-queue + all-queue（多队列）
```
### 通配符详解（TOPIC）
两个通配符

|通配符|含义|例子|
|---|---|---|
|*****|匹配"一个单词"（一个点分隔段）|`order.*` → order.created ✅|
|**#**|匹配"零个或多个单词"|`order.#` → order.created.paid ✅|
对照示例
```
// routingKey：order.created
// 绑定模式：
// order.*   → ✅（* 匹配 created）
// order.#   → ✅（# 匹配 created）
// *.created → ✅（* 匹配 order）
// #         → ✅（# 匹配全部）
// order.paid→ ❌（不相等）

// routingKey：order.created.paid
// order.*   → ❌（* 只匹配一段）
// order.#   → ✅（# 匹配多段）
// *.created → ❌（两段）
// #.paid    → ✅（# 匹配 order.created）

// *：恰好一段（严格一个单词）
// #：任意段（零到多）
// 点号是分隔符（类似 URL 路径的 /）
```
### 四种类型的路由规则
|类型|规则|routingKey|例子|
|---|---|---|---|
|**Direct**|精确相等|✅|order.created 精确|
|**Topic**|通配符|✅|order.* / #|
|**Fanout**|全部广播|❌ 忽略|无条件|
|**Headers**|消息头|❌ 不用|header 匹配|
### 实战场景
场景：订单事件分发
```
// Exchange：order-exchange（Topic）

// 绑定：
// 库存队列 ← "order.created"（只关心下单）
// 支付队列 ← "order.paid"
// 统计队列 ← "order.*"（关心所有订单事件）
// 审计队列 ← "#"（关心所有消息）

// 发送 order.created：
// → 库存队列 ✅
// → 统计队列 ✅（order.*）
// → 审计队列 ✅（#）
// → 支付队列 ❌
```
场景：日志分级
```
// Exchange：log-exchange（Direct）

// 绑定：
// error-queue ← "log.error"（告警）
// warn-queue ← "log.warn"
// info-queue ← "log.info"

// 发送 log.error → 只进 error-queue（精确）
```
### 面试高频题
#### 题目1：消息路由的原理
```
// Exchange 按 routingKey + 绑定规则
// 决定消息进哪个 Queue
```
#### r题目2：outingKey是什么
```
// 消息的路由地址标签
// Exchange 匹配的依据
```
#### 题目3：*和#的区别
```
// *：恰好一个单词（一段）
// #：零到多个单词（多段）
```
#### 题目4：一条消息可以进入多个队列吗
```
// 能！多个绑定匹配 → 进多个队列（Fanout/Topic）
```
#### 题目5：消息没有匹配队列会怎么样
```
// 消息丢失（Exchange 不存）
// mandatory 开启 → 返回生产者
// 或进死信
```
#### 题目6：路由实战怎么设计
```
// Topic 通配符（order.*）最灵活
// Direct 精确（log.error）精准
// 分层命名 routingKey（order.created.paid）
```
