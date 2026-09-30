## 交换机
Exchange是什么->Direct->Topic->Fanout->Headers->选型->面试高频题
### Exchange是什么
Exchange：消息的中转站，负责按路由规则把消息分发给绑定的Queue。它自己不存消息。
```
Producer ──→ Exchange ──(路由)──→ Queue ──→ Consumer
              （不存储）           （存储）
```
关键概念
```
// ① routingKey（路由键）：消息携带，路由依据
// ② Binding（绑定）：Exchange 和 Queue 的关系
// ③ 四种类型决定"怎么路由"
```
### Direct 直连
规则：精确匹配
```
// routingKey 完全相等才路由
// 一对一（一条消息进一个 Queue，除非绑定多个）

// 声明：
channel.exchangeDeclare("direct-exchange", BuiltinExchangeType.DIRECT);

// 绑定（队列绑定到交换机，指定 key）：
channel.queueBind("error-queue", "direct-exchange", "error");
channel.queueBind("info-queue", "direct-exchange", "info");

// 发送：
channel.basicPublish("direct-exchange", "error", null, msg);
// routingKey=error → error-queue ✅
// routingKey=info → info-queue ✅
```
场景
```
// 按级别分发日志：
// error → 告警队列（通知值班）
// warn → 日志队列
// info → 归档队列

// 特点：精确、一对一、最常用
```
### Topic 主题
规则：通配符匹配
```
// routingKey 支持通配符：
// *：匹配一个单词（点分隔的一段）
// #：匹配零个或多个单词

// 声明：
channel.exchangeDeclare("topic-exchange", BuiltinExchangeType.TOPIC);

// 绑定：
channel.queueBind("order-queue", "topic-exchange", "order.*");
channel.queueBind("all-queue", "topic-exchange", "#");

// 发送：
channel.basicPublish("topic-exchange", "order.created", null, msg);
// order.created：
//   order.* → ✅（order.created 匹配）
//   # → ✅（全匹配）
channel.basicPublish("topic-exchange", "order.paid", ...);
// order.paid → order-queue ✅
```
通配符对照
```
// routingKey = order.created.paid
// order.* → ❌（* 只匹配一段）
// order.# → ✅（# 匹配多段）
// *.created → ✅
// #.paid → ✅

// 特点：灵活（按模式订阅）
// 适用：多个服务订阅相关事件
```
### Fanout 扇形广播
规则：全部广播
```
// 忽略 routingKey，发给所有绑定的 Queue

// 声明：
channel.exchangeDeclare("fanout-exchange", BuiltinExchangeType.FANOUT);

// 绑定（多个队列）：
channel.queueBind("queue1", "fanout-exchange", "");
channel.queueBind("queue2", "fanout-exchange", "");
channel.queueBind("queue3", "fanout-exchange", "");

// 发送（key 无所谓）：
channel.basicPublish("fanout-exchange", "any-key", null, msg);
// → queue1 ✅ queue2 ✅ queue3 ✅ 全部收到
```
场景
```
// ① 全局广播通知（所有服务刷新配置）
// ② 多系统订阅同一事件（日志分发到多个消费者）
// ③ 类似发布订阅（每条消息所有人）

// 特点：广播、无路由逻辑、类似 RocketMQ 广播模式
```
### Headers 消息头
规则：按消息头匹配
```
// 不看 routingKey，看消息的 header 属性
// x-match：
// all：所有 header 都匹配
// any：任一 header 匹配

// 声明：
channel.exchangeDeclare("headers-exchange", BuiltinExchangeType.HEADERS);

// 绑定（指定 header）：
Map<String, Object> args = new HashMap<>();
args.put("x-match", "all");        // 必须全部匹配
args.put("type", "order");
args.put("priority", "high");
channel.queueBind("order-high-queue", "headers-exchange", "", args);

// 发送（带 header）：
AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
    .headers(Map.of("type", "order", "priority", "high"))
    .build();
channel.basicPublish("headers-exchange", "", props, msg);
// → order-high-queue ✅（type=order + priority=high）
```
特点
```
// ① 灵活（任意属性组合）
// ② 不直观（配置复杂）
// ③ 很少用（Direct/Topic 够）
// 适用：需要按多维度属性路由
```
### 四种类型对比
| 类型          | 匹配规则  | routingKey | 使用频率 |
| ----------- | ----- | ---------- | ---- |
| **Direct**  | 精确相等  | ✅ 需要       | ⭐⭐⭐⭐ |
| **Topic**   | 通配符   | ✅ 需要       | ⭐⭐⭐⭐ |
| **Fanout**  | 全部广播  | ❌ 忽略       | ⭐⭐⭐  |
| **Headers** | 消息头匹配 | ❌ 不用       | ⭐    |
选型
```
// ① 精确路由（一对一）→ Direct
// ② 按模式订阅（模糊）→ Topic
// ③ 广播所有 → Fanout
// ④ 按属性多维度 → Headers（少用）

// 生产最常见：Topic + Direct
```
### 面试高频题
#### Exchange有哪四种
Direct精确、Topic通配、Fanout广播、Headers头匹配
#### Direct和Topic有什么区别
```
// Direct：routingKey 完全相等
// Topic：通配符匹配（* 一段 / # 多段）
```
#### Fanout特点
```
// 忽略 routingKey，广播给所有绑定队列
```
#### 消息没有匹配的Queue会怎么样
```
// 消息丢失！（Exchange 不存储）
// 可设置 mandatory → 返回给生产者（确认）
// 或进死信队列
```
#### 一个Queue能绑定多个Exchange吗
```
// 能！Queue 和 Exchange 多对多（通过 Binding）
```
#### 实战怎么选
```
// 精确 Direct、模糊 Topic、广播 Fanout
// Headers 少用
```