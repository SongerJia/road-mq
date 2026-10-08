## 死信
死信是什么->死信三种来源->DLX配置->死信处理->惰性队列->面试高频题
### 死信是什么
Dead Letter：死信，无法被正常消费的消息。死信不会消息，可能会转到死信队列专门处理。
```
// 消息成为死信 → 转发到 DLX（死信交换机）→ 死信队列
// 不是直接丢弃，而是"转移"到专门的地方
```
为什么需要死信队列
```
// ① 失败消息不丢（能查到）
// ② 集中处理（监控、补偿、重投）
// ③ 实现延迟队列（TTL + DLX）
// ④ 消息可回溯
```
### 死信的三种来源
消息被拒绝
```
// 消费者 nack/reject 且 requeue=false
// 消息进死信

channel.basicNack(deliveryTag, false, false);  // requeue=false → 死信
```
消息过期
```
// 消息存活时间超过 TTL → 进死信
// 用于实现延迟队列

// 队列级 TTL：
Map<String, Object> args = new HashMap<>();
args.put("x-message-ttl", 30000);   // 30 秒过期
channel.queueDeclare("delay-queue", true, false, false, args);

// 消息级 TTL：
AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
    .expiration("30000")  // 30 秒
    .build();
```
队列达到最大长度
```
// 队列满了，新消息进不去 → 成为死信

Map<String, Object> args = new HashMap<>();
args.put("x-max-length", 10000);     // 最多 1 万条
args.put("x-max-length-bytes", 10 * 1024 * 1024);  // 最多 10MB
channel.queueDeclare("queue", true, false, false, args);
```
三种来源总结

|来源|触发|说明|
|---|---|---|
|**消息被拒绝**|nack/reject + requeue=false|消费失败|
|**消息过期**|TTL 到期|延迟队列基础|
|**队列满**|超过 max-length|容量控制|
### DLX配置
配置步骤
```
// ① 声明死信交换机（普通交换机即可）
channel.exchangeDeclare("dlx-exchange", BuiltinExchangeType.DIRECT, true);

// ② 声明死信队列
channel.queueDeclare("dlx-queue", true, false, false, null);

// ③ 绑定（死信队列绑到死信交换机）
channel.queueBind("dlx-queue", "dlx-exchange", "dlx");

// ④ 普通队列配置 DLX（关键！）
Map<String, Object> args = new HashMap<>();
args.put("x-dead-letter-exchange", "dlx-exchange");  // 指定死信交换机
args.put("x-dead-letter-routing-key", "dlx");        // 死信路由键
channel.queueDeclare("normal-queue", true, false, false, args);

// 流程：
// normal-queue 中变死信的消息
// → 自动转到 dlx-exchange
// → 进入 dlx-queue（专门处理）
```
Spring配置
```
@Configuration
public class DlxConfig {

    // 死信交换机
    @Bean
    public DirectExchange dlxExchange() {
        return new DirectExchange("dlx-exchange", true, false);
    }

    // 死信队列
    @Bean
    public Queue dlxQueue() {
        return new Queue("dlx-queue", true);
    }

    // 绑定
    @Bean
    public Binding dlxBinding() {
        return BindingBuilder.bind(dlxQueue()).to(dlxExchange()).with("dlx");
    }

    // 普通队列（配置死信）
    @Bean
    public Queue normalQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", "dlx-exchange");
        args.put("x-dead-letter-routing-key", "dlx");
        return new Queue("normal-queue", true, false, false, args);
    }
}
```
### 死信处理
消费死信队列
```
// 死信队列的消费者：专门处理失败消息
@RabbitListener(queues = "dlx-queue")
public void onDeadLetter(Message message) {
    // ① 记录失败原因
    log.error("死信消息: {}", new String(message.getBody()));

    // ② 处理策略：
    // a. 重投（修复后重新发送）
    // b. 人工介入（告警）
    // c. 归档（存 DB 后续分析）
    // d. 忽略（确实无用的）
}
```
死信监控
```
// 生产最佳实践：
// ① 死信队列消息数监控（告警）
// ② 死信消息要可查（控制台/日志）
// ③ 定期处理（不能堆积）
// ④ 分析死信原因（业务 bug？数据问题？）
```
### 惰性队列 Lazy Queue
```
// Lazy Queue（惰性队列）：
// 消息尽量存磁盘（内存压力小）
// 适合：消息量大、内存有限

// 对比普通队列：
// 普通队列：消息尽量放内存（快，但内存压力大）
// 惰性队列：消息直接落盘（内存稳定，但慢）
```
配置
```
// 声明惰性队列
Map<String, Object> args = new HashMap<>();
args.put("x-queue-mode", "lazy");   // 惰性模式
channel.queueDeclare("lazy-queue", true, false, false, args);

// 或通过 policy：
rabbitmqctl set_policy Lazy "^lazy-" '{"queue-mode":"lazy"}'
```
适用场景
```
// ① 消息积压大（突发流量）
// ② 内存紧张（保护 Broker）
// ③ 对延迟不敏感（批量消费）

// 注意：
// ① 惰性队列牺牲性能（磁盘 IO）
// ② 普通场景不用（内存够就用普通）
```
### 面试高频题
#### 题目1：死信是什么？三种来源
```
// 无法消费的消息 → 转死信队列
// 被拒绝（requeue=false）、TTL 过期、队列满
```
#### 题目2：DLX怎么配置
```
// ① 声明死信交换机
// ② 普通队列设 x-dead-letter-exchange
// ③ 死信自动转到 DLX → 死信队列
```
#### 题目3：死信处理方式
```
// 重投、人工介入、归档、忽略
// 必须监控（不能堆积）
```
#### 题目4：惰性队列是什么
```
// 消息直接落盘（内存压力小）
// 适合消息积压、内存紧张
```
#### 题目5：TTL和死信的关系
```
// TTL 过期 → 进死信
// 这是延迟队列的基础
```
#### 题目6：死信队列的作用
```
// 失败消息不丢、集中处理、监控、实现延迟
```
