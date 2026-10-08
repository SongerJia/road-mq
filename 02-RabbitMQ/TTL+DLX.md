## 延迟队列
延迟队列是什么->实现原理->两种TTL->完整实现->注意事项->面试高频题
### 延迟队列是什么
延迟队列：消息发送后不立即消费，延迟指定时间后才可消费。RabbitMQ没有原生延迟队列，使用TTL+死信组合实现。
应用场景
```
// ① 订单超时取消（30 分钟未支付取消）
// ② 延迟通知（支付成功 10 分钟后评价提醒）
// ③ 定时任务替代（固定延迟执行）
// ④ 重试延迟（失败后延迟重试）
```
### 实现原理
```
// 利用死信机制：
// ① 消息进入"延迟队列"（设 TTL）
// ② TTL 到期 → 消息变死信
// ③ 死信 → 转发到"实际消费队列"（DLX）
// ④ 消费者从实际队列消费

// 延迟的时间 = TTL 时间
```
流程图
```
Producer 发消息
    ↓
延迟队列（x-message-ttl=30s，x-dead-letter-exchange=业务DLX）
    │
    │ 30 秒后消息过期（TTL）
    ▼
死信转发 → 业务交换机（DLX）
    ↓
业务队列（实际消费队列）
    ↓
Consumer 消费（30 秒后才收到）
```
### 两种TTL设置
队列级TTL（统一延迟时间）
```
// 整个队列的消息都延迟相同时间
Map<String, Object> args = new HashMap<>();
args.put("x-message-ttl", 30000);              // 30 秒过期
args.put("x-dead-letter-exchange", "dlx-exchange");  // 死信转发
channel.queueDeclare("delay-30s", true, false, false, args);
```
消息级TTL（每个消息不同延迟）
```
// 同一个队列，每条消息延迟时间不同
AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
    .expiration("30000")   // 这条 30 秒
    .build();
channel.basicPublish("", "delay-queue", props, body);

// 另一条：
props = new AMQP.BasicProperties.Builder()
    .expiration("60000")   // 这条 60 秒
    .build();
```
对比

|方式|延迟粒度|使用|
|---|---|---|
|**队列级 TTL**|统一延迟|简单（一种延迟）|
|**消息级 TTL**|每条不同|灵活|
```
// 消息级 TTL 的问题：
// 队列中的消息是 FIFO 的
// 前面消息的 TTL 长，后面消息的 TTL 短
// 前面的不过期 → 后面的即使到期也要等！
// （消息只能按顺序出队）

// 解决：不同延迟用不同队列（每个延迟一个延迟队列）
```
### 完整实现
延迟队列三步配置
```
// ① 死信交换机 + 业务队列
channel.exchangeDeclare("order-exchange", BuiltinExchangeType.DIRECT, true);
channel.queueDeclare("order-queue", true, false, false, null);
channel.queueBind("order-queue", "order-exchange", "order.timeout");

// ② 延迟队列（TTL + 指向死信）
Map<String, Object> args = new HashMap<>();
args.put("x-message-ttl", 30 * 60 * 1000);       // 30 分钟
args.put("x-dead-letter-exchange", "order-exchange");  // 过期转到业务交换机
args.put("x-dead-letter-routing-key", "order.timeout"); // 路由键
channel.queueDeclare("order-delay-queue", true, false, false, args);

// ③ 发送到延迟队列
channel.basicPublish("", "order-delay-queue", null, body);
// → 30 分钟后自动转到 order-queue
// → 消费者才收到
```
Spring实现
```
@Configuration
public class DelayQueueConfig {

    // 业务队列
    @Bean
    public Queue orderQueue() {
        return new Queue("order-queue", true);
    }

    // 业务交换机（死信目标）
    @Bean
    public DirectExchange orderExchange() {
        return new DirectExchange("order-exchange", true);
    }

    @Bean
    public Binding orderBinding() {
        return BindingBuilder.bind(orderQueue())
            .to(orderExchange()).with("order.timeout");
    }

    // 延迟队列（TTL 30 分钟 + 死信指向业务交换机）
    @Bean
    public Queue delayQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 30 * 60 * 1000);
        args.put("x-dead-letter-exchange", "order-exchange");
        args.put("x-dead-letter-routing-key", "order.timeout");
        return new Queue("order-delay-queue", true, false, false, args);
    }

    // 生产者发送到延迟队列
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory factory) {
        RabbitTemplate template = new RabbitTemplate(factory);
        // 发送到延迟队列
        template.convertAndSend("order-delay-queue", order);
        return template;
    }
}

// 消费者（30 分钟后收到）
@RabbitListener(queues = "order-queue")
public void onTimeout(Order order) {
    // 检查订单是否支付，未支付则取消
    orderService.cancelIfUnpaid(order.getId());
}
```
### 注意事项
消息级TTL的乱序问题
```
// FIFO 队列：前面的 TTL 长会阻塞后面的
// 解决：不同延迟 → 不同延迟队列
// 例：30s 延迟队列、1m 延迟队列、30m 延迟队列
```
延迟消息不精确
```
// TTL 是"过期后才检测"
// 检测有延迟（毫秒级）→ 延迟时间略长
// 精度：秒级够用（业务场景）

// 要毫秒级 → 用 RocketMQ 定时消息/时间轮
```
消息积压问题
```
// 延迟队列消息多：
// ① 延迟队列会积压（等过期）
// ② 监控延迟队列长度
// ③ 惰性队列可缓解内存
```
死信后消息保留
```
// 转死信后：
// ① 原始消息头保留
// ② 会加 x-death 头（记录死信原因、次数）
// ③ 可以查死信来源
```
### 面试高频题
#### 题目1：RabbitMQ怎么实现延迟队列
```
// TTL + 死信队列（DLX）
// 消息进入'延迟队列' → TTL 到期 → 死信转业务队列 → 消费
```
#### 题目2：为什么没有原生死信延迟队列
```
// RabbitMQ 原生不支持延迟
// 用 TTL + DLX 组合实现（变通方案）
// RocketMQ 有原生延迟消息（18 级）
```
#### 题目3：TTL有几种
```
// 队列级（统一）、消息级（每条不同）
// 消息级有 FIFO 阻塞问题
```
#### 题目4：消息级TTL有什么坑
```
// 前面消息不过期 → 后面到期也要等（FIFO）
// 不同延迟用不同队列
```
#### 题目5：延迟队列的延迟准确吗
```
// 秒级精度（TTL 检测有延迟）
// 要精确用 RocketMQ 定时消息
```
#### 题目6：订单超时取消怎么做
```
// 下单 → 发延迟队列（30 分钟 TTL）
// 到期转业务队列 → 查订单 → 未支付则取消
```
