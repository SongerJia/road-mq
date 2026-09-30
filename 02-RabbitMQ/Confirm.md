## 消息确认
为什么需要确认->生产者Confirm->消费者Ack->Ack模式->消息不丢全景->面试高频题
### 为什么需要确认
消息丢失的三个环节
```
// ① 生产端：发出去了吗？（Broker 收到没）
// ② Broker：存住了吗？（持久化）
// ③ 消费端：处理成功了吗？（消费完成没）

// 每个环节都要"确认"才能保证不丢
```
### 生产者确认
三种发布确认模式

|模式|说明|可靠性|性能|
|---|---|---|---|
|**无确认**|发完不管|低（可能丢）|最高|
|**单个确认**|每发一条等确认|高|低|
|**批量确认**|一批一起确认|高|中|
Confirm机制
```
// ① 开启 Confirm
channel.confirmSelect();

// ② 发送消息
channel.basicPublish("order-exchange", "order.created", null, body);

// ③ 等待确认
if (channel.waitForConfirms()) {
    // 发送成功 ✅
} else {
    // 失败 → 重发
}
```
Spring AMQP的Confirm
```yaml
spring:
  rabbitmq:
    publisher-confirm-type: correlated   # 开启 Confirm
    publisher-returns: true             # 失败返回
```
```java
// 回调确认
@Configuration
public class RabbitConfig {
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory factory) {
        RabbitTemplate template = new RabbitTemplate(factory);

        // 确认回调（消息到达 Exchange）
        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (ack) {
                // 到达 Exchange ✅
            } else {
                // 失败 → 记录重发
                log.error("消息发送失败: {}", cause);
            }
        });

        // 返回回调（消息没路由到 Queue）
        template.setReturnsCallback(returned -> {
            log.error("消息未路由: {} → {}", returned.getExchange(), returned.getRoutingKey());
        });
        return template;
    }
}
```
生产端不丢的完整套路
```
// ① 开启 Confirm（确认到达 Exchange）
// ② 开启 Returns（确认路由到 Queue）
// ③ 失败 → 本地表/MQ 记录 → 重发
// ④ 定时补偿（扫未确认的消息）
```
### 消费者确认
三种确认方式

|方式|说明|风险|
|---|---|---|
|**自动确认（autoAck=true）**|收到就确认|丢消息！|
|**手动确认（false）**|处理完再 ack|✅ 可靠|
|**拒绝（reject）**|明确拒绝|进死信/重发|
```
// ① 开启手动确认
boolean autoAck = false;   // 关掉自动确认
channel.basicConsume("order-queue", autoAck, consumer);
```
手动确认代码
```
// 消费回调
DeliverCallback deliverCallback = (consumerTag, delivery) -> {
    try {
        // ① 处理业务
        process(delivery.getBody());

        // ② 确认成功（处理完才 ack）
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    } catch (Exception e) {
        // ③ 失败 → 重新入队（重试）
        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        // requeue=true：重新放回队列（可重试）
    }
};
```
Spring @RabbitListener
```
// Spring 自动管理 ack
@RabbitListener(queues = "order-queue")
public void onOrder(Order order) {
    // 方法正常返回 → 自动 ack
    // 方法抛异常 → 自动重试（Retry）
}

// 配置：
spring:
  rabbitmq:
    listener:
      simple:
        acknowledge-mode: manual   # 手动模式
        retry:
          enabled: true            # 失败重试
          max-attempts: 3          # 最多 3 次
```
### Ack的细节
ack/nack/reject
```
// basicAck(tag, multiple)：确认（成功）
// basicNack(tag, multiple, requeue)：否定（失败）
// basicReject(tag, requeue)：拒绝（单条）

// requeue 参数：
// true：重新放回队列（重试）
// false：丢弃/进死信队列
```
先业务后ack
```
// ❌ 错误：先 ack 后业务
// ack 了 → 业务崩溃 → 消息没了 → 丢！

// ✅ 正确：先业务后 ack
// 业务成功 → ack（确认）
// 业务失败 → nack（重试）
// 保证"消息不丢"
```
重复消费问题
```
// 手动 ack 后，消费者在处理中崩溃：
// ① 已 ack → 消息不重发（这条丢了？如果业务没完成就 ack 了 → 丢）
// ② 未 ack → 连接断开 → 消息重发 → 重复消费！

// 结论：ack 机制保证"不丢"（未确认会重发）
// 但可能"重复"（重发）→ 业务要幂等
```
死信处理
```
// nack + requeue=false → 进死信队列（DLX）
// 或重试多次后进死信
// 死信需要人工/程序处理（不能默默堆积）
```
### 消息不丢全景
```
① 生产端不丢：
   Confirm（确认到达 Exchange）
   + Returns（确认路由到 Queue）
   + 失败重发/补偿

② Broker 不丢：
   持久化（Queue + 消息）
   + 镜像/Quorum 高可用

③ 消费端不丢：
   手动 ack（处理完再确认）
   + 失败重试
   + 死信监控

任何一个环节缺 → 消息可能丢！
```
### 高频面试题
#### 消息不丢怎么保证
```
// 生产：Confirm + Returns
// Broker：持久化 + 高可用
// 消费：手动 ack（先业务后 ack）
```
#### confirm是什么
```
// 生产者确认：消息到达 Exchange 的确认
// 失败回调 → 重发
```
#### 自动趣确认和手动确认
```
// 自动：收到就 ack（可能丢）
// 手动：处理完再 ack（可靠，推荐）
```
#### 为什么要先业务后ack
```
// 先 ack 后业务崩溃 → 消息丢了
// 先业务后 ack → 未确认会重发（不丢但可能重复）
```
#### nack的requeue参数
```
// true：重新入队（重试）
// false：丢弃/死信
```
#### 会重复消费吗
```
// 会！未 ack 连接断开 → 重发
// 解决：业务幂等
```
