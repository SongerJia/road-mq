## 持久化
持久化是什么->三处持久化->队列持久化->消息持久化->交换机持久化->完整不丢组合->面试高频题
### 持久化是什么
持久化：消息在RabbitMQ重启依然存在，不会丢失
```
// 不持久化：Broker 重启 → 队列和消息消失 → 数据丢失！

// 持久化 = 写入磁盘（重启可恢复）
```
三处都要持久化
```
// ① 交换机持久化（Exchange durable）
// ② 队列持久化（Queue durable）
// ③ 消息持久化（Message persistent）

// 三个都设，消息才真正不丢！
// 只设一个 → 还是可能丢
```
### 队列持久化
```
// 声明持久化队列
boolean durable = true;
channel.queueDeclare("order-queue", durable, false, false, null);
// 参数：队列名、持久化、独占、自动删除、参数

// durable=true：队列重启后还在（元数据保存）

// 对比：
// 非持久化队列：重启后消失（消息也没了）
```
注意
```
// ① 队列持久化只保证"队列存在"
// ② 不保证"队列里的消息"存在（消息还要持久化）
// ③ 已存在的队列不能改持久化属性（删除重建）
```
### 消息持久化
```
// 发送持久化消息
AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
    .deliveryMode(2)   // 2 = 持久化，1 = 非持久化
    .build();

channel.basicPublish("order-exchange", "order.created", props, body);

// 只有消息持久化 + 队列持久化 → 消息重启不丢
```
消息持久化原理
```
// ① 消息写入磁盘（落盘）
// ② 重启时从磁盘恢复

// 注意：
// ① 持久化有性能损耗（磁盘 IO）
// ② 重要消息持久化，普通消息可以不
// ③ 持久化在"进入队列时"完成
```
### 交换机持久化
```
// 声明持久化交换机
boolean durable = true;
channel.exchangeDeclare("order-exchange", BuiltinExchangeType.TOPIC, durable);

// durable=true：交换机重启后还在（绑定关系保留）
```
三处持久化总结

|对象|持久化参数|不持久化的后果|
|---|---|---|
|**交换机**|durable=true|重启后消失（绑定也没了）|
|**队列**|durable=true|重启后消失（消息也没了）|
|**消息**|deliveryMode=2|重启后消息丢失|
### 完整不丢组合
消息不丢=三处持久化+生产者确认+消费者ack
```
// ① 交换机 durable=true
// ② 队列 durable=true
// ③ 消息 deliveryMode=2
// ④ 生产者 Confirm（确认到达）
// ⑤ 消费者手动 ack（处理完确认）

// 全链路才可靠！
```
Spring AMQP配置
```yaml
spring:
  rabbitmq:
    publisher-confirm-type: correlated   # 生产确认
    publisher-returns: true             # 路由确认
```
```java
// Spring 声明持久化队列/交换机
@Configuration
public class RabbitConfig {
    @Bean
    public Queue orderQueue() {
        // durable=true 持久化
        return new Queue("order-queue", true);
    }

    @Bean
    public TopicExchange orderExchange() {
        // durable=true 持久化
        return new TopicExchange("order-exchange", true);
    }

    @Bean
    public Binding binding() {
        return BindingBuilder
            .bind(orderQueue())
            .to(orderExchange())
            .with("order.created");
    }
}
```
### 面试高频题
#### 题目1：持久化是什么
```
// 写入磁盘，重启不丢
// 交换机 + 队列 + 消息三处
```
#### 题目2：三处持久化缺一不可
```
// 是！只持久化队列，消息不持久化 → 消息丢
// 三者都要 durable
```
#### 题目3：消息怎么持久化
```
// deliveryMode=2
// 写入磁盘（落盘）
```
#### 题目4：持久化的性能影响
```
// 有！磁盘 IO 开销
// 重要消息持久化，普通消息不持久化
```
#### 题目5：持久化+confirm+Ack的关系
```
// 持久化：Broker 存住
// Confirm：生产确认到没到
// Ack：消费确认处理完
// 三层配合才不丢
```
#### 题目6：队列已经创建能改持久化吗
```
// 不能！要删除重建
```

