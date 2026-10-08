## 重复消费与乱序
为什么重复->为什么乱序->幂等方案->顺序保证->实战处理->面试高频题
### 为什么重复消费
重复消费的原因
```
// ① offset 未提交就崩溃
//   处理成功 → 提交前崩溃 → 重启重新消费（重复）

// ② Rebalance
//   分区重新分配 → 新消费者从旧 offset 开始（重复）

// ③ 手动提交失败
//   处理成功 → 提交失败 → 下次拉取重复

// ④ 生产者重试
//   发送超时重试 → 消息写两次（生产端重复）
```
本质
```
// At-least-once 语义 = 不丢但可能重复
// Kafka 默认就是 At-least-once
// 重复是"常态"，必须靠业务幂等处理
```
### 为什么乱序
乱序的原因
```
// ① 同一 key 进了不同分区
//   不同分区不同消费者 → 并行处理 → 乱序

// ② 分区重分配
//   Rebalance 后顺序可能被打乱

// ③ 生产者重试（乱序）
//   消息 A 发送失败重试，消息 B 先发出 → B 先到
//   解决：幂等生产者（idempotence）保证重试不乱序

// ④ 多线程消费
//   一个分区多线程消费（poll + 多线程处理）→ 乱序
```
核心认知
```
// Kafka 只保证"分区内有序"
// 跨分区天然无序
// 保证有序的代价：一个分区一个消费者单线程
```
### 幂等方案
方案一：唯一键去重
```
// ① 消息带唯一键（业务 ID）
// ② 消费时查"是否已处理"
// ③ 已处理 → 跳过（幂等）

// 用 Redis 去重：
public void onOrder(OrderMessage msg) {
    String key = "processed:" + msg.getOrderId();

    // ① 尝试标记（不存在才设置）
    Boolean first = redis.setIfAbsent(key, "1", 1, TimeUnit.DAYS);

    if (first) {
        // ② 第一次处理
        orderService.process(msg);
    }
    // ③ 重复消息直接忽略
}

// 用数据库唯一约束：
// 订单处理表加 order_id 唯一索引
// 重复插入报错 → 捕获返回成功（幂等）
```
方案二：版本号/状态机
```
// ① 消息带版本号/目标状态
// ② 处理前校验状态
// ③ 已处理过 → 跳过

// 状态机：PENDING → PROCESSED
// 只有 PENDING 才能处理
// 重复消息看到 PROCESSED → 跳过
```
方案三：消费事务
```
// ① 消费 + 处理 + 提交 offset 在一个事务
// ② read_committed 隔离
// ③ 端到端精确一次

// 代价：性能损耗、复杂度
// 一般不用，业务幂等够
```
### 顺序保证方案
保证分区内有序
```
// ① 生产端：同一 key 进同一分区
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
// key = 业务 ID
producer.send(new ProducerRecord<>("topic", "order-" + orderId, msg));
// 同一 orderId → 哈希到同一分区 → 有序

// ② 生产端：幂等（重试不乱序）
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
// 幂等 + 分区内有序（重试也不乱）

// ③ 消费端：单线程消费一个分区
// 一个消费者处理一个分区（默认）
// 不要在一个分区上多线程消费
```
多线程消费的乱序
```
// ❌ 错误：一个分区多线程处理
while (true) {
    ConsumerRecords records = consumer.poll();
    records.forEach(msg -> executor.submit(() -> process(msg)));
    // 多线程并发处理 → 乱序！
}

// ✅ 有序：单线程处理（或按 key 分线程）
records.forEach(msg -> process(msg));  // 顺序处理
// 或用"按 key 单线程"（keyed worker）
```
### 实战处理套路
防重复+保序完整方案
```
// 需求：订单状态流转（创建→支付→发货）不重复不乱序

// ① 生产端：
//    key = orderId（同一订单同一分区）
//    幂等开启（重试不乱序）

// ② 消费端：
//    单分区单线程（保序）
//    唯一键去重（防重复）

// ③ 状态机兜底：
//    只有当前状态的前置状态才能流转
//    重复/乱序消息 → 状态不符 → 拒绝/忽略

public void onOrderEvent(OrderEvent event) {
    // ① 幂等：已处理过则跳过
    if (idempotentService.isProcessed(event.getEventId())) {
        return;
    }

    // ② 状态机：校验前置状态
    Order order = orderService.getById(event.getOrderId());
    if (!StateMachine.canTransition(order.getStatus(), event.getType())) {
        log.warn("非法状态流转: {} -> {}", order.getStatus(), event.getType());
        return;  // 乱序/重复，忽略
    }

    // ③ 处理 + 标记已处理
    orderService.process(event);
    idempotentService.markProcessed(event.getEventId());
}
```
### 高频面试题
#### 题目1：kafka为什么重复消费
```
// offset 未提交崩溃 / Rebalance / 重试
// At-least-once 语义必然可能重复
```
#### 题目2：怎么解决重复
```
// 业务幂等：唯一键去重、状态机、唯一约束
// 或消费事务（Exactly-once）
```
#### 题目3：为什么乱序
```
// 跨分区天然无序
// 重试可能乱序（幂等解决）
// 多线程消费乱序
```
#### 题目4：怎么保证有序
```
// 同一 key 同一分区
// 幂等生产（重试不乱）
// 单分区单线程消费
```
#### 题目5：有序和吞吐怎么平衡
```
// 分区内有序 = 单线程（吞吐降）
// 用"分区粒度"并行（不同 key 并行）
```
#### 题目6：幂等和事务选哪个
```
// 一般业务：业务幂等（简单通用）
// 严格 Exactly-once：消费事务（复杂）
```
