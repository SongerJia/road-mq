## 事务消息
解决的问题->传统方案的问题->半消息机制->完整流程->代码实现->与2PC和本地消息表对比->面试高频题
### 解决什么问题
场景：本地事务+发消息
```
// 需求：创建订单 + 发"订单创建"消息给库存服务
// ① 本地：订单落库（DB 事务）
// ② 发消息：通知库存扣减

// 难点：两个操作要"一起成功或一起失败"
// ① 先落库后发消息：消息发送失败 → 订单有了但库存不知道
// ② 先发消息后落库：落库失败 → 消息发了但订单没有

// 需要：保证"DB 操作"和"发消息"最终一致
```
### 传统方案的痛点
#### 方案1：本地消息表
```
// ① 本地事务：业务 + 消息表（同库）
// ② 定时扫描消息表，重发未成功的消息
// ③ 消费端幂等

// 问题：要建消息表、要定时任务（重活）
// 但：可靠，通用
```
#### 方案2：事务消息
```
// RocketMQ 把"消息表 + 定时重发"内置了
// 直接发事务消息 → MQ 帮你保证最终一致
// 代码更简单
```
### 半消息机制
```
// 半消息（Half Message）：
// ① 消息先发送到 Broker
// ② 但"暂不可见"（消费者看不到）
// ③ 等本地事务提交 → 才真正可见（可消费）

// 类比：
// 先把信送到邮局（Broker），但盖上"暂不发"章
// 你确认 OK 了，邮局才真正投递
```
两种状态
```
// ① 半消息（未确认）：存着，消费者不可见
// ② 确认后：投递状态，消费者可见

// 事务消息 = 半消息 + 本地事务 + 回查
```
### 完整流程
事务消息流程
```
Producer 发送事务消息
    │
    ├── ① 发送半消息（Broker 暂存，消费者不可见）
    │
    ├── ② 执行本地事务（订单落库）
    │
    ├── ③ 返回事务状态给 Broker
    │    ├── COMMIT → 半消息变可见 → 消费者可消费 ✅
    │    ├── ROLLBACK → 删除半消息 ❌
    │    └── UNKNOWN → 不知道，等待回查
    │
    └── ④ 如果 Broker 长时间没收到状态（UNKNOWN）
        → 回查（回调本地事务检查）
        → 根据结果 COMMIT 或 ROLLBACK
```
回查机制
```
// 问题：网络异常/进程崩溃，Broker 一直没收到事务状态
// 解决：Broker 定时回查（默认 60s 一次，最多 15 次）

// 回查流程：
// ① Broker 发现半消息一直未确认
// ② 调用 Producer 的 checkLocalTransaction()
// ③ 检查本地事务是否成功
// ④ 返回 COMMIT / ROLLBACK

// 关键：本地事务必须"可查询"（幂等查询）
// 例：查订单是否存在 → 存在=提交，不存在=回滚
```
### 代码实现
实现TransactionListener
```
@Component
public class OrderTransactionListener implements TransactionListener {

    @Autowired
    private OrderService orderService;

    // 本地事务执行（发送半消息后回调）
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        try {
            // 执行本地事务（订单落库）
            orderService.createOrder(msg);
            // 成功 → 提交消息（消费者可见）
            return LocalTransactionState.COMMIT_MESSAGE;
        } catch (Exception e) {
            // 失败 → 回滚（消息删除）
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
    }

    // 回查（Broker 没收到状态时回调）
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        // 查询本地事务是否成功
        Long orderId = Long.valueOf(msg.getKeys());
        boolean exists = orderService.isOrderExists(orderId);

        return exists
            ? LocalTransactionState.COMMIT_MESSAGE   // 已提交
            : LocalTransactionState.ROLLBACK_MESSAGE; // 未提交
    }
}
```
发送事务消息
```
@Autowired
private TransactionMQProducer producer;

public void createOrderWithMessage(OrderRequest req) {
    Message msg = new Message("order-topic", "created",
        req.getOrderId().toString(),   // Key（业务键）
        jsonBody(req).getBytes());

    // 发送事务消息（arg 传给 executeLocalTransaction）
    TransactionSendResult result = producer.sendMessageInTransaction(
        msg, req);

    // 状态：
    // SEND_OK：发送成功（后续看事务状态）
    // 事务状态决定消息是否可见
}
```
### 与其他方案对比
| 对比       | 事务消息     | 本地消息表    | XA/2PC | TCC    |
| -------- | -------- | -------- | ------ | ------ |
| **侵入性**  | 低（MQ 封装） | 中（建表+定时） | 高      | 高      |
| **实现**   | MQ 内置    | 手写       | 数据库支持  | 手写     |
| **性能**   | 高        | 中        | 低（锁）   | 中      |
| **适用范围** | 本地事务+发消息 | 本地事务+发消息 | 跨库强一致  | 跨服务强一致 |
| **可靠性**  | 高（回查）    | 高        | 高      | 高      |
选型
```
// ① 本地事务 + 发消息 → RocketMQ 事务消息 ✅（最简单）
// ② 跨服务多个操作 → TCC/Saga（Seata）
// ③ 跨库强一致 → XA（少用）
// ④ 已用本地消息表 → 可迁到事务消息（省代码）
```
事务消息 vs 本地消息表
```
// 本质相同：都是"消息暂存 + 定时确认"
// 区别：
// 本地消息表：自己建表 + 定时任务
// 事务消息：MQ 内置半消息 + 回查

// 事务消息 = 本地消息表的"框架化封装"
```
### 面试高频题
#### 事务消息是什么
```
// 半消息 + 本地事务 + 回查
// 保证本地事务和发消息的最终一致
```
#### 半消息是什么
```
// 发到 Broker 但暂不可见的消息
// 本地事务提交后才可见
```
#### 回查机制
```
// Broker 长时间没收到状态 → 回调检查本地事务
// 最多 15 次
```
#### 为什么能保证一致性
```
// ① 半消息暂存（本地事务前不投递）
// ② 本地事务成功 → 提交消息
// ③ 本地事务失败 → 回滚消息
// ④ 不确定 → 回查确认
// → 消息和本地事务要么都有要么都无
```
#### 回查要满足什么
```
// 本地事务必须"可查询"（幂等）
// 例：查订单存在与否
```
#### 和本地消息表区别
```
// 本质一样，事务消息是框架化封装（更简单）
```

