## 消费模式
Push vs Pull->Push本质->消费代码->消费进度->重试机制->死信队列->面试高频题
### Push vs Pull
| 模式          | 说明              | 使用                    |
| ----------- | --------------- | --------------------- |
| **Push（推）** | MQ 推送消息给消费者（默认） | DefaultMQPushConsumer |
| **Pull（拉）** | 消费者主动拉取消息       | DefaultMQPullConsumer |
```
// Push（默认，常用）
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group");
consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
    // 自动收到消息
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
});

// Pull（手动）
DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("group");
PullResult result = consumer.pull(...);
// 手动拉取、手动更新 offset
```
### Push的本质是长轮询
Push是推吗
```
// 答案：不是真推！本质是"长轮询"（Long Polling）

// 流程：
// ① 消费者主动请求 Broker（拉取）
// ② Broker 没有新消息 → 不立即返回，挂起请求（最多 15s）
// ③ 有消息到了 → 立即返回给消费者
// ④ 没消息超时 → 返回空，消费者再发新请求

// 好处：
// ① 消息一到就返回（近实时）
// ② 不占太多连接（比短轮询高效）
// ③ 服务端不主动连客户端（省资源）

// 所以叫"Push"是因为体验像推（消息主动来了）
// 实际是"服务端阻塞的长轮询"
```
长轮询 vs 短轮询 vs 真推送

|模式|机制|实时性|资源|
|---|---|---|---|
|短轮询|定期问（间隔）|差|高|
|**长轮询**|挂着等（有就返回）|好|中 ✅|
|真推送|服务端主动推|最好|高|
### 面试回答

> **Q:** "RocketMQ 的 Push 是真推吗？" 
> **A:** "不是。RocketMQ 的 Push 模式本质是**长轮询**：消费者向 Broker 发起拉取请求，Broker 没有新消息时挂起请求（默认最长 15 秒），有新消息立刻返回。既保证了近实时性，又比短轮询节省资源。所以叫 Push 只是体验像推送。"
### 消费代码（Push模式）
```
// 创建消费者
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order-group");
consumer.setNamesrvAddr("127.0.0.1:9876");

// 订阅
consumer.subscribe("order-topic", "*");

// 注册消息监听（核心）
consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
    for (MessageExt msg : msgs) {
        // ① 处理业务
        process(msg);

        // ② 返回成功（确认消费）
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
});

// ③ 失败 → 重试
return ConsumeConcurrentlyStatus.RECONSUME_LATER;

// 启动
consumer.start();
```
消息返回状态

|状态|含义|
|---|---|
|CONSUME_SUCCESS|消费成功（提交进度）|
|RECONSUME_LATER|失败，稍后重试|
### 消费进度管理
```
// 集群模式：Broker 存（consumerOffset.json）
// 广播模式：本地存

// 作用：
// ① 断点续传
// ② 防重复（已消费的跳过）
// ③ 回溯（重置 offset 重新消费）
```
自动 vs 手动提交
```
// ① 自动提交（默认）：
//    返回 CONSUME_SUCCESS → 自动更新 offset
// ② 手动提交：
//    consumer.setConsumeTimestamp/重置 offset

// 回溯消费：
consumer.resetOffset("order-group", "order-topic", 0);
// 从最早重新消费
```
### 重试机制
消息失败重试
```
// ① 返回 RECONSUME_LATER → 进入重试队列
// ② 重试 16 次（默认），间隔递增
// ③ 16 次仍失败 → 进入死信队列

// 重试间隔（递增）：
// 10s、30s、1m、2m、3m ... 10m、30m、1h、2h
```
重试注销
```
// ① 重试是"投递重试"（重新投递，不是重新拉取）
// ② 重试消息会进重试队列（%RETRY%groupName）
// ③ 重试前要保证"幂等"（重复消费无害）
// ④ 快速失败 → 延迟会拉长队列（影响其他消息？不会，独立队列）
```
顺序消费的重试不同
```
// 并发消费：失败 → RECONSUME_LATER（重试队列）
// 顺序消费：失败 → 阻塞队列重试（默认）

// 顺序消费失败：
// ① 锁住队列，暂停后续消息
// ② 重试处理当前消息
// ③ 连续失败超过次数 → 跳过（解锁，避免死锁）
```
### 死信队列 DLQ
```
// 死信队列（Dead Letter Queue）：
// 消息重试 16 次仍失败 → 进入死信队列
// 需要人工/程序处理

// 死信队列命名：
// %DLQ%groupName
// （按消费组区分，每个组一个）
```
死信产生的原因
```
// ① 业务逻辑错误（处理代码 bug）
// ② 依赖的服务一直不可用
// ③ 数据异常（消息格式问题）
// ④ 重复失败（逻辑上无法处理）
```
处理死信
```
// ① 监控死信队列（数量告警）
// ② 人工排查（看消息内容）
// ③ 修复后重投（重建消息重新发送）
// ④ 程序自动处理（重投/记录/忽略）

// 生产最佳实践：
// 死信必须"可见可查"（控制台/监控）
// 不能默默堆积！
```
### 面试高频题
#### Push和Pull的区别
```
// Push：长轮询（默认，体验像推）
// Pull：手动拉取（更可控）
```
#### Push是真推吗
```
// 不是！是长轮询
// Broker 挂起请求等消息
```
#### 消费失败怎么办
```
// 返回 RECONSUME_LATER → 重试队列
// 16 次失败 → 死信队列
```
#### 死信队列是什么
```
// 重试仍失败的消息进 DLQ
// 需人工/程序处理
```
#### 怎么防止重复消费
```
// ① 消费成功才 ack
// ② 业务幂等（重试时无害）
```
#### 消费进度存在哪
```
// 集群：Broker
// 广播：本地
```
