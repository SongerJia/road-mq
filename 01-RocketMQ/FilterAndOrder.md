## 消息过滤与顺序
Tag过滤->SQL92过滤->过滤对比->顺序消息->顺序实现->顺序的问题->面试高频题
### 消息过滤Tag
```
// Tag：Topic 下的二级分类
// 消费者按 Tag 过滤（只消费自己关心的）

// 例：
// order-topic 下有 tag：created、paid、shipped
// 库存服务只订阅 created（需要扣库存）
// 物流服务只订阅 shipped（需要发货）
```
使用
```
// 生产：设置 Tag
Message msg = new Message("order-topic", "created", body);

// 消费：订阅指定 Tag
consumer.subscribe("order-topic", "created");      // 只收 created
consumer.subscribe("order-topic", "created || paid");  // 多 Tag
consumer.subscribe("order-topic", "*");            // 所有
```
Tag过滤的优点
```
// ① 简单（Broker 端过滤，少传数据）
// ② 节省带宽（不消费的 Tag 不传）
// ③ 逻辑清晰（业务分类）

// 缺点：
// ① 只能按"相等"匹配（不能复杂条件）
// ② 无法按消息属性过滤
```
### SQL92过滤
```
// 按消息的"属性"做复杂过滤
// 需要 Broker 开启：enablePropertyFilter=true
// 语法类似 SQL WHERE

// 例：
// 消息带属性：amount、city、vip
// 过滤：amount > 100 AND city = '北京'
```
使用
```
// 生产：设置属性
Message msg = new Message("order-topic", "created", body);
msg.putUserProperty("amount", "200");
msg.putUserProperty("city", "北京");
msg.putUserProperty("vip", "true");

// 消费：SQL 过滤
consumer.subscribe("order-topic",
    MessageSelector.bySql("amount > 100 AND city = '北京'"));
```
支持的语法
```
// 比较：=、>、<、>=、<=、<>
// 逻辑：AND、OR、NOT
// 判断：IS NULL、IS NOT NULL、IN
// 字符串：LIKE
```
两种过滤对比

|对比|Tag 过滤|SQL92 过滤|
|---|---|---|
|**匹配方式**|相等匹配|复杂表达式|
|**Broker 配置**|默认支持|需开启|
|**性能**|快|稍慢|
|**灵活性**|低|高|
|**适用**|✅ 常用简单场景|复杂条件|
选型
```
// 简单分类 → Tag（默认，推荐）
// 复杂条件 → SQL92（按属性过滤）

// 面试一句话：
// "Tag 是 Topic 下的简单分类，适合按业务类型过滤；
// SQL92 按消息属性做复杂过滤（如金额大于 100 且城市为北京）。
// 简单场景用 Tag 就够了，性能好。"
```
### 顺序消息
```
// 场景：订单状态流转
// 创建 → 支付 → 发货
// 乱序：支付先处理，创建后处理 → 状态错乱！

// 两种顺序：
// ① 全局顺序：所有消息严格有序（1 个 Queue）
// ② 分区顺序：同一业务 key 有序（推荐）
```
### 分区顺序实现
生产端：同一key->同一queue
```
// 按订单 ID 哈希选队列
// 同一个订单 → 同一个 Queue → 顺序存储

producer.send(msg, new MessageQueueSelector() {
    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        Long orderId = (Long) arg;
        // 按订单 ID 取模选队列
        int index = (int) (orderId % mqs.size());
        return mqs.get(index);
    }
}, orderId);
```
消费端：顺序消费
```
// 顺序消费：同一队列单线程按序处理
consumer.registerMessageListener(new MessageListenerOrderly() {
    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
            ConsumeOrderlyContext context) {
        for (MessageExt msg : msgs) {
            // 按序处理
            process(msg);
        }
        return ConsumeOrderlyStatus.SUCCESS;
    }
});
```
关键点
```
// ① 同一 key 必须选同一个 Queue（哈希）
// ② 消费端用"顺序消费"（单队列串行）
// ③ 不同 key 可以并行（不同 Queue）
// → 顺序 + 并行的平衡
```
### 顺序的坑
全局顺序的代价
```
// 全局顺序：整个 Topic 只有一个 Queue
// ① 吞吐极低（单队列单线程）
// ② 一个消息卡住 → 全部阻塞
// ③ 一般不用（分区顺序够）

// 面试：
// "全局顺序几乎不用，因为 1 个 Queue 吞吐太低。
// 一般用分区顺序：同一业务 key 保证有序，不同 key 并行。"
```
顺序消费失败会怎么样
```
// 顺序消费中一条失败：
// ① 默认锁住队列（阻塞后续消息）
// ② 重试处理
// ③ 连续失败 → 锁释放，跳过该消息（避免死锁）

// 注意：
// 顺序消息的"重试"会阻塞后面的消息
// 所以尽量保证处理逻辑不抛异常
```
顺序和事务的配合
```
// 顺序消息 + 幂等：
// ① 顺序保证"不乱序"
// ② 幂等保证"重复无害"
// 两者配合才可靠
```
### 面试高频题
#### Tag过滤和SQL92过滤
```
// Tag：简单相等匹配（默认推荐）
// SQL92：按属性复杂过滤（需开启）
```
#### 怎么保证顺序消息
```
// 生产：同一 key 选同一 Queue（哈希）
// 消费：顺序消费（单队列串行）
```
#### 全局顺序和分区顺序
```
// 全局：1 个 Queue（吞吐低）
// 分区：同一 key 有序（推荐）
```
#### 顺序消费失败会怎样
```
// 阻塞队列重试，连续失败跳过（避免死锁）
```
分区顺序选队列怎么选
```
// 按业务 key 哈希取模
// 同一 key → 同一队列
```
#### 顺序和幂等的关系
```
// 顺序防乱序，幂等防重复
// 配合使用
```