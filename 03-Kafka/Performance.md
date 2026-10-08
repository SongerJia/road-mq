## 高性能
高性能总览->生产端优化->Broker优化->消费端优化->参数配置->面试高频题
### 高性能总览
```
// Kafka 高吞吐的完整链路：
// 生产端：批量 + 压缩 + 分区并行
// Broker：顺序写 + 页缓存 + 零拷贝
// 消费端：批量拉取 + 分区并行

Producer：批量、压缩、并行发送
    ↓
Broker：顺序写、页缓存、零拷贝
    ↓
Consumer：批量拉取、并行消费
```
### 生产端优化
批量发送 batch
```
// 消息先攒一批再发（batch）
// 一次请求多条消息（减少网络往返）

props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);    // 批量大小 32KB
props.put(ProducerConfig.LINGER_MS_CONFIG, 5);          // 攒 5ms 再发
props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);  // 缓冲 32MB

// 注意：
// linger.ms 越大 → 批量越大 → 吞吐越高（但延迟变大）
// 延迟敏感：linger=0（立即发）
// 吞吐优先：linger=5~10ms
```
压缩
```
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
// gzip / snappy / lz4 / zstd

// 好处：
// ① 减少网络传输
// ② 减少磁盘占用
// ③ 批量时收益最大

// 代价：CPU 压缩开销
// snappy 是平衡选择（快 + 压缩比不错）
```
分区并行
```
// 多个分区可以并行发送（多线程）
// 分区数 = 并行度

// 一个分区内：顺序写入（保证有序）
// 多个分区：并行（吞吐翻倍）

// 分区数设计：
// 太少：并行不够
// 太多：管理开销、文件多
// 一般按峰值吞吐预估
```
### Broker优化
顺序写
```
// 消息追加写（append-only）
// 顺序 IO（无磁头跳转）
// 现代磁盘顺序写 500MB/s+

// 对比随机写：只有 20MB/s 左右
// 这是 Kafka 敢用磁盘的原因
```
页缓存
```
// 用 OS PageCache（不自己管理）
// 写先入内存、读命中内存
// 整个可用内存都是缓存

// 参数：
// 不需要配置（OS 自动）
// 内存足够大 → 缓存命中高
```
零拷贝
```
// sendfile：磁盘 → 内核 → 网卡
// 跳过应用层（4 次拷贝 → 2 次）

// 消费端拉取消息用零拷贝
// 大消息/高吞吐时收益明显

// 应用层参数：不配置（自动）
```
### 消费端优化
批量拉取
```
// 消费者一次拉一批（fetch）
// 不是一条一条（减少往返）

props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);  // 一次最多 500 条
props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024 * 1024);  // 攒 1MB 才拉
props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);  // 最多等 500ms

// 平衡：
// 批量大 → 吞吐高（延迟大）
// 批量小 → 延迟低（吞吐低）
```
分区并行
```
// 一个分区一个消费者线程
// 分区数越多 → 并行度越高

// 注意：
// 消费者数 > 分区数 → 部分消费者空闲
// 分区数 > 消费者数 → 一个消费者处理多分区

// 最优：消费者数 ≈ 分区数
```
### 完整参数配置
```properties
# Producer（高吞吐配置）
batch.size=32768              # 批量 32KB
linger.ms=10                  # 攒 10ms
compression.type=snappy       # 压缩
buffer.memory=67108864        # 缓冲 64MB
acks=1                        # 性能与可靠平衡

# Consumer（高吞吐配置）
fetch.min.bytes=1048576       # 攒 1MB 拉
fetch.max.wait.ms=500         # 最多等 500ms
max.poll.records=1000         # 一次 1000 条
enable.auto.commit=false      # 手动提交

# Broker
num.network.threads=3         # 网络线程
num.io.threads=8              # IO 线程
log.segment.bytes=1073741824  # 段 1GB
```
调优思路
```
// ① 先看瓶颈在哪：
//    - 网络？→ 批量 + 压缩
//    - 磁盘？→ 顺序写已优化，看刷盘
//    - CPU？→ 压缩级别调低
//    - 内存？→ 页缓存（加内存）
// ② 一次只改一个参数（对比）
// ③ 压测验证（吞吐、延迟、CPU）
```
### 面试高频题
#### 题目1：kafka高性能有哪些手段
```
// 生产：批量、压缩、分区并行
// Broker：顺序写、页缓存、零拷贝
// 消费：批量拉取、分区并行
```
#### 题目2：批量有什么代价
```
// 攒批增加延迟
// 延迟敏感：linger=0
// 吞吐优先：linger 大些
```
#### 题目3：压缩选什么
```
// snappy（平衡）常用
// gzip（压缩比高、CPU 高）
// 批量大时压缩收益大
```
#### 题目4：分区数和并行
```
// 分区 = 并行度
// 消费者数 ≈ 分区数最优
```
#### 题目5：消费端怎么高吞吐
```
// 批量拉取（fetch.min.bytes）
// 多分区并行
```
#### 题目6：高吞吐和高可靠怎么平衡
```
// 高吞吐：acks=0/1、批量、压缩
// 高可靠：acks=all、幂等、重试
// 一般用 acks=1 + 重试（平衡）
```