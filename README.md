<h1 align="center">Redis 抢占式队列</h1>

<p align="center">
  <i>一个优雅的、基于 Redis 的抢占式队列消费机制，适用于分布式系统。</i>
</p>

## 概述

**Redis 抢占式队列**是一个开源的、基于 Redis 构建的抢占式队列管理系统。它使开发者能够在分布式环境中高效地处理任务消费，并支持抢占式任务调度。本系统利用 Redis 作为可靠的键值存储，来管理任务队列和执行工作者，从而实现任务处理的灵活性和可扩展性。

## 特性

- **抢占式调度**：支持抢占式任务消费，优先处理紧急或高优先级任务。
- **Redis 后端**：利用 Redis 进行可靠且高效的任务管理。
- **可扩展的工作池**：可配置的工作池以并发处理任务。
- **灵活的回调机制**：允许注册任务前、任务后以及任务失败的回调函数，增强对任务生命周期的控制。
- **容错性**：恢复失败的任务，防止关键信息丢失。

## 安装

```sh
# 克隆仓库
git clone https://github.com/yourusername/redis-preemptive-queue.git

# 切换到项目目录
cd redis-preemptive-queue

# 安装依赖
go mod tidy
```

## 使用示例

以下是如何初始化和运行队列的示例：

```go
package main

import (
    "fmt"
    "github.com/jelech/redis-preemptive-queue"
)

func main() {
    redisURL := fmt.Sprintf("redis://:%s@%s/%d", "", "localhost", 0)
    
    q := queue.NewQuk(&queue.Conf{
        QueueName: "example_queue",
        RedisURL:  redisURL,
    })

    q.BeforeWork(func() error { return nil })
    q.AfterWork(func() error { return nil })

    q.RegistryWork("ExampleTask", exampleTask)

    q.Submit("ExampleTask", struct {Message string `json:"message"`}{Message: "Hello, Queue!"}, nil)

    if err := q.Run(); err != nil {
        fmt.Println(err)
    }
}

func exampleTask(jsonStr string) error {
    fmt.Println("执行任务，数据:", jsonStr)
    return nil
}
```

## 配置

队列系统可以通过 `Conf` 结构体进行自定义，开发者可以：
- 设置队列名称和分组。
- 配置工作者并发数量。
- 指定 Redis 连接详情。

## 任务注册

可以使用 `RegistryWork` 或 `RegistryWorks` 注册任务，从而灵活地将多个工作者分配到队列中。以下是注册多个工作者的示例：

```go
q.RegistryWorks([]queue.Qwork{
    {"TaskA", taskA},
    {"TaskB", taskB},
})
```

## 回调机制

本系统支持任务生命周期回调，可用于日志记录、监控或自定义业务逻辑：
- **BeforeWork**：在任何工作者开始处理任务之前执行。
- **AfterWork**：在工作者完成任务后调用。
- **失败任务**：可以重新加入队列或记录失败日志。

## 贡献

我们欢迎社区的贡献！如果您希望改进这个项目，请随时 fork 仓库并提交 pull request。要报告问题，请使用[issues 页面](https://github.com/jelech/redis-preemptive-queue/issues)。

## 许可证

本项目基于 MIT 许可证 - 详细信息请参阅 [LICENSE](LICENSE) 文件。

## 致谢

- 基于 [Redis](https://redis.io/) 构建
- 受分布式任务队列系统的启发
