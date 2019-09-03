go-worker-pool
===========
基于 goroutine 和 channel 的生产和消费工具，可以启动`一个生产者`和可配置数量的`多个消费者`，支持取消、超时，自动销毁。可以捕获消费者panic事件，影响消费者数量。
### 安装
```
go get -u github.com/smokezl/go-worker-pool
```

### 导入
```go
import "github.com/smokezl/govalidators"
```

### 基本使用方式
##### 初始化
 ```go
worker := NewGoWorker(ctx, &GoWorkerConfig{
	Timeout:   10 * time.Minute,
    WorkerNum: 20,
    // Sync 表示是否需要调用 waitGroup
    Sync:  false,
})
```
##### 针对单个生产者一次或多次 push slice、array或map
```go
// 生产者逻辑
go func() {
    // 不要忘记关闭生产者通道
    defer worker.CloseItemChan()
    for i := 0; i < 10; i++ {
        var arr []int
        for j := 0; j < 100; j++ {
            arr = append(arr, j)
        }
        err = worker.IterationProducer(arr)
        if err != nil {
            // err
            return
        }
    }
}()
worker.RegisterFinishFunc(func() {
    // 注册退出执行函数
    // worker执行完退出或销毁时触发
})
worker.RegisterErrFunc(func(err error) {
    // 注册错误执行函数
    // worker出错（超时或者panic时触发）
})
// 消费者逻辑
worker.IterationConsumer(func(ctx context.Context, item interface{}) {
    num := item.(int)
    // 执行消费者代码
})
```

##### 针对单个生产者多次 push 单个元素
```go
// 生产者逻辑
go func() {
    // 不要忘记关闭生产者通道 
    defer worker.CloseItemChan()
    for i := 0; i < 100; i++ {
        err = worker.PushProducerItem(i)
        if err != nil {
            // err
            return
        }
    }
}()
// 消费者逻辑同上
```
##### 更多用法可以查看 test 文件
