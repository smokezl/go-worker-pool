package go_worker_pool

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultTimeout = 10 * time.Minute
	defaultWorkNum = 10

	workerName = "goWorker"
)

var (
	errItemChanClosed = errors.New("item chan closed")
)

type GoWorkerConfig struct {
	// goWorker 运行最大运行时间，-1 为不限时，直到生产者全部生产完毕为止，默认 defaultTimeout
	Timeout time.Duration
	// goWorker 消费者最大数量，默认 defaultWorkNum
	WorkerNum int
	// 是否是阻塞调用，默认 false
	Sync bool
}

// 并行任务共享通道
type goWorkerItem struct {
	itemChan       chan interface{}
	itemChanClosed int32
}

func (item *goWorkerItem) isItemChanClosed() bool {
	return item.itemChanClosed >= 1
}

// 消费者数量监听通道，用于并行任务初始化或由于panic等导致异常退出的信号
type goWorkerSign struct {
	workerFailedSign chan struct{}
	isSignChanClosed int32
}

type goWorker struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	workerNum  int
	finishNum  int64
	sync       bool
	finishFunc func()
	errFunc    func(err error)
	wg         sync.WaitGroup
	*goWorkerItem
	*goWorkerSign
}

func NewGoWorker(ctx context.Context, c *GoWorkerConfig) *goWorker {
	if c.WorkerNum <= 0 {
		c.WorkerNum = defaultWorkNum
	}
	if c.Timeout <= 0 && c.Timeout != -1 {
		c.Timeout = defaultTimeout
	}
	worker := &goWorker{
		workerNum: c.WorkerNum,
		sync:      c.Sync,
		goWorkerItem: &goWorkerItem{
			// 缓冲通道，长度为2倍的WorkerNum
			itemChan: make(chan interface{}, 2*c.WorkerNum),
		},
		goWorkerSign: &goWorkerSign{
			// 缓冲通道，长度为1倍的WorkerNum
			workerFailedSign: make(chan struct{}, c.WorkerNum),
		},
	}
	if c.Timeout != -1 {
		worker.ctx, worker.cancelFunc = context.WithDeadline(ctx, time.Now().Add(c.Timeout))
	} else {
		worker.ctx, worker.cancelFunc = context.WithCancel(ctx)
	}
	worker.initWorker()
	return worker
}

func (g *goWorker) initWorker() {
	// 将需要启动的 goroutine 数量放入监听通道中
	for i := 0; i < g.workerNum; i++ {
		g.workerFailedSign <- struct{}{}
	}
	if g.sync {
		g.wg.Add(g.workerNum)
	}
}

func (g *goWorker) RegisterFinishFunc(f func()) {
	g.finishFunc = f

}
func (g *goWorker) RegisterErrFunc(f func(err error)) {
	g.errFunc = f
}

func (g *goWorker) IterationProducer(list interface{}) (err error) {
	if g.isItemChanClosed() {
		return errItemChanClosed
	}
	defer func() {
		if err != nil {
			// 生产者中关闭通道
			g.shutdownItemChan()
		}
	}()
	v := reflect.ValueOf(list)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array && v.Kind() != reflect.Map {
		err = errors.New("param must be slice | array | map")
		return
	}
	l := v.Len()
	if l <= 0 {
		err = errors.New("param elem length is zero")
		return
	}
	if v.Kind() == reflect.Map {
		mapKeys := v.MapKeys()
		for _, mapKey := range mapKeys {
			goOn := g.pushItem(v.MapIndex(mapKey).Interface())
			if !goOn {
				err = errItemChanClosed
				return

			}
		}
	} else {
		for i := 0; i < l; i++ {
			goOn := g.pushItem(v.Index(i).Interface())
			if !goOn {
				err = errItemChanClosed
				return
			}
		}
	}
	return
}

func (g *goWorker) PushProducerItem(item interface{}) error {
	if g.isItemChanClosed() {
		return errItemChanClosed
	}
	goOn := g.pushItem(item)
	if !goOn {
		// 生产者中关闭通道
		g.shutdownItemChan()
		return errItemChanClosed
	}
	return nil
}

func (g *goWorker) IterationConsumer(f func(ctx context.Context, item interface{})) {
	go g.startWorker(f)
	if g.sync {
		g.wg.Wait()
	}
}

func (g *goWorker) startWorker(f func(ctx context.Context, item interface{})) {
	for _ = range g.workerFailedSign {
		go g.doConsumer(f)
	}
}

func (g *goWorker) doConsumer(f func(ctx context.Context, item interface{})) {
	defer func() {
		if err := recover(); err != nil {
			// 异常处理
			err := fmt.Errorf("%s-doConsumer-panic err,%v", workerName, err)
			g.dealErrFunc(err)
			g.addFailedSign()
			return
		}
		g.consumerDoneEvent()
	}()
	for {
		select {
		case item, ok := <-g.itemChan:
			if !ok {
				// 如果通道关闭并且通道中没有数据，return
				return
			}
			select {
			case <-g.ctx.Done():
				// 消费者绝对不允许关闭通道，这里只打个日志就好
				err := fmt.Errorf("%s-doConsumer err, %s", workerName, g.ctx.Err())
				g.dealErrFunc(err)
				return
			default:
				f(g.ctx, item)
			}
		case <-g.ctx.Done():
			// 消费者绝对不允许关闭通道，这里只打个日志就好
			err := fmt.Errorf("%s-doConsumer err, %s", workerName, g.ctx.Err())
			g.dealErrFunc(err)
			return
		}
	}
}

func (g *goWorker) consumerDoneEvent() {
	finishNum := atomic.AddInt64(&g.finishNum, 1)
	if int(finishNum) == g.workerNum {
		// 关闭通道
		g.closeFailedSignChan()
		// goWorker处理结束后，执行通知函数
		if g.finishFunc != nil {
			g.finishFunc()
		}
	}
	if g.sync && int(finishNum) <= g.workerNum {
		g.wg.Done()
	}
}

func (g *goWorker) pushItem(item interface{}) bool {
	select {
	case g.itemChan <- item:
		select {
		case <-g.ctx.Done():
			err := fmt.Errorf("%s-pushItem err, %s", workerName, g.ctx.Err())
			g.dealErrFunc(err)
			return false
		default:
		}
	case <-g.ctx.Done():
		err := fmt.Errorf("%s-pushItem err, %s", workerName, g.ctx.Err())
		g.dealErrFunc(err)
		return false
	}
	return true
}

func (g *goWorker) addFailedSign() {
	g.workerFailedSign <- struct{}{}
}

func (g *goWorker) dealErrFunc(err error) {
	if g.errFunc == nil {
		return
	}
	g.errFunc(err)
}

func (g *goWorker) closeFailedSignChan() {
	if atomic.CompareAndSwapInt32(&g.isSignChanClosed, 0, 1) {
		close(g.workerFailedSign)
	}
}

func (g *goWorker) CloseItemChan() {
	if atomic.CompareAndSwapInt32(&g.itemChanClosed, 0, 1) {
		close(g.itemChan)
	}
}

func (g *goWorker) shutdownItemChan() {
	g.CloseItemChan()
	// 把队列里的数据都扔掉
	for {
		_, valid := <-g.itemChan
		if !valid {
			break
		}
	}
}

func (g *goWorker) Shutdown() {
	g.cancelFunc()
}
