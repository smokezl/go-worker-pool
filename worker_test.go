package go_worker_pool

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testIterationProducer(needWait bool, t *testing.T) {
	var (
		funcName         = "IterationProducer"
		arr              []int
		err              error
		maxNum                 = 100
		sunNum           int64 = 0
		compareNum       int64 = 0
		compareSumNum    int64 = 0
		finishStr              = ""
		compareFinishStr       = "woker finish"
	)
	ctx, _ := context.WithCancel(context.Background())
	worker := NewGoWorker(ctx, &GoWorkerConfig{
		Timeout:   10 * time.Minute,
		WorkerNum: 10,
		Sync:      needWait,
	})
	for i := 0; i < maxNum; i++ {
		arr = append(arr, i)
		sunNum += int64(i)
	}
	go func() {
		err = worker.IterationProducer(arr)
		assert.NoError(t, err, funcName)
		worker.CloseItemChan()
	}()
	worker.RegisterFinishFunc(func() {
		finishStr = compareFinishStr
	})
	worker.RegisterErrFunc(func(err error) {
		assert.NoError(t, err, funcName)
	})
	worker.IterationConsumer(func(ctx context.Context, item interface{}) {
		num := item.(int)
		atomic.AddInt64(&compareNum, 1)
		atomic.AddInt64(&compareSumNum, int64(num))
	})
	if !needWait {
		time.Sleep(time.Millisecond * 50)
	}
	assert.Equal(t, int64(maxNum), compareNum, funcName+"maxNum")
	assert.Equal(t, finishStr, compareFinishStr, funcName+"finishStr")
	assert.Equal(t, sunNum, compareSumNum, funcName+"sunNum")
}

func TestIterationProducer(t *testing.T) {
	testIterationProducer(false, t)
}

func TestIterationProducerWait(t *testing.T) {
	testIterationProducer(true, t)
}

func testIterationProducerCancel(needWait bool, t *testing.T) {
	var (
		funcName         = "IterationProducerCancel"
		arr              []int
		err              error
		maxNum                 = 200
		sunNum           int64 = 0
		compareSumNum    int64 = 0
		finishStr              = ""
		compareFinishStr       = "woker finish"
	)
	ctx, cancel := context.WithCancel(context.Background())
	worker := NewGoWorker(ctx, &GoWorkerConfig{
		Timeout:   10 * time.Minute,
		WorkerNum: 20,
		Sync:      needWait,
	})
	for i := 0; i < maxNum; i++ {
		arr = append(arr, i)
		sunNum += int64(i)
	}
	go func() {
		err = worker.IterationProducer(arr)
		assert.Error(t, err, funcName)
		worker.CloseItemChan()
	}()
	worker.RegisterFinishFunc(func() {
		finishStr = compareFinishStr
	})
	worker.RegisterErrFunc(func(err error) {
		assert.Error(t, err, funcName)
	})
	worker.IterationConsumer(func(ctx context.Context, item interface{}) {
		num, _ := item.(int)
		atomic.AddInt64(&compareSumNum, int64(num))
		if num == 100 {
			cancel()
		}
	})
	if !needWait {
		time.Sleep(time.Millisecond * 50)
	}
	assert.Equal(t, finishStr, compareFinishStr, funcName+"finishStr")
	assert.NotEqual(t, sunNum, compareSumNum, funcName+"sunNum")
}

func TestIterationProducerCancel(t *testing.T) {
	testIterationProducerCancel(false, t)
}

func TestIterationProducerCancelWait(t *testing.T) {
	testIterationProducerCancel(true, t)
}

func testIterationProducerTimeout(needWait bool, t *testing.T) {
	var (
		funcName         = "IterationProducerTimeout"
		arr              []int
		err              error
		maxNum                 = 100
		sunNum           int64 = 0
		compareSumNum    int64 = 0
		finishStr              = ""
		compareFinishStr       = "woker finish"
	)
	ctx, _ := context.WithCancel(context.Background())
	worker := NewGoWorker(ctx, &GoWorkerConfig{
		Timeout:   100 * time.Millisecond,
		WorkerNum: 10,
		Sync:      needWait,
	})
	for i := 0; i < maxNum; i++ {
		arr = append(arr, i)
		sunNum += int64(i)
	}
	go func() {
		err = worker.IterationProducer(arr)
		assert.Error(t, err, funcName)
		worker.CloseItemChan()
	}()
	worker.RegisterFinishFunc(func() {
		finishStr = compareFinishStr
	})
	worker.RegisterErrFunc(func(err error) {
		assert.Error(t, err, funcName)
	})
	worker.IterationConsumer(func(ctx context.Context, item interface{}) {
		num, _ := item.(int)
		atomic.AddInt64(&compareSumNum, int64(num))
		time.Sleep(20 * time.Millisecond)
	})
	if !needWait {
		time.Sleep(time.Millisecond * 150)
	}
	assert.Equal(t, finishStr, compareFinishStr, funcName+"finishStr")
	assert.NotEqual(t, sunNum, compareSumNum, funcName+"sunNum")
}

func TestIterationProducerTimeout(t *testing.T) {
	testIterationProducerTimeout(false, t)
}

func TestIterationProducerTimeoutWait(t *testing.T) {
	testIterationProducerTimeout(true, t)
}

func testIterationProducerShutdown(needWait bool, t *testing.T) {
	var (
		funcName         = "IterationProducerShutdown"
		arr              []int
		err              error
		maxNum                 = 200
		sunNum           int64 = 0
		compareSumNum    int64 = 0
		finishStr              = ""
		compareFinishStr       = "woker finish"
	)
	ctx, _ := context.WithCancel(context.Background())
	worker := NewGoWorker(ctx, &GoWorkerConfig{
		Timeout:   10 * time.Minute,
		WorkerNum: 20,
		Sync:      needWait,
	})
	for i := 0; i < maxNum; i++ {
		arr = append(arr, i)
		sunNum += int64(i)
	}
	go func() {
		err = worker.IterationProducer(arr)
		assert.Error(t, err, funcName)
		worker.CloseItemChan()
	}()
	worker.RegisterFinishFunc(func() {
		finishStr = compareFinishStr
	})
	worker.RegisterErrFunc(func(err error) {
		assert.Error(t, err, funcName)
	})
	worker.IterationConsumer(func(ctx context.Context, item interface{}) {
		num, _ := item.(int)
		atomic.AddInt64(&compareSumNum, int64(num))
		if num == 100 {
			worker.Shutdown()
		}
	})
	if !needWait {
		time.Sleep(time.Millisecond * 50)
	}
	assert.Equal(t, finishStr, compareFinishStr, funcName+"finishStr")
	assert.NotEqual(t, sunNum, compareSumNum, funcName+"sunNum")
}

func TestIterationProducerShutdown(t *testing.T) {
	testIterationProducerShutdown(false, t)
}

func TestIterationProducerShutdownWait(t *testing.T) {
	testIterationProducerShutdown(true, t)
}

func testGoWorkerPush(needWait bool, t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var (
		funcName         = "GoWorkerPush"
		err              error
		maxNum                 = 100
		sunNum           int64 = 0
		compareNum       int64 = 0
		compareSumNum    int64 = 0
		finishStr              = ""
		compareFinishStr       = "woker finish"
	)
	ctx, _ := context.WithCancel(context.Background())
	worker := NewGoWorker(ctx, &GoWorkerConfig{
		Timeout:   10 * time.Minute,
		WorkerNum: 5,
		Sync:      needWait,
	})
	go func() {
		for i := 0; i < maxNum; i++ {
			sunNum += int64(i)
			err = worker.PushProducerItem(i)
			assert.NoError(t, err, funcName)
		}
		worker.CloseItemChan()
	}()
	worker.RegisterFinishFunc(func() {
		finishStr = compareFinishStr
	})
	worker.RegisterErrFunc(func(err error) {
		assert.NoError(t, err, funcName)
	})
	worker.IterationConsumer(func(ctx context.Context, item interface{}) {
		num := item.(int)
		atomic.AddInt64(&compareNum, 1)
		atomic.AddInt64(&compareSumNum, int64(num))
	})
	if !needWait {
		time.Sleep(time.Millisecond * 50)
	}
	assert.Equal(t, int64(maxNum), compareNum, funcName+"maxNum")
	assert.Equal(t, finishStr, compareFinishStr, funcName+"finishStr")
	assert.Equal(t, sunNum, compareSumNum, funcName+"sunNum")
}

func TestGoWorkerPush(t *testing.T) {
	testGoWorkerPush(false, t)
}

func TestGoWorkerPushWait(t *testing.T) {
	testGoWorkerPush(true, t)
}

func testGoWorkerPushCancel(needWait bool, t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var (
		funcName         = "GoWorkerPushCancel"
		err              error
		maxNum                 = 200
		sunNum           int64 = 0
		compareSumNum    int64 = 0
		finishStr              = ""
		compareFinishStr       = "woker finish"
	)
	ctx, cancel := context.WithCancel(context.Background())
	worker := NewGoWorker(ctx, &GoWorkerConfig{
		Timeout:   10 * time.Minute,
		WorkerNum: 5,
		Sync:      needWait,
	})
	go func() {
		for i := 0; i < maxNum; i++ {
			err = worker.PushProducerItem(i)
			if err != nil {
				assert.Error(t, err, funcName)
			}
		}
		worker.CloseItemChan()
	}()
	worker.RegisterFinishFunc(func() {
		finishStr = compareFinishStr
	})
	worker.RegisterErrFunc(func(err error) {
		assert.Error(t, err, funcName)
	})
	worker.IterationConsumer(func(ctx context.Context, item interface{}) {
		num, _ := item.(int)
		atomic.AddInt64(&compareSumNum, int64(num))
		if num == 10 {
			cancel()
		}
	})
	if !needWait {
		time.Sleep(time.Millisecond * 50)
	}
	assert.Equal(t, finishStr, compareFinishStr, funcName+"finishStr")
	assert.NotEqual(t, sunNum, compareSumNum, funcName+"sunNum")
}

func TestGoWorkerPushCancel(t *testing.T) {
	testGoWorkerPushCancel(false, t)
}

func TestGoWorkerPushCancelWait(t *testing.T) {
	testGoWorkerPushCancel(true, t)
}

func TestPanic(t *testing.T) {
	var (
		funcName         = "Panic"
		arr              []int
		err              error
		maxNum                 = 100
		compareNum       int64 = 0
		finishStr              = ""
		compareFinishStr       = "woker finish"
	)
	ctx, _ := context.WithCancel(context.Background())
	worker := NewGoWorker(ctx, &GoWorkerConfig{
		Timeout:   10 * time.Minute,
		WorkerNum: 10,
		Sync:      true,
	})
	for i := 0; i < maxNum; i++ {
		arr = append(arr, i)
	}
	go func() {
		err = worker.IterationProducer(arr)
		assert.NoError(t, err, funcName)
		worker.CloseItemChan()
	}()
	worker.RegisterFinishFunc(func() {
		finishStr = compareFinishStr
	})
	worker.RegisterErrFunc(func(err error) {
		atomic.AddInt64(&compareNum, 1)
		assert.Error(t, err, funcName)
	})
	worker.IterationConsumer(func(ctx context.Context, item interface{}) {
		panic("consumer panic")
	})
	assert.Equal(t, int64(maxNum), compareNum, funcName+"maxNum")
	assert.Equal(t, finishStr, compareFinishStr, funcName+"finishStr")
}
