package HKafkaQueue

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCreatePool(t *testing.T) {
	pool := NewHQueuePool("/tmp/hqueue", 60)
	fmt.Println(pool.hqueueMap)
	pool.Destroy()
}

func TestPoolDelete(t *testing.T) {
	pool := NewHQueuePool("/tmp/kqueue", 30)
	hqueue, _ := NewHQueue("test2", "/tmp/kqueue")
	hqueue2, _ := NewHQueue("test2", "/tmp/kqueue", "p2")
	hqueue2.consumerIndex.putBlockNum(5)
	readBlock, _ := NewHQueueBlock(hqueue2.consumerIndex, formatHqueueBlockPath("/tmp/kqueue", "test2", hqueue2.consumerIndex.blockNum))
	hqueue2.readBlock = readBlock
	pool.hqueueMap["test2"] = hqueue
	var w sync.WaitGroup
	w.Add(2)
	go read(hqueue2, &w)
	go write(hqueue, &w)
	w.Wait()
	pool.Destroy()
}

func write(hqueue *HQueue, w *sync.WaitGroup) {
	var i = 0
	for {
		hqueue.Offer([]byte("Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co."))
		i = i + 1
		if i > 1000000 {
			hqueue.Close()
			w.Done()
			break
		}
	}
	fmt.Println(hqueue.producerIndex.counter)
	fmt.Println(i)
}

func read(hqueue *HQueue, w *sync.WaitGroup) {
	var i = 0
	for {
		_, err := hqueue.Poll()
		if err != nil {
			continue
		} else {
			//fmt.Println(String(bytes))
			i = i + 1
		}
		if i == 1000000 {
			time.Sleep(time.Second * 60)
			hqueue.Close()
			w.Done()
			break
		}
	}
}
