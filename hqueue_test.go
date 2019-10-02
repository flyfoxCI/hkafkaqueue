package HKafkaQueue

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var dataDir = "/tmp/hqueue"
var queueName = "test"

func TestNewHQueue(t *testing.T) {
	hqueue, err := NewHQueue(queueName, dataDir)
	if err != nil {
		t.Fatalf("create hqueue error %v", err)
	}
	fmt.Println(hqueue)
}

func TestQueueWrite(t *testing.T) {
	hqueue, err := NewHQueue(queueName, dataDir)
	var i = 0
	if err != nil {
		t.Fatalf("create hqueue error %v", err)
	}
	for {
		if hqueue.index.writeBlockNum > 3 {
			break
		}
		_, err := hqueue.offer([]byte("Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co."))
		if err != nil {
			t.Fatalf("write error :%v", err)
			break
		}
		i = i + 1
	}
	fmt.Println(hqueue.index.writeCounter)
	fmt.Println(i)
	hqueue.sync()

}

func TestQueueRead(t *testing.T) {
	hqueue, err := NewHQueue(queueName, dataDir)
	var i = 0
	if err != nil {
		t.Fatalf("create hqueue error %v", err)
	}
	for {
		bytes, err := hqueue.poll()
		if err != nil {
			break
		} else {
			fmt.Println(String(bytes))
			i = i + 1
		}
	}
	fmt.Println(hqueue.index.readCounter)
	fmt.Println(i)
	hqueue.sync()
}

func TestMutipleReadWrite(t *testing.T) {
	hqueue, _ := NewHQueue(queueName, dataDir)
	var w sync.WaitGroup
	w.Add(1)
	go read(hqueue)
	go write(hqueue)
	w.Wait()
}

func write(hqueue *HQueue) {
	var i = 0
	for {
		_, err := hqueue.offer([]byte("Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co."))
		if err != nil {
			continue
		}
		i = i + 1
		if i%1000 == 0 {
			fmt.Printf("write msg count:%d", i)
			fmt.Println(hqueue)
			time.Sleep(time.Second * 5)
		}
	}
}

func read(hqueue *HQueue) {
	var i = 0
	for {
		fmt.Println("read......")
		msg, err := hqueue.poll()
		if err != nil {
			if _, ok := err.(*ReadZeroError); ok {
				time.Sleep(time.Second)
			}
		} else {
			i = i + 1
			fmt.Printf("read msg i:%d :%s\n", i, string(msg))

		}

	}

}
