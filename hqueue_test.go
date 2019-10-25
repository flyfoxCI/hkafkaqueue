package HKafkaQueue

import (
	"fmt"
	"testing"
)

var dataDir = "/tmp/kqueue"
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
		_, err := hqueue.Offer([]byte("Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co."))
		if err != nil {
			t.Fatalf("write error :%v", err)
			break
		}
		i = i + 1
		if i > 1000000 {
			hqueue.Close()
			break
		}
	}
	fmt.Println(hqueue.producerIndex.counter)
	fmt.Println(i)

}

func TestQueueRead(t *testing.T) {
	hqueue, err := NewHQueue(queueName, dataDir, "p4")
	hqueue.ResetConsumerIndex(2, 0)
	var i = 0
	if err != nil {
		t.Fatalf("create hqueue error: %v", err)
	}
	for {
		_, err := hqueue.Poll()
		if err != nil {
			continue
		} else {
			//fmt.Println(String(bytes))
			i = i + 1
		}
		if i == 1000000 {
			hqueue.Close()
			break
		}
	}
	fmt.Println(hqueue.consumerIndex.counter)
	fmt.Println(i)
}
