package HKafkaQueue

import (
	"fmt"
	"testing"
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
	}
	hqueue.sync()

}

func TestQueueRead(t *testing.T) {
	hqueue, err := NewHQueue(queueName, dataDir)
	if err != nil {
		t.Fatalf("create hqueue error %v", err)
	}
	for {
		bytes, err := hqueue.poll()
		if err != nil {
			break
		} else {
			fmt.Println(String(bytes))
			fmt.Println(hqueue.getSize())
		}
	}
	hqueue.sync()
}
