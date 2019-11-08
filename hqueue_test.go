package HKafkaQueue

import (
	"fmt"
	"os"
	"testing"

	_ "github.com/panjf2000/ants"
)

var dataDir = "/tmp/hqueue/"
var queueName = "test"

func TestNewHQueue(t *testing.T) {
	hqueue, err := NewHQueue(queueName, dataDir)
	if err != nil {
		t.Fatalf("create hqueue error %v", err)
	}
	fmt.Println(hqueue)
}

func BenchmarkWrite(t *testing.B) {
	hqueue, err := NewHQueue(queueName, dataDir)

	var i = 0
	if err != nil {
		t.Fatalf("create hqueue error %v", err)
	}
	msg := "Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co." +
		"Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co." +
		"Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co." +
		"Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co." +
		"Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co." +
		"Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co." +
		"Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co." +
		"Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co." +
		"Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co." +
		"Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co." +
		"Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co." +
		"Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co."
	for {
		_, err := hqueue.Offer([]byte(msg))
		if err != nil {
			t.Fatalf("write error :%v", err)
		}
		i = i + 1
		if i == 10000 {
			break
		}
	}
	//var w sync.WaitGroup
	//p,_:= ants.NewPoolWithFunc(1, func(i interface{}) {
	//	_, err := hqueue.Offer([]byte("Bridgewater Associates AQR Capital Management Millennium Management Citadel Soros Fund Management Winton Capital Management D.E. Shaw& Co. enaissance Technologies LLC Two Sigma Paulson & Co."))
	//	if err != nil {
	//		t.Fatalf("write error :%v", err)
	//	}
	//	w.Done()
	//})
	//defer p.Release()
	//for i=0;i<1000000;i++{
	//	w.Add(1)
	//	_=p.Invoke(i)
	//}
	//w.Wait()
	//fmt.Println(hqueue.producerIndex.counter)
	//fmt.Println(i)

}

func BenchmarkRead(t *testing.B) {
	hqueue, err := NewHQueue(queueName, dataDir, "p014")
	//hqueue.ResetConsumerI ndex(2, 0)
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
		if i == 10000 {
			break
		}
	}
}

func TestIntoUint(t *testing.T) {
	f, _ := os.OpenFile("/app/gopath/src/github.com/flyfoxCI/pv-hangout/7958.blk", os.O_RDWR, 0644)
	//m, _ := mmap.NewSharedFileMmap(f, 0, INDEX_SIZE, PROT_PAGE)
	f.Seek(15827061, 0)
	b := make([]byte, 300000)
	f.Read(b)
	s := String(b)
	fmt.Println(s)

}
