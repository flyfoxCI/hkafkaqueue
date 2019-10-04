package HKafkaQueue

import (
	"fmt"
	"sync"
	"testing"
)

func TestCreatePool(t *testing.T) {
	pool := NewHQueuePool("/tmp/hqueue")
	fmt.Println(pool.hqueueMap)
	pool.destroy()
}

func TestPoolDelete(t *testing.T) {
	pool := NewHQueuePool("/tmp/hqueue")
	hqueue, _ := NewHQueue("test2", "/tmp/hqueue")
	pool.hqueueMap["test2"] = hqueue
	var w sync.WaitGroup
	w.Add(2)
	go read(hqueue, &w)
	go write(hqueue, &w)
	w.Wait()
	pool.destroy()
}
