package HKafkaQueue

import (
	"github.com/Workiva/go-datastructures/queue"
	"github.com/golang/glog"
	"io/ioutil"
	"os"
	"time"
)

type HQueuePool struct {
	dataDirPath string
	dataDir     *os.File
	hqueueMap   map[string]*HQueue
	ticker      *time.Ticker
}

var ringBuffer = queue.NewRingBuffer(64)

func NewHQueuePool(dataDirPath string) *HQueuePool {
	f, err := os.Open(dataDirPath)
	if err != nil {
		glog.Fatalf("can not create directory: %s", dataDirPath)
	}
	hqueuePool := &HQueuePool{
		dataDirPath: dataDirPath,
		dataDir:     f,
		hqueueMap:   make(map[string]*HQueue),
	}
	hqueuePool.scanDir(dataDirPath)
	hqueuePool.ticker = time.NewTicker(time.Second * 5)
	go func() {
		for _ = range hqueuePool.ticker.C {
			for _, v := range hqueuePool.hqueueMap {
				v.sync()
			}
			deleteBlockFile()
		}
	}()
	return hqueuePool
}

func deleteBlockFile() {
	for {
		path, err := ringBuffer.Poll(5 * time.Millisecond)
		if err != nil {
			break
		} else {
			if path != "" {
				err := os.Remove(path.(string))
				if err != nil {
					glog.Errorf("file can't not delete: %s", path)
				}
			}
		}
	}
}

func (p *HQueuePool) scanDir(dirPath string) {
	fileInfos, err := ioutil.ReadDir(dirPath)
	if err != nil {
		glog.Errorf("read dir: %s fail", dirPath)
	}
	for _, fileInfo := range fileInfos {
		var queueName = fileInfo.Name()
		queue, err := NewHQueue(queueName, dirPath)
		if err != nil {
			glog.Errorf("create queue object: %s cause error: %s in scan dir ", queueName, err.Error())
			continue
		}
		p.hqueueMap[queueName] = queue
	}
}

func (p *HQueuePool) destroy() {
	if p != nil {
		p.disposal()
	}

}

func (p *HQueuePool) SetQueueMap(queueName string, queue *HQueue) {
	p.hqueueMap[queueName] = queue
}

func toClear(blockPath string) {
	ringBuffer.Put(blockPath)
}

func (p *HQueuePool) Disposal() {
	for _, v := range p.hqueueMap {
		v.close()
	}
	p.ticker.Stop()
	deleteBlockFile()
}
