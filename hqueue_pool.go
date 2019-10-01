package HKafkaQueue

import (
	queue "github.com/Workiva/go-datastructures/queue"
	"github.com/golang/glog"
	"io/ioutil"
	"os"
	"strings"
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
	f, err := os.OpenFile(dataDirPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		glog.Fatalf("can not create directory %s", dataDirPath)
	}
	hqueuePool := &HQueuePool{
		dataDirPath: dataDirPath,
		dataDir:     f,
	}
	hqueuePool.scanDir(dataDirPath)
	hqueuePool.ticker = time.NewTicker(time.Second * 10)
	go func() {
		for _ = range hqueuePool.ticker.C {
			for _, v := range hqueuePool.hqueueMap {
				v.sync()
			}
			deleteBlockFile()
			//fmt.Printf("ticked at %v", time.Now())
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
					glog.Errorf("file can't not delete %s", path)
				}
			}
		}
	}
}

func (p *HQueuePool) scanDir(dirPath string) {
	fileInfos, err := ioutil.ReadDir(dirPath)
	if err != nil {
		glog.Errorf("read dir %s fail", dirPath)
	}
	for _, fileInfo := range fileInfos {
		var name = fileInfo.Name()
		if strings.Contains(name, INDEX_SUFFIX) {
			queueName := strings.Split(name, ".")[0]
			queue, err := NewHQueue(queueName, dirPath)
			if err != nil {
				glog.Errorf("scan queue data dir error :%s", err)
				continue
			}
			p.hqueueMap[queueName] = queue
		}
	}
}

func (p *HQueuePool) destroy() {
	if p != nil {
		p.disposal()
	}

}

func toClear(blockPath string) {
	ringBuffer.Put(blockPath)
}

func (p *HQueuePool) disposal() {
	for _, v := range p.hqueueMap {
		v.writeBlock.close()
	}
	p.ticker.Stop()
	deleteBlockFile()
}
