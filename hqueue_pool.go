package HKafkaQueue

import (
	"github.com/Workiva/go-datastructures/queue"
	"github.com/golang/glog"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type HQueuePool struct {
	dataDirPath   string
	dataDir       *os.File
	hqueueMap     map[string]*HQueue
	syncTicker    *time.Ticker
	deleteTicker  *time.Ticker
	retentionTime int64
}

var ringBuffer = queue.NewRingBuffer(64)

func NewHQueuePool(dataDirPath string, retentionTime int64) *HQueuePool {
	f, err := os.Open(dataDirPath)
	if err != nil {
		glog.Fatalf("can not create directory: %s", dataDirPath)
	}
	hqueuePool := &HQueuePool{
		dataDirPath:   dataDirPath,
		dataDir:       f,
		hqueueMap:     make(map[string]*HQueue),
		retentionTime: retentionTime,
	}
	hqueuePool.scanDir(dataDirPath)
	hqueuePool.syncTicker = time.NewTicker(time.Second * 5)
	go func() {
		for _ = range hqueuePool.syncTicker.C {
			for _, v := range hqueuePool.hqueueMap {
				v.sync()
			}
		}
	}()
	hqueuePool.deleteTicker = time.NewTicker(time.Minute * 2)
	go func() {
		for _ = range hqueuePool.deleteTicker.C {
			hqueuePool.scanExpiredBlocks()
			deleteBlockFile()
		}
	}()

	return hqueuePool
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

func (p *HQueuePool) Destroy() {
	if p != nil {
		p.disposal()
	}

}

func (p *HQueuePool) SetQueueMap(queueName string, queue *HQueue) {
	p.hqueueMap[queueName] = queue
}

func (p *HQueuePool) disposal() {
	for _, v := range p.hqueueMap {
		v.close()
	}
	p.syncTicker.Stop()
	p.deleteTicker.Stop()
	deleteBlockFile()
}

func (p *HQueuePool) scanExpiredBlocks() {
	err := filepath.Walk(p.dataDirPath, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		if strings.Contains(path, BLOCK_FILE_SUFFIX) {
			dirs := strings.Split(path, string(os.PathSeparator))
			queueName := dirs[len(dirs)-2]
			blockName := dirs[len(dirs)-1]
			blockNumStr := strings.Split(blockName, ".")[0]
			blockNum, err := strconv.ParseUint(blockNumStr, 10, 64)
			if err != nil {
				glog.Errorf("parse blockNum error: %s", err)
				return err
			}
			currentReadBlockNum := p.hqueueMap[queueName].index.readBlockNum
			if time.Now().Unix()-f.ModTime().Unix() > p.retentionTime && blockNum < currentReadBlockNum {
				toClear(path)
			}
		}
		return nil
	})
	if err != nil {
		glog.Errorf("walk file path:%s,error:%s", p.dataDirPath, err)
	}
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

func toClear(blockPath string) {
	err := ringBuffer.Put(blockPath)
	if err != nil {
		glog.Errorf("put delete file to ringbuffer error: %s", blockPath)
	}
}
