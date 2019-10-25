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
	deleteTicker  *time.Ticker
	retentionTime int64
}

var ringBuffer = queue.NewRingBuffer(64)

func NewHQueuePool(rootDir string, retentionTime int64) *HQueuePool {
	if !isExists(rootDir) {
		err := os.MkdirAll(rootDir, 0711)
		if err != nil {
			glog.Fatalf("create dir :%s error: %s", rootDir, err)
		}
	}
	hqueuePool := &HQueuePool{
		dataDirPath:   rootDir,
		hqueueMap:     make(map[string]*HQueue),
		retentionTime: retentionTime,
	}
	hqueuePool.scanDir(rootDir)
	hqueuePool.deleteTicker = time.NewTicker(time.Minute)
	go func() {
		for {
			<-hqueuePool.deleteTicker.C
			hqueuePool.scanExpiredBlocks()
			deleteBlockFile()
		}
	}()

	return hqueuePool
}

func (p *HQueuePool) scanDir(rootDir string) {
	dataDir := getHqueueDataDir(rootDir)
	if !isExists(dataDir) {
		err := os.MkdirAll(dataDir, 0711)
		if err != nil {
			glog.Fatalf("create dir :%s error: %s", dataDir, err)
		}
	}
	fileInfos, err := ioutil.ReadDir(dataDir)
	if err != nil {
		glog.Errorf("read dir: %s fail", dataDir)
	}
	for _, fileInfo := range fileInfos {
		var queueName = fileInfo.Name()
		queue, err := NewHQueue(queueName, rootDir)
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
		v.Close()
	}
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
			currentWriteBlockNum := p.hqueueMap[queueName].producerIndex.blockNum
			if time.Now().Unix()-f.ModTime().Unix() > p.retentionTime && currentWriteBlockNum != blockNum {
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
	glog.Infof("add: %s to delete queue", blockPath)
	if err != nil {
		glog.Errorf("put delete file to ringbuffer error: %s", blockPath)
	}
}
