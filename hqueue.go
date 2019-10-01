package HKafkaQueue

import (
	"github.com/golang/glog"
	"sync"
	"sync/atomic"
)

type HQueue struct {
	queueName   string
	dataDirPath string
	index       *HQueueIndex
	readBlock   *HQueueBlock
	writeBlock  *HQueueBlock
	size        int64
	readLock    sync.Mutex
	writeLock   sync.Mutex
}

func NewHQueue(queueName string, dataDir string) (*HQueue, error) {
	indexPath := formatHqueueIndexPath(dataDir, queueName)
	index := NewHQueueIndex(indexPath)
	writeBlock, err := NewHQueueBlock(index, formatHqueueBlockPath(dataDir, queueName, index.writeBlockNum))
	if err != nil {
		return nil, err
	}
	hqueue := &HQueue{
		queueName:   queueName,
		dataDirPath: dataDir,
		index:       index,
		writeBlock:  writeBlock,
	}
	if index.readBlockNum == index.writeBlockNum {
		hqueue.readBlock = writeBlock.duplicate()
	}
	atomic.StoreInt64(&hqueue.size, 0)
	return hqueue, nil
}

func (q *HQueue) getSize() int64 {
	return q.size
}

func (q *HQueue) rotateNextWriteBlock() {
	nextWriteBlockNum := q.index.writeBlockNum + 1
	if nextWriteBlockNum < 0 {
		nextWriteBlockNum = 0
	}
	q.writeBlock.putEOF()
	if q.index.readBlockNum == q.index.writeBlockNum {
		q.writeBlock.sync()
	}
	block, err := NewHQueueBlock(q.index, formatHqueueBlockPath(q.dataDirPath, q.queueName, nextWriteBlockNum))
	if err != nil {
		glog.Errorf("rotate next block failed,when create new block, error:%s", err)
		return
	}
	q.writeBlock = block
	q.index.putWriteBlockNum(nextWriteBlockNum)
	q.index.putWritePosition(0)

}

func (q *HQueue) rotateNextReadBlock() {
	if q.index.readBlockNum == q.index.writeBlockNum {
		return
	}
	nextReadBlockNum := q.index.readBlockNum + 1
	if nextReadBlockNum < 0 {
		nextReadBlockNum = 0
	}
	blockPath := q.readBlock.blockFilePath
	if nextReadBlockNum == q.index.writeBlockNum {
		q.readBlock = q.writeBlock.duplicate()
	} else {
		block, err := NewHQueueBlock(q.index, formatHqueueBlockPath(q.dataDirPath, q.queueName, nextReadBlockNum))
		if err != nil {
			glog.Errorf("rotated next read block error:%s", err)
		}
		q.readBlock = block
	}
	q.index.putReadBlockNum(nextReadBlockNum)
	q.index.putReadPosition(0)
	toClear(blockPath)
}

func (q *HQueue) offer(bytes []byte) {
	if len(bytes) == 0 {
		return
	}
	TryCatch{}.Try(func() {
		q.writeLock.Lock()
		if !q.writeBlock.isSpaceQvailable(uint64(len(bytes))) {
			q.rotateNextWriteBlock()
		}
		q.writeBlock.write(bytes)
		atomic.AddInt64(&q.size, 1)
	}).CatchAll(func(err error) {
		glog.Errorf("HQueue write bytes error %s", err.Error())
	}).Finally(func() {
		q.writeLock.Unlock()
	})

}

func (q *HQueue) poll() []byte {
	q.readLock.Lock()
	TryCatch{}.Try(func() {
		if q.readBlock.eof() {
			q.rotateNextReadBlock()
		}
		bytes, err := q.readBlock.read()
		if bytes != nil {
			atomic.AddInt64(&q.size, -1)
		}
	}).CatchAll(func(err error) {
		glog.Errorf("HQueue write bytes error %s", err.Error())
	}).Finally(func() {
		q.readLock.Unlock()
	})
	return bytes
}

func (q *HQueue) sync() {
	q.writeBlock.sync()
	q.index.sync()
}
