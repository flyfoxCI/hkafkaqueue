package HKafkaQueue

import (
	"github.com/golang/glog"
	"os"
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
	checkQueueDir(dataDir, queueName)
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
	} else {
		readBlock, err := NewHQueueBlock(index, formatHqueueBlockPath(dataDir, queueName, index.readBlockNum))
		if err != nil {
			return nil, err
		}
		hqueue.readBlock = readBlock
	}
	atomic.StoreInt64(&hqueue.size, int64(index.writeCounter-index.readCounter))
	return hqueue, nil
}

func checkQueueDir(dataDir string, queueName string) {
	fullQueuePath := dataDir + string(os.PathSeparator) + queueName
	if !isExists(fullQueuePath) {
		os.MkdirAll(fullQueuePath, 0711)
	}
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
			glog.Errorf("rotated next read block error: %s", err)
		}
		q.readBlock = block
	}
	q.index.putReadBlockNum(nextReadBlockNum)
	q.index.putReadPosition(0)
	toClear(blockPath)
}

func (q *HQueue) Offer(bytes []byte) (int, error) {
	if len(bytes) == 0 {
		return 0, nil
	}
	q.writeLock.Lock()
	if !q.writeBlock.isSpaceAvailable(uint64(len(bytes))) {
		q.rotateNextWriteBlock()
	}
	writeLen, err := q.writeBlock.write(bytes)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&q.size, 1)
	q.writeLock.Unlock()
	return writeLen, nil

}

func (q *HQueue) Poll() ([]byte, error) {
	q.readLock.Lock()
	if q.readBlock.eof() {
		q.rotateNextReadBlock()
	}
	bytes, err := q.readBlock.read()
	if err != nil {
		glog.Errorf("HQueue read bytes error: %s")
		q.readLock.Unlock()
		return nil, err
	}
	if bytes != nil {
		atomic.AddInt64(&q.size, -1)
	}
	q.readLock.Unlock()
	return bytes, nil
}

func (q *HQueue) sync() {
	q.writeBlock.sync()
	q.index.sync()
}

func (q *HQueue) close() {
	q.writeBlock.close()
	q.index.close()
}
