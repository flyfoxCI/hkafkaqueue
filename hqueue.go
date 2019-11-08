package HKafkaQueue

import "C"
import (
	"github.com/golang/glog"
	"os"
	"sync"
	"time"
)

type HQueue struct {
	queueName     string
	dataDirPath   string
	producerIndex *HQueueIndex
	consumerIndex *HQueueIndex
	readBlock     *HQueueBlock
	writeBlock    *HQueueBlock
	readLock      sync.Mutex
	writeLock     sync.Mutex
	syncTicker    *time.Ticker
}

func NewHQueue(queueName string, dataDir string, consumerName ...string) (*HQueue, error) {
	checkDir(getHqueueDataDir(dataDir) + string(os.PathSeparator) + queueName)
	hqueue := &HQueue{
		queueName:   queueName,
		dataDirPath: dataDir,
	}
	producerIndexPath := formatHqueueProducerIndexPath(dataDir, queueName)
	producerIndex := NewHQueueIndex(producerIndexPath)
	hqueue.producerIndex = producerIndex
	if len(consumerName) > 0 {
		checkDir(getConsumerIndexDir(dataDir) + string(os.PathSeparator) + consumerName[0])
		consumerIndexPath := formatHqueueConsumerIndexPath(dataDir, queueName, consumerName[0])
		consumerIndex := NewHQueueIndex(consumerIndexPath)
		hqueue.consumerIndex = consumerIndex
		//if consumerIndex.blockNum == producerIndex.blockNum {
		//	hqueue.readBlock = writeBlock.duplicate()
		//} else {
		readBlock, err := NewHQueueBlock(consumerIndex, formatHqueueBlockPath(dataDir, queueName, consumerIndex.blockNum))
		if err != nil {
			return nil, err
		}
		hqueue.readBlock = readBlock
		hqueue.checkProduceIndex()
	} else {
		writeBlock, err := NewHQueueBlock(producerIndex, formatHqueueBlockPath(dataDir, queueName, producerIndex.blockNum))
		if err != nil {
			return nil, err
		}
		hqueue.writeBlock = writeBlock
	}

	return hqueue, nil
}

func checkDir(dataDir string) {
	if !isExists(dataDir) {
		os.MkdirAll(dataDir, 0711)
	}
}

func (q *HQueue) rotateNextWriteBlock() {
	nextWriteBlockNum := q.producerIndex.blockNum + 1
	if nextWriteBlockNum < 0 {
		nextWriteBlockNum = 0
	}
	q.writeBlock.putEOF()
	q.writeBlock.close()
	block, err := NewHQueueBlock(q.producerIndex, formatHqueueBlockPath(q.dataDirPath, q.queueName, nextWriteBlockNum))
	if err != nil {
		glog.Errorf("rotate next block failed,when create new block, error: %s", err)
		return
	}
	q.writeBlock = block
	q.producerIndex.putBlockNum(nextWriteBlockNum)
	q.producerIndex.putPosition(0)

}

func (q *HQueue) rotateNextReadBlock() {
	if q.consumerIndex.blockNum == q.producerIndex.blockNum {
		return
	}
	nextReadBlockNum := q.consumerIndex.blockNum + 1
	if nextReadBlockNum < 0 {
		nextReadBlockNum = 0
	}
	block, err := NewHQueueBlock(q.consumerIndex, formatHqueueBlockPath(q.dataDirPath, q.queueName, nextReadBlockNum))
	if err != nil {
		glog.Errorf("rotated next read block error: %s", err)
	}
	q.readBlock.close()
	q.readBlock = block
	q.consumerIndex.putBlockNum(nextReadBlockNum)
	q.consumerIndex.putPosition(0)
}

func (q *HQueue) Offer(bytes []byte) (int, error) {
	if len(bytes) <= 0 || len(bytes) >= BLOCK_SIZE {
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
		if _, ok := err.(*PutLengthError); ok {
			q.rotateNextReadBlock()
		}
		q.readLock.Unlock()
		return nil, err
	}
	q.readLock.Unlock()
	return bytes, nil
}

func (q *HQueue) Sync() {
	if q.writeBlock != nil {
		q.writeBlock.sync()
		q.producerIndex.sync()
	}
	if q.consumerIndex != nil {
		q.consumerIndex.sync()
	}
}

func (q *HQueue) Close() {
	if q.readBlock != nil {
		q.readBlock.close()
	}
	if q.consumerIndex != nil {
		q.consumerIndex.close()
	}
	if q.writeBlock != nil {
		q.writeBlock.close()
		q.producerIndex.close()
	}
	if q.syncTicker != nil {
		q.syncTicker.Stop()
	}

}

func (q *HQueue) checkProduceIndex() {
	ticker := time.NewTicker(time.Second / 2)
	q.syncTicker = ticker
	go func() {
		for {
			<-q.syncTicker.C
			q.producerIndex.reload()
		}
	}()
}

func (q *HQueue) ResetConsumerIndex(blockNum uint64, position uint64) {
	q.consumerIndex.putBlockNum(blockNum)
	q.consumerIndex.putPosition(position)
	q.consumerIndex.putCounter(0)
	readBlock, _ := NewHQueueBlock(q.consumerIndex, formatHqueueBlockPath(q.dataDirPath, q.queueName, blockNum))
	q.readBlock = readBlock

}

func main() {}
