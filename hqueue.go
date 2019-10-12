package HKafkaQueue

import (
	"github.com/fsnotify/fsnotify"
	"github.com/golang/glog"
	"os"
	"sync"
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
}

func NewHQueue(queueName string, dataDir string, consumerName ...string) (*HQueue, error) {
	checkQueueDir(dataDir, queueName)
	producerIndexPath := formatHqueueProducerIndexPath(dataDir, queueName)
	producerIndex := NewHQueueIndex(producerIndexPath)
	writeBlock, err := NewHQueueBlock(producerIndex, formatHqueueBlockPath(dataDir, queueName, producerIndex.blockNum))
	if err != nil {
		return nil, err
	}
	hqueue := &HQueue{
		queueName:     queueName,
		dataDirPath:   dataDir,
		producerIndex: producerIndex,
		writeBlock:    writeBlock,
	}
	if len(consumerName) > 0 {
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
		hqueue.addIndexMonitor()
	}

	return hqueue, nil
}

func checkQueueDir(dataDir string, queueName string) {
	fullQueuePath := dataDir + string(os.PathSeparator) + queueName
	if !isExists(fullQueuePath) {
		os.MkdirAll(fullQueuePath, 0711)
	}
}

func (q *HQueue) rotateNextWriteBlock() {
	nextWriteBlockNum := q.producerIndex.blockNum + 1
	if nextWriteBlockNum < 0 {
		nextWriteBlockNum = 0
	}
	q.writeBlock.putEOF()
	q.writeBlock.sync()
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
	q.readBlock = block
	q.consumerIndex.putBlockNum(nextReadBlockNum)
	q.consumerIndex.putPosition(0)
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
		q.readLock.Unlock()
		return nil, err
	}
	q.readLock.Unlock()
	return bytes, nil
}

func (q *HQueue) Sync() {
	q.writeBlock.sync()
	q.producerIndex.sync()
	if q.consumerIndex != nil {
		q.consumerIndex.sync()
	}
}

func (q *HQueue) Close() {
	q.writeBlock.close()
	q.producerIndex.close()
	if q.readBlock != nil {
		q.readBlock.close()
	}
	if q.consumerIndex != nil {
		q.consumerIndex.close()
	}

}

func (q *HQueue) addIndexMonitor() {
	watcher, err := fsnotify.NewWatcher()
	defer watcher.Close()
	if err != nil {
		glog.Fatal(err)
	}
	defer watcher.Close()
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				if event.Op&fsnotify.Write == fsnotify.Write {
					q.producerIndex.reload()
				}

			case err := <-watcher.Errors:
				if err != nil {
					glog.Errorf("watch the consumer index: %s error: %s", q.consumerIndex.indexFile.Name(), err)
				}
			}
		}
	}()
	err = watcher.Add(q.producerIndex.indexFile.Name())
	if err != nil {
		glog.Fatalf("add monitor file error: %s", err)
	}
}
