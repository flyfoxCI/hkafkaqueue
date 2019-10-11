package HKafkaQueue

import (
	"github.com/golang/glog"
	"github.com/grandecola/mmap"
	"os"
	"strings"
	"syscall"
)

const INDEX_SUFFIX = ".idx"
const INDEX_SIZE = 1 << 5
const NUM_OFFSET = 8
const POS_OFFSET = 16
const CNT_OFFSET = 24
const MAGIC = "v1.00000"

type HQueueIndex struct {
	versionNo string
	blockNum  uint64
	position  uint64
	counter   uint64

	indexFile *os.File
	mapFile   mmap.File
}

func NewHQueueIndex(indexFilePath string) *HQueueIndex {
	if !isExists(indexFilePath) {
		_, err := os.Create(indexFilePath)
		if err != nil {
			glog.Errorf("create file eorr: %s", err)
			return nil
		}
	}
	f, err := os.OpenFile(indexFilePath, os.O_RDWR, 0644)
	if err != nil {
		glog.Errorf("open index file %s error: %s", indexFilePath, err)
	}
	fileInfo, _ := f.Stat()
	var i *HQueueIndex = new(HQueueIndex)
	if fileInfo.Size() > 0 {
		indexMapFile, errMmap := mmap.NewSharedFileMmap(f, 0, INDEX_SIZE, PROT_PAGE)
		if errMmap != nil {
			glog.Errorf("mmap file %s error: %s", indexFilePath, errMmap)
		}
		sb := &strings.Builder{}
		sb.Grow(8)
		indexMapFile.ReadStringAt(sb, 0)
		versionNo := sb.String()
		blockNum := indexMapFile.ReadUint64At(NUM_OFFSET)
		position := indexMapFile.ReadUint64At(POS_OFFSET)
		counter := indexMapFile.ReadUint64At(CNT_OFFSET)
		i.mapFile = indexMapFile
		i.indexFile = f
		i.versionNo = versionNo
		i.blockNum = blockNum
		i.position = position
		i.counter = counter
	} else {
		if _, err := f.WriteAt([]byte{byte(0)}, INDEX_SIZE-1); nil != err {
			glog.Errorf("expand empty file error: %s", err)
		}
		indexMapFile, errMmap := mmap.NewSharedFileMmap(f, 0, INDEX_SIZE, PROT_PAGE)
		if errMmap != nil {
			glog.Errorf("mmap file: %s error: %s", indexFilePath, errMmap)
		}
		i.mapFile = indexMapFile
		i.indexFile = f
		i.putMagic()
		i.putBlockNum(0)
		i.putPosition(0)
		i.putCounter(0)
		i.sync()
	}
	return i
}

func (i *HQueueIndex) putBlockNum(blockNum uint64) {
	i.blockNum = blockNum
	i.mapFile.WriteUint64At(blockNum, NUM_OFFSET)
}

func (i *HQueueIndex) putPosition(position uint64) {
	i.position = position
	i.mapFile.WriteUint64At(position, POS_OFFSET)
}

func (i *HQueueIndex) putMagic() {
	i.versionNo = MAGIC
	i.mapFile.WriteStringAt(MAGIC, 0)
}

func (i *HQueueIndex) putCounter(c uint64) {
	i.counter = c
	i.mapFile.WriteUint64At(c, CNT_OFFSET)
}

func (i *HQueueIndex) sync() {
	err := i.mapFile.Flush(syscall.MS_SYNC)
	if err != nil {
		glog.Errorf("producer index sync err :%s", err)
	}
}

func (i *HQueueIndex) close() {
	if i.mapFile == nil {
		return
	}
	i.sync()
	i.indexFile.Sync()
	i.mapFile.Unmap()
	i.indexFile.Close()
}

func formatHqueueProducerIndexPath(dataDir string, queueName string) string {
	return dataDir + string(os.PathSeparator) + queueName + string(os.PathSeparator) + queueName + INDEX_SUFFIX
}

func formatHqueueConsumerIndexPath(dataDir string, queueName string, consumerName string) string {
	return dataDir + string(os.PathSeparator) + queueName + string(os.PathSeparator) + "consumer_" + consumerName + INDEX_SUFFIX
}

//func (i *HQueueIndex) reset() {
//	remain := i.counter - i.readCounter
//	i.putReadCounter(0)
//	i.putcounter(remain)
//	if remain == 0 && i.readCounter == i.counter {
//		i.putReadPosition(0)
//		i.putcounter(0)
//	}
//}
