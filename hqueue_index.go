package HKafkaQueue

import (
	"github.com/golang/glog"
	"github.com/grandecola/mmap"
	"os"
	"strings"
)

const INDEX_SUFFIX = ".idx"
const INDEX_SIZE = 32
const READ_NUM_OFFSET = 8
const READ_POS_OFFSET = 12
const READ_CNT_OFFSET = 16
const WRITE_NUM_OFFSET = 20
const WRITE_POS_OFFSET = 24
const WRITE_CNT_OFFSET = 28
const MAGIC = "v1.00000"

type HQueueIndex struct {
	versionNo     string
	readBlockNum  uint32
	readPosition  uint32
	readCounter   uint32
	writeBlockNum uint32
	writePosition uint32
	writeCounter  uint32

	indexFile    *os.File
	indexMapFile *FileImpl
}

func NewHQueueIndex(indexFilePath string) *HQueueIndex {
	f, err := os.OpenFile(indexFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		glog.Errorf("open index file %s error: %s", indexFilePath, err)
	}
	defer f.Close()
	fileInfo, _ := f.Stat()
	i := HQueueIndex{}
	if fileInfo.Size() > 0 {
		indexMapFile, errMmap := mmap.NewSharedFileMmap(f, 0, INDEX_SIZE, PROT_PAGE)
		if errMmap != nil {
			glog.Errorf("mmap file %s error:%s", indexFilePath, errMmap)
		}
		defer indexMapFile.Unmap()
		m := FileImpl{indexMapFile}

		sb := &strings.Builder{}
		sb.Grow(8)
		m.ReadStringAt(sb, 0)
		i.versionNo = sb.String()
		i.readBlockNum, _ = m.ReadUint32(READ_NUM_OFFSET)
		i.readPosition, _ = m.ReadUint32(READ_POS_OFFSET)
		i.readCounter, _ = m.ReadUint32(READ_CNT_OFFSET)
		i.writeBlockNum, _ = m.ReadUint32(WRITE_NUM_OFFSET)
		i.writePosition, _ = m.ReadUint32(WRITE_POS_OFFSET)
		i.writeCounter, _ = m.ReadUint32(WRITE_CNT_OFFSET)
		i.indexFile = f
		i.indexMapFile = &m
	} else {
		i.indexFile = f
		i.putMagic()
		i.putReadBlockNum(0)
		i.putReadPosition(0)
		i.putReadCounter(0)
		i.putWriteBlockNum(0)
		i.putWritePosition(0)
		i.putWriteCounter(0)
	}
	return &i
}

func (i *HQueueIndex) putWriteBlockNum(blockNum uint32) {
	i.writeBlockNum = blockNum
	i.indexMapFile.WriteUint32(blockNum, WRITE_NUM_OFFSET)
}

func (i *HQueueIndex) putWritePosition(position uint32) {
	i.writePosition = position
	i.indexMapFile.WriteUint32(position, WRITE_POS_OFFSET)
}

func (i *HQueueIndex) putReadBlockNum(blockNum uint32) {
	i.indexMapFile.WriteUint32(blockNum, READ_NUM_OFFSET)
	i.readBlockNum = blockNum
}

func (i *HQueueIndex) putReadPosition(position uint32) {
	i.readPosition = position
	i.indexMapFile.WriteUint32(position, READ_POS_OFFSET)
}

func (i *HQueueIndex) putReadCounter(c uint32) {
	i.readCounter = c
	i.indexMapFile.WriteUint32(c, READ_CNT_OFFSET)
}

func (i *HQueueIndex) putMagic() {
	i.versionNo = MAGIC
	i.indexMapFile.WriteStringAt(MAGIC, 0)
}

func (i *HQueueIndex) putWriteCounter(c uint32) {
	i.writeCounter = c
	i.indexMapFile.WriteUint32(c, WRITE_CNT_OFFSET)
}

func (i *HQueueIndex) reset() {
	remain := i.writeCounter - i.readCounter
	i.putReadCounter(0)
	i.putWriteCounter(remain)
	if remain == 0 && i.readCounter == i.writeCounter {
		i.putReadPosition(0)
		i.putWriteCounter(0)
	}
}

func (i *HQueueIndex) sync() {
	i.indexMapFile.Flush(syscall.MS_SYNC)
}

func (i *HQueueIndex) close() {
	if i.indexMapFile == nil {
		return
	}
	i.sync()
	i.indexMapFile.Unmap()
	i.indexFile.Close()

}

func formatHqueueIndexPath(dataDir string, queueName string) string {
	return dataDir + string(os.PathSeparator) + queueName + INDEX_SUFFIX
}
