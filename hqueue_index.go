package HKafkaQueue

import (
	"github.com/golang/glog"
	"github.com/grandecola/mmap"
	"os"
	"strings"
	"syscall"
)

const INDEX_SUFFIX = ".idx"
const INDEX_SIZE = 1 << 6
const READ_NUM_OFFSET = 8
const READ_POS_OFFSET = 16
const READ_CNT_OFFSET = 24
const WRITE_NUM_OFFSET = 32
const WRITE_POS_OFFSET = 40
const WRITE_CNT_OFFSET = 48
const MAGIC = "v1.00000"

type HQueueIndex struct {
	versionNo     string
	readBlockNum  uint64
	readPosition  uint64
	readCounter   uint64
	writeBlockNum uint64
	writePosition uint64
	writeCounter  uint64

	indexFile *os.File
	mapFile   mmap.File
}

func NewHQueueIndex(indexFilePath string) *HQueueIndex {
	if !isExists(indexFilePath) {
		_, err := os.Create(indexFilePath)
		if err != nil {
			glog.Errorf("create file eorr %s", err)
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
			glog.Errorf("mmap file %s error:%s", indexFilePath, errMmap)
		}
		sb := &strings.Builder{}
		sb.Grow(8)
		indexMapFile.ReadStringAt(sb, 0)
		versionNo := sb.String()
		readBlockNum := indexMapFile.ReadUint64At(READ_NUM_OFFSET)
		readPosition := indexMapFile.ReadUint64At(READ_POS_OFFSET)
		readCounter := indexMapFile.ReadUint64At(READ_CNT_OFFSET)
		writeBlockNum := indexMapFile.ReadUint64At(WRITE_NUM_OFFSET)
		writePosition := indexMapFile.ReadUint64At(WRITE_POS_OFFSET)
		writeCounter := indexMapFile.ReadUint64At(WRITE_CNT_OFFSET)
		i.mapFile = indexMapFile
		i.indexFile = f
		i.versionNo = versionNo
		i.readBlockNum = readBlockNum
		i.readPosition = readPosition
		i.readCounter = readCounter
		i.writeBlockNum = writeBlockNum
		i.writePosition = writePosition
		i.writeCounter = writeCounter
	} else {
		if _, err := f.WriteAt([]byte{byte(0)}, INDEX_SIZE-1); nil != err {
			glog.Errorf("expand empty file error %s", err)
		}
		indexMapFile, errMmap := mmap.NewSharedFileMmap(f, 0, INDEX_SIZE, PROT_PAGE)
		if errMmap != nil {
			glog.Errorf("mmap file %s error:%s", indexFilePath, errMmap)
		}
		i.mapFile = indexMapFile
		i.indexFile = f
		i.putMagic()
		i.putReadPosition(0)
		i.putReadBlockNum(0)
		i.putReadCounter(0)
		i.putWriteBlockNum(0)
		i.putWritePosition(0)
		i.putWriteCounter(0)
		i.sync()
	}
	return i
}

func (i *HQueueIndex) putWriteBlockNum(blockNum uint64) {
	i.writeBlockNum = blockNum
	i.mapFile.WriteUint64At(blockNum, WRITE_NUM_OFFSET)
}

func (i *HQueueIndex) putWritePosition(position uint64) {
	i.writePosition = position
	i.mapFile.WriteUint64At(position, WRITE_POS_OFFSET)
}

func (i *HQueueIndex) putReadBlockNum(blockNum uint64) {
	i.readBlockNum = blockNum
	i.mapFile.WriteUint64At(blockNum, int64(READ_NUM_OFFSET))

}

func (i *HQueueIndex) putReadPosition(position uint64) {
	i.readPosition = position
	i.mapFile.WriteUint64At(position, READ_POS_OFFSET)
}

func (i *HQueueIndex) putReadCounter(c uint64) {
	i.readCounter = c
	i.mapFile.WriteUint64At(c, READ_CNT_OFFSET)
}

func (i *HQueueIndex) putMagic() {
	i.versionNo = MAGIC
	i.mapFile.WriteStringAt(MAGIC, 0)
}

func (i *HQueueIndex) putWriteCounter(c uint64) {
	i.writeCounter = c
	i.mapFile.WriteUint64At(c, WRITE_CNT_OFFSET)
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
	i.mapFile.Flush(syscall.MS_SYNC)
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

func formatHqueueIndexPath(dataDir string, queueName string) string {
	return dataDir + string(os.PathSeparator) + queueName + INDEX_SUFFIX
}
