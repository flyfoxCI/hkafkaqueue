package HKafkaQueue

import (
	"errors"
	"github.com/golang/glog"
	"github.com/grandecola/mmap"
	"math"
	"os"
	"strconv"
	"syscall"
)

const BLOCK_FILE_SUFFIX = ".blk"
const BLOCK_SIZE = 1 * 1024
const EOF = math.MaxUint64
const PROT_PAGE = syscall.PROT_READ | syscall.PROT_WRITE

type HQueueBlock struct {
	blockFilePath string
	index         *HQueueIndex
	blockFile     *os.File
	mapFile       mmap.File
}

func NewHQueueBlock(index *HQueueIndex, blockFilePath string) (*HQueueBlock, error) {
	bf, err := os.OpenFile(blockFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	fileInfo, _ := bf.Stat()
	if fileInfo.Size() < 1 {
		if _, err := bf.WriteAt([]byte{byte(0)}, BLOCK_SIZE-1); nil != err {
			return nil, err
		}
	}
	mapFile, errMmap := mmap.NewSharedFileMmap(bf, 0, BLOCK_SIZE, PROT_PAGE)
	if errMmap != nil {
		return nil, errMmap
	}
	hqueueBlock := &HQueueBlock{
		blockFilePath: blockFilePath,
		index:         index,
		blockFile:     bf,
		mapFile:       mapFile,
	}
	return hqueueBlock, nil
}

func (b *HQueueBlock) putEOF() {
	b.mapFile.WriteUint64At(EOF, int64(b.index.writePosition))

}

func (b *HQueueBlock) sync() {
	err := b.mapFile.Flush(syscall.MS_SYNC)
	if err != nil {
		glog.Errorf("sync map file:%s error:%s", b.blockFile.Name(), err)
	}
}

func (b *HQueueBlock) isSpaceAvailable(len uint64) bool {
	remain := uint64(BLOCK_SIZE) - b.index.writePosition
	var flag bool = false
	if remain > len+16 {
		flag = true
	}
	return flag
}

func (b *HQueueBlock) write(bytes []byte) (int, error) {
	currentWritePosition := b.index.writePosition
	b.mapFile.WriteUint64At(uint64(len(bytes)), int64(currentWritePosition))
	writeLen, err := b.mapFile.WriteAt(bytes, int64(currentWritePosition+8))
	b.index.writePosition = currentWritePosition + uint64(len(bytes)+8)
	b.index.putWritePosition(currentWritePosition + uint64(len(bytes)+8))
	b.index.putWriteCounter(b.index.writeCounter + 1)
	return writeLen, err
}

func (b *HQueueBlock) eof() bool {
	readPosition := b.index.readPosition
	u := b.mapFile.ReadUint64At(int64(readPosition))
	return u == EOF
}

func (b *HQueueBlock) read() ([]byte, error) {
	currentReadPosition := b.index.readPosition
	dataLen := b.mapFile.ReadUint64At(int64(currentReadPosition))
	if dataLen == 0 {
		return nil, errors.New("the message in queue is 0")
	}
	data := make([]byte, dataLen)
	_, err := b.mapFile.ReadAt(data, int64(currentReadPosition+8))
	if err != nil {
		return nil, err
	}
	b.index.putReadPosition(currentReadPosition + 8 + dataLen)
	b.index.putReadCounter(b.index.readCounter + 1)
	return data, nil
}

func (b *HQueueBlock) close() {
	if b == nil {
		return
	}
	TryCatch{}.Try(func() {
		b.sync()
		b.blockFile.Close()
		b.mapFile.Unmap()
	}).CatchAll(func(err error) {
		glog.Errorf("close block file err %s", err.Error())
	})

}

func (b *HQueueBlock) duplicate() *HQueueBlock {
	newBlock := &b
	return *newBlock
}

func formatHqueueBlockPath(dataDir string, queueName string, blockNum uint64) string {
	return dataDir + string(os.PathSeparator) + queueName + string(os.PathSeparator) + strconv.FormatUint(blockNum, 10) + BLOCK_FILE_SUFFIX
}
