package HKafkaQueue

import (
	"encoding/json"
	"github.com/golang/glog"
	"github.com/grandecola/mmap"
	"os"
	"syscall"
)

const BLOCK_FILE_SUFFIX = ".blk"
const BLOCK_SIZE = 256 * 1024 * 1024
const EOF = 0
const PROT_PAGE = syscall.PROT_READ | syscall.PROT_WRITE

type HQueueBlock struct {
	blockFilePath string
	index         HQueueIndex
	blockFile     *os.File
	mapFile       FileImpl
}

func NewHQueueBlock(index HQueueIndex, blockFilePath string) *HQueueBlock {
	bf, err := os.OpenFile(blockFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		glog.Errorf("create block file error %s", err)
		return nil
	}
	defer bf.Close()
	mapFile, errMmap := mmap.NewSharedFileMmap(bf, 0, 0, PROT_PAGE)
	defer mapFile.Unmap()
	if errMmap != nil {
		glog.Fatal("error in mapping :: %s", errMmap)
	}
	hqueueBlock := &HQueueBlock{
		blockFilePath: blockFilePath,
		index:         index,
		blockFile:     bf,
		mapFile:       FileImpl{mapFile},
	}
	return hqueueBlock
}

func (b *HQueueBlock) putEOF() {
	b.mapFile.WriteUint64At(uint64(EOF), int64(b.index.writePosition))
}

func (b *HQueueBlock) sync() {
	b.mapFile.Flush(syscall.MS_SYNC)
}

func (b *HQueueBlock) duplicate() *HQueueBlock {
	oj, _ := json.Marshal(b)
	copy := new(HQueueBlock)
	_ = json.Unmarshal(oj, copy)
	return copy
}

func (b *HQueueBlock) isSpaceQvailable(len int) bool {
	fileInfo, _ := os.Stat(b.blockFilePath)
	flag := BLOCK_SIZE-fileInfo.Size()-8-int64(len) > 0
	return flag
}

func (b *HQueueBlock) write(bytes []byte) {
	currentWritePosition := b.index.writePosition
	b.mapFile.WriteUint32(uint32(len(bytes)), int64(currentWritePosition))
	b.mapFile.WriteAt(bytes, int64(currentWritePosition+4))
	b.index.writePosition = uint32(len(bytes) + 4)
}

func (b *HQueueBlock) eof() bool {
	readPosition := b.index.readPosition
	v, _ := b.mapFile.ReadInt(int64(readPosition))
	return int64(v) == EOF
}

func (b *HQueueBlock) read() []byte {
	currentReadPosition := b.index.readPosition
	dataLen, _ := b.mapFile.ReadUint32(int64(currentReadPosition))
	data := make([]byte, dataLen)
	b.mapFile.ReadAt(data, int64(currentReadPosition+4))
	return data
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

func formatHqueueBlockPath(dataDir string, queueName string, blockNum uint32) string {
	return dataDir + string(os.PathSeparator) + queueName + string(os.PathSeparator) + string(blockNum) + BLOCK_FILE_SUFFIX
}
