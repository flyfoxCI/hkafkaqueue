package HKafkaQueue

import (
	"github.com/golang/glog"
	"github.com/grandecola/mmap"
	"math"
	"os"
	"strconv"
	"syscall"
)

const BLOCK_FILE_SUFFIX = ".blk"
const BLOCK_SIZE = 32 * 1024 * 1024
const EOF = math.MaxUint32
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
	b.mapFile.WriteUint64At(EOF, int64(b.index.position))

}

func (b *HQueueBlock) sync() {
	TryCatch{}.Try(func() {
		err := b.mapFile.Flush(syscall.MS_SYNC)
		if err != nil {
			panic(err)
		}
	}).CatchAll(func(err error) {
		glog.Errorf("sync map file: %s error: %s", b.blockFile.Name(), err)
	})
}

func (b *HQueueBlock) isSpaceAvailable(len uint64) bool {
	remain := uint64(BLOCK_SIZE) - b.index.position
	var flag bool = false
	if remain > len+16 {
		flag = true
	}
	return flag
}

func (b *HQueueBlock) write(bytes []byte) (int, error) {
	currentWritePosition := b.index.position
	b.mapFile.WriteUint64At(uint64(len(bytes)), int64(currentWritePosition))
	writeLen, err := b.mapFile.WriteAt(bytes, int64(currentWritePosition+8))
	b.index.position = currentWritePosition + uint64(len(bytes)+8)
	b.index.putPosition(currentWritePosition + uint64(len(bytes)+8))
	b.index.putCounter(b.index.counter + 1)
	return writeLen, err
}

func (b *HQueueBlock) eof() bool {
	readPosition := b.index.position
	u := b.mapFile.ReadUint64At(int64(readPosition))
	return u == EOF
}

func (b *HQueueBlock) read() ([]byte, error) {
	currentReadPosition := b.index.position
	dataLen := b.mapFile.ReadUint64At(int64(currentReadPosition))
	if dataLen == 0 || dataLen == EOF {
		return nil, &ReadZeroError{}
	}
	if dataLen < 0 || dataLen >= math.MaxUint32 {
		glog.Errorf(" put data length error :%d", dataLen)
		return nil, &PutLengthError{}
	}
	data := make([]byte, dataLen, dataLen+10)
	_, err := b.mapFile.ReadAt(data, int64(currentReadPosition+8))
	if err != nil {
		return nil, err
	}
	if data[dataLen-1] == 0 {
		return nil, &ReadDirtyError{}
	}
	b.index.putPosition(currentReadPosition + 8 + dataLen)
	b.index.putCounter(b.index.counter + 1)
	return data, nil
}

func (b *HQueueBlock) close() {
	if b == nil {
		return
	}
	TryCatch{}.Try(func() {
		b.sync()
	}).CatchAll(func(err error) {
		glog.Errorf("close block file err: %s", err)
	}).Finally(func() {
		b.mapFile.Unmap()
		b.blockFile.Close()
	})

}

func (b *HQueueBlock) duplicate() *HQueueBlock {
	newBlock := &b
	return *newBlock
}

func formatHqueueBlockPath(rootDir string, queueName string, blockNum uint64) string {
	return getHqueueDataDir(rootDir) + string(os.PathSeparator) + queueName + string(os.PathSeparator) + strconv.FormatUint(blockNum, 10) + BLOCK_FILE_SUFFIX
}
