package HKafkaQueue

import (
	"fmt"
	"github.com/grandecola/mmap"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"strings"
	"syscall"
	"testing"
)

var indexPath = "/tmp/hkafkaqueue/test.idx"

func TestNewHQueueIndex(t *testing.T) {
	hqueueIndex := NewHQueueIndex(indexPath)
	hqueueIndex.sync()
	hqueueIndex.close()
}

func TestReadExistIndex(t *testing.T) {
	hqueueIndex := NewHQueueIndex(indexPath)
	assert.Equal(t, "v1.00000", hqueueIndex.versionNo)
	assert.Equal(t, uint64(0), hqueueIndex.readBlockNum)
	assert.Equal(t, uint64(0), hqueueIndex.readPosition)
	assert.Equal(t, uint64(0), hqueueIndex.readCounter)
	assert.Equal(t, uint64(0), hqueueIndex.writeBlockNum)
	assert.Equal(t, uint64(0), hqueueIndex.writePosition)
	assert.Equal(t, uint64(0), hqueueIndex.writeCounter)
}

func TestPutData(t *testing.T) {
	hqueueIndex := NewHQueueIndex(indexPath)
	hqueueIndex.putWriteBlockNum(1)
	hqueueIndex.putReadBlockNum(1)
	hqueueIndex.putReadPosition(10)
	hqueueIndex.putWritePosition(20)
	hqueueIndex.putReadCounter(10)
	hqueueIndex.putWriteCounter(10)
	hqueueIndex.close()
}

func TestReadData(t *testing.T) {
	hqueueIndex := NewHQueueIndex(indexPath)
	fmt.Println(hqueueIndex.readBlockNum)
	fmt.Println(hqueueIndex.readPosition)
	fmt.Println(hqueueIndex.readCounter)
	fmt.Println(hqueueIndex.writeBlockNum)
	fmt.Println(hqueueIndex.writePosition)
	fmt.Println(hqueueIndex.writeCounter)
	hqueueIndex.close()
}

func TestMmapFile(t *testing.T) {
	f, errFile := os.OpenFile("/tmp/hkafkaqueue/mmap_test5.txt", os.O_RDWR|os.O_CREATE, 0644)
	if _, err := f.WriteAt([]byte{byte(0)}, 1<<6-1); nil != err {
		log.Fatalln(err)
	}
	if errFile != nil {
		t.Fatalf("error in opening file :: %v", errFile)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Fatalf("error in closing file :: %v", err)
		}
	}()
	m, errMmap := mmap.NewSharedFileMmap(f, 0, 1<<6, syscall.PROT_READ|syscall.PROT_WRITE)
	if errMmap != nil {
		t.Fatalf("error in mapping :: %v", errMmap)
	}
	defer func() {
		if err := m.Unmap(); err != nil {
			t.Fatalf("error in calling unmap :: %v", err)
		}
	}()
	// Write
	var i *HQueueIndex = new(HQueueIndex)
	i.mapFile = m
	i.putMagic()
	i.putReadBlockNum(0)
	i.putReadPosition(1)
	i.putReadCounter(2)
	i.putWriteBlockNum(3)
	i.putWritePosition(4)
	i.putWriteCounter(5)
	i.sync()
	sb := strings.Builder{}
	sb.Grow(8)
	m.ReadStringAt(&sb, 0)
	fmt.Println(sb.String())
	fmt.Println(m.ReadUint64At(8))
	fmt.Println(m.ReadUint64At(16))
	fmt.Println(m.ReadUint64At(24))
	fmt.Println(m.ReadUint64At(32))
	fmt.Println(m.ReadUint64At(40))
	fmt.Println(m.ReadUint64At(48))
	fmt.Println(m.ReadUint64At(56))
	fmt.Println(m.ReadUint64At(64))
	//p:=make([]byte,1)
	//m.ReadAt(p,63)
	//fmt.Println(String(p))
}
