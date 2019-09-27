package HKafkaQueue

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var indexPath = "/home/zhanxiaohui/hkafkaqueue/test"

func TestNewHQueueIndex(t *testing.T) {
	hqueueIndex := NewHQueueIndex(indexPath)
	hqueueIndex.sync()
	hqueueIndex.close()
}

func TestReadExistIndex(t *testing.T) {
	hqueueIndex := NewHQueueIndex(indexPath)
	assert.Equal(t, "v1.00000", hqueueIndex.versionNo)
	assert.Equal(t, 0, hqueueIndex.readBlockNum)
	assert.Equal(t, 0, hqueueIndex.readPosition)
	assert.Equal(t, 0, hqueueIndex.readCounter)
	assert.Equal(t, 0, hqueueIndex.writeBlockNum)
	assert.Equal(t, 0, hqueueIndex.writePosition)
	assert.Equal(t, 0, hqueueIndex.writeCounter)
	hqueueIndex.close()
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

	hqueueIndex2 := NewHQueueIndex(indexPath)
	assert.Equal(t, 1, hqueueIndex2.readBlockNum)
	assert.Equal(t, 1, hqueueIndex2.writeBlockNum)
	assert.Equal(t, 10, hqueueIndex2.readPosition)
	assert.Equal(t, 20, hqueueIndex2.writePosition)
	assert.Equal(t, 10, hqueueIndex2.readCounter)
	assert.Equal(t, 10, hqueueIndex2.writeCounter)
}
