package HKafkaQueue

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func setup(consumerName ...string) (*HQueueBlock, error) {
	var indexPath = "/tmp/hkafkaqueue/consumer-p1.idx"
	var blkPath = "/tmp/hkafkaqueue/test2.blk"
	hqueueIndex := NewHQueueIndex(indexPath)
	blk, err := NewHQueueBlock(hqueueIndex, blkPath)
	return blk, err
}

func TestReadNull(t *testing.T) {
	blk, err := setup()
	if err != nil {
		t.Fatalf("write block faild: %s", err)
	}
	blk.write([]byte{byte(1), byte(0)})
	blk.sync()
	data := make([]byte, 100)
	blk.mapFile.ReadAt(data, 0)
	d := data[91:99]
	fmt.Println(bytes2Int(d))
	fmt.Println(data)

}

func TestHQueueBlockWrite(t *testing.T) {
	blk, err := setup()
	if err != nil {
		t.Fatalf("write block faild: %s", err)
	}
	blk.write([]byte("helloworld"))
	blk.index.close()
	blk.close()
}

func TestHQueueBlockRead(t *testing.T) {
	blk, err := setup()
	if err != nil {
		t.Fatalf("write block faild: %s", err)
	}
	bytes, err := blk.read()
	assert.Equal(t, "helloworld", String(bytes))

}

func TestBlockFull(t *testing.T) {
	blk, err := setup()
	if err != nil {
		t.Fatalf("write block faild: %s", err)
	}
	for {
		if blk.isSpaceAvailable(10) {
			blk.write([]byte("helloworld"))
		} else {
			blk.putEOF()
			blk.index.close()
			blk.close()
			break
		}
	}
}

func TestBlockRead(t *testing.T) {
	blk, err := setup()
	if err != nil {
		t.Fatalf("write block faild: %s", err)
	}
	for {
		if !blk.eof() {
			bytes, _ := blk.read()
			fmt.Println(String(bytes))
			fmt.Println(blk.index.counter)
			fmt.Println(blk.index.position)
		} else {
			break
		}
	}
	blk.index.close()
	blk.close()

}

func TestDuplicate(t *testing.T) {
	blk, err := setup()
	if err != nil {
		t.Fatalf("write block faild: %s", err)
	}
	n := blk.duplicate()
	blk.blockFilePath = "/tmp/test2/txt"
	fmt.Println(n.blockFilePath)
}
