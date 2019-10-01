package HKafkaQueue

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func setup() (*HQueueBlock, error) {
	var indexPath = "/tmp/hkafkaqueue/test.idx"
	var blkPath = "/tmp/hkafkaqueue/test.blk"
	hqueueIndex := NewHQueueIndex(indexPath)
	blk, err := NewHQueueBlock(hqueueIndex, blkPath)
	return blk, err
}

func TestHQueueBlockWrite(t *testing.T) {
	blk, err := setup()
	if err != nil {
		t.Fatalf("write block faild %s", err)
	}
	blk.write([]byte("helloworld"))
	blk.index.close()
	blk.close()
}

func TestHQueueBlockRead(t *testing.T) {
	blk, err := setup()
	if err != nil {
		t.Fatalf("write block faild %s", err)
	}
	bytes, err := blk.read()
	assert.Equal(t, "helloworld", String(bytes))

}

func TestBlockFull(t *testing.T) {
	blk, err := setup()
	if err != nil {
		t.Fatalf("write block faild %s", err)
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
		t.Fatalf("write block faild %s", err)
	}
	for {
		if !blk.eof() {
			bytes, _ := blk.read()
			fmt.Println(String(bytes))
			fmt.Println(blk.index.readCounter)
			fmt.Println(blk.index.readPosition)
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
		t.Fatalf("write block faild %s", err)
	}
	n := blk.duplicate()
	blk.blockFilePath = "/tmp/test2/txt"
	fmt.Println(n.blockFilePath)
}
