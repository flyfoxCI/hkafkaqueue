package HKafkaQueue

import (
	"github.com/grandecola/mmap"
)

type FileImpl struct {
	mmap.File
}


func (m *FileImpl) ReadUint32(offset int64) (uint32, error) {
	bytes:=make([]byte,4)
	m.ReadAt(bytes,offset)
	return bytes2Uint32(bytes), nil
}

func (m *FileImpl) ReadInt(offset int64) (int, error) {
	bytes:=make([]byte,4)
	m.ReadAt(bytes,offset)
	return bytes2Int(bytes), nil
}

func (m *FileImpl) WriteUint32(num uint32, offset int64) (int, error) {
	bytes:=int2bytes(int(num))
	m.WriteAt(bytes,offset)
	return len(bytes),nil
}
