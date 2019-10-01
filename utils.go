package HKafkaQueue

import (
	"encoding/binary"
	"os"
	"unsafe"
)

func int2bytes(i int) []byte {
	bs := make([]byte, 4)
	if isLittleEndian() {
		binary.LittleEndian.PutUint32(bs, uint32(i))
	} else {
		binary.BigEndian.PutUint32(bs, uint32(i))
	}

	return bs
}

func bytes2Uint32(bytes []byte) uint32 {
	var result uint32
	if isLittleEndian() {
		result = binary.LittleEndian.Uint32(bytes)
	} else {
		result = binary.BigEndian.Uint32(bytes)
	}

	return result
}

func bytes2Int(bytes []byte) int {
	var result int
	if isLittleEndian() {
		result = int(binary.LittleEndian.Uint64(bytes))
	} else {
		result = int(binary.BigEndian.Uint64(bytes))
	}

	return result
}

func isLittleEndian() bool {
	s := int16(0x1234)
	b := int8(s)
	return 0x34 == b

}

func String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func isExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		} else {
			return false
		}
	}
	return true
}
