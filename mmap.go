package go_kvstore

import (
	"os"
	"syscall"
)

func MMap(file *os.File, prot int) ([]byte, bool, error) {
	fileSize, err := GetFileSize(file)
	if err != nil {
		return []byte{}, false, err
	}

	firstCreate := false
	if fileSize == 0 {
		firstCreate = true
		_, err := file.WriteAt([]byte{byte(0)}, 1<<12-1)
		if err != nil {
			return []byte{}, firstCreate, err
		}
		fileSize += PageSize
	}
	buf, err := syscall.Mmap(int(file.Fd()), 0, fileSize, prot, syscall.MAP_SHARED)
	if err != nil {
		return []byte{}, firstCreate, err
	}
	return buf, firstCreate, nil
}

func GetFileSize(file *os.File) (int, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return -1, err
	}

	return int(fileInfo.Size()), nil
}
