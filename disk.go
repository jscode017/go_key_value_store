package go_kvstore

import (
	"encoding/binary"
)

func DiskRead(id int, buf []byte) (*Node, error) {
	//
	offset := id * 4096
	diskNode := buf[offset : offset+4096]
	node, err := BytesToTreeNode(diskNode)
	if err != nil {
		return nil, err
	}
	return node, nil
	//defer file.Close() //ignore error
	//lock and mmap
}
func DiskWrite(id int, buf []byte, node *Node) error {
	offset := id * 4096

	modifiedContent, err := TreeNodeToBytes(node)
	if err != nil {
		return err
	}

	copy(buf[offset:offset+4096], modifiedContent)

	return nil
}
func TreeNodeToBytes(node *Node) ([]byte, error) {
	retBytes := make([]byte, 4096)
	bufPtr := 0
	retBytes[bufPtr] = 0x1
	bufPtr++
	binary.BigEndian.PutUint64(retBytes[bufPtr:bufPtr+8], node.ID)
	bufPtr += 8
	dataLen := uint64(len(node.Datas))
	binary.BigEndian.PutUint64(retBytes[bufPtr:bufPtr+8], dataLen)
	bufPtr += 8
	for i := 0; i < int(dataLen); i++ {
		keyLen := uint16(len(node.Datas[i].Key))
		binary.BigEndian.PutUint16(retBytes[bufPtr:bufPtr+2], keyLen)
		bufPtr += 2

		copy(retBytes[bufPtr:bufPtr+len(node.Datas[i].Key)], []byte(node.Datas[i].Key))
		bufPtr += 30

		valueLen := uint16(len(node.Datas[i].Value))
		binary.BigEndian.PutUint16(retBytes[bufPtr:bufPtr+2], valueLen)
		bufPtr += 2

		copy(retBytes[bufPtr:bufPtr+len(node.Datas[i].Value)], []byte(node.Datas[i].Value))
		bufPtr += 100
	}

	bufPtr += ((2*MinimumDegree - 1 - int(dataLen)) * 134) //28 data spaces
	if !node.IsLeaf {
		retBytes[bufPtr] = 0x0
		bufPtr++

		childLen := dataLen + 1
		for i := 0; i < int(childLen); i++ {
			binary.BigEndian.PutUint64(retBytes[bufPtr:bufPtr+8], node.Children[i])
			bufPtr += 8
		}
	} else {
		retBytes[bufPtr] = 0x1
	}
	return retBytes, nil
}
func BytesToTreeNode(buf []byte) (*Node, error) {
	node := &Node{}
	bufPtr := 0
	if buf[bufPtr] == 0x0 { //empty node
		return nil, nil
	}

	bufPtr++
	id := binary.BigEndian.Uint64(buf[bufPtr : bufPtr+8])
	node.ID = id
	bufPtr += 8
	dataLen := int(binary.BigEndian.Uint64(buf[bufPtr : bufPtr+8]))
	node.Datas = make([]KVPair, dataLen)
	bufPtr += 8

	for i := 0; i < int(dataLen); i++ {
		keyLen := int(binary.BigEndian.Uint16(buf[bufPtr : bufPtr+2])) //keyLen should not be more then 30,do this check at write

		bufPtr += 2

		key := string(buf[bufPtr : bufPtr+keyLen])
		bufPtr += 30
		valueLen := int(binary.BigEndian.Uint16(buf[bufPtr : bufPtr+2])) ////keyLen should not be more then 30,do this check at write
		bufPtr += 2
		val := string(buf[bufPtr : bufPtr+valueLen])
		bufPtr += 100
		keyValuePair := KVPair{
			Key:   key,
			Value: val,
		}
		node.Datas[i] = keyValuePair
	}
	bufPtr += (2*MinimumDegree - 1 - dataLen) * 134 //28 data spaces

	isLeaf := (buf[bufPtr] != 0x0)
	node.IsLeaf = isLeaf
	bufPtr += 1

	if !isLeaf {
		childLen := dataLen + 1
		node.Children = make([]uint64, childLen)
		for i := 0; i < int(childLen); i++ {
			childID := binary.BigEndian.Uint64(buf[bufPtr : bufPtr+8])
			bufPtr += 8
			node.Children[i] = childID
		}
	} else {
		node.Children = make([]uint64, 0)
	}
	return node, nil
}
