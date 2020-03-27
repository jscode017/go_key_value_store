package go_kvstore

import (
	"testing"
)

func TestNodeToBytesRoundTrip(t *testing.T) {
	node1 := NewNode(false)
	node1.ID = 17
	kvpair1 := KVPair{
		Key:   "k",
		Value: "v",
	}
	kvpair2 := KVPair{
		Key:   "f",
		Value: "h",
	}
	node1.Datas = []KVPair{kvpair1, kvpair2}
	node1.Children = []uint64{2, 3, 4}
	nodeBytes, err := TreeNodeToBytes(node1)
	if err != nil {
		t.Fatal(err)
	}

	node2, err := BytesToTreeNode(nodeBytes)
	if err != nil {
		t.Fatal(err)
	}

	if node1.ID != node2.ID {
		t.Fatal("round trip test failed")
	}

	if len(node1.Datas) != len(node2.Datas) {
		t.Fatal("data length not equal")
	}

	for i := range node1.Datas {
		if node1.Datas[i] != node2.Datas[i] {
			t.Fatal("round trip test failed")
		}
	}

	if len(node1.Children) != len(node2.Children) {
		t.Fatal("children length not equal")
	}

	for i := range node1.Children {
		if node1.Children[i] != node2.Children[i] {
			t.Fatal("round trip test failed")
		}
	}

	if node1.IsLeaf != node2.IsLeaf {
		t.Fatal("round trip test failed")
	}

}
