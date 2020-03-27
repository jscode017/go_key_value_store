package go_kvstore

import (
	"errors"
)

const (
	MinimumDegree = 14
)

//TODO: change int to int64 or int32

type Node struct {
	ID       uint64
	IsLeaf   bool
	Datas    []KVPair
	Children []uint64
}
type KVPair struct {
	Key   string //length:30
	Value string //length:100
}
type BTree struct {
	Root *Node
}

func NewNode(isLeaf bool) *Node {
	return &Node{
		IsLeaf: isLeaf,
	}
}
func NewTree() *BTree {
	node := NewNode(true)
	return &BTree{
		Root: node,
	}
}

func Search(db *DB, root *Node, key string) (string, error) {
	keyIndex := 0
	for keyIndex < len(root.Datas) && key > root.Datas[keyIndex].Key {
		keyIndex++
	}
	if keyIndex < len(root.Datas) && root.Datas[keyIndex].Key == key {
		return root.Datas[keyIndex].Value, nil
	}

	if root.IsLeaf {
		return "", errors.New("key not exist")
	}
	//todo: perform disk read
	child, err := db.ReadNodeFromID(root.Children[keyIndex])
	if err != nil {
		return "", err
	}
	return Search(db, child, key)
}

func (btree *BTree) Insert(db *DB, key, value string) error {
	root := btree.Root
	if root == nil {
		node := NewNode(true)
		node.ID = db.CurrentPageNums
		db.CurrentPageNums++
		node.Datas = []KVPair{
			KVPair{
				Key:   key,
				Value: value,
			},
		}
		btree.Root = node
		err := db.WriteDirtyPage(0, node)
		if err != nil {
			return err
		}
		return nil
	}
	if len(root.Datas) == 0 { //I do not think this should happen
		root.Datas = []KVPair{
			KVPair{
				Key:   key,
				Value: value,
			},
		}
		err := db.WriteDirtyPage(0, root)
		if err != nil {
			return err
		}
		return nil
	}
	if len(root.Datas) == 2*MinimumDegree-1 {
		newRoot := NewNode(false)
		newRoot.ID = 0
		btree.Root = newRoot

		root.ID = db.CurrentPageNums
		db.CurrentPageNums++

		newRoot.Children = append(newRoot.Children, root.ID)
		err := db.WriteDirtyPage(0, newRoot)
		if err != nil {
			return err
		}

		err = db.WriteDirtyPage(root.ID, root)
		if err != nil {
			return err
		}

		err = SplitChild(db, newRoot, 0) // disk write perform in SplitChild
		if err != nil {
			return err
		}

		err = InsertNoneFull(db, newRoot, key, value)
		if err != nil {
			return err
		}
	} else {
		err := InsertNoneFull(db, root, key, value)
		if err != nil {
			return err
		}
	}

	return nil
}

func InsertNoneFull(db *DB, root *Node, key, value string) error {
	if root.IsLeaf {
		index := len(root.Datas) - 1
		for index >= 0 && key < root.Datas[index].Key {
			index--
		}

		if index >= 0 && root.Datas[index].Key == key {
			root.Datas[index].Value = value
			err := db.WriteDirtyPage(root.ID, root)

			if err != nil {
				return err
			}
			return nil
		}

		root.Datas = append(root.Datas, KVPair{})
		for i := len(root.Datas) - 1; i >= index+2; i-- {
			root.Datas[i].Key = root.Datas[i-1].Key
			root.Datas[i].Value = root.Datas[i-1].Value
		}
		root.Datas[index+1] = KVPair{
			Key:   key,
			Value: value,
		}

		err := db.WriteDirtyPage(root.ID, root)
		if err != nil {
			return err
		}
		return nil
	} else {
		index := len(root.Datas) - 1
		for index >= 0 && key < root.Datas[index].Key {
			index--
		}
		if index >= 0 && root.Datas[index].Key == key {
			root.Datas[index].Value = value
			err := db.WriteDirtyPage(root.ID, root)

			if err != nil {
				return err
			}
			return nil
		}
		index += 1
		child, err := db.ReadNodeFromID(root.Children[index])
		if err != nil {
			return err
		}
		if len(child.Datas) == 2*MinimumDegree-1 {
			err := SplitChild(db, root, index)
			if err != nil {
				return nil
			}
			if key > root.Datas[index].Key {
				index++
			}
		}
		child, err = db.ReadNodeFromID(root.Children[index])
		if err != nil {
			return err
		}
		err = InsertNoneFull(db, child, key, value)
		if err != nil {
			return err
		}
		return nil
	}
}
func SplitChild(db *DB, root *Node, index int) error {
	child, err := db.ReadNodeFromID(root.Children[index])
	if err != nil {
		return err
	}
	keyToBeMoveUp := child.Datas[MinimumDegree-1].Key
	valueToBeMoveUp := child.Datas[MinimumDegree-1].Value

	splitedChild := NewNode(child.IsLeaf)
	splitedChild.Datas = make([]KVPair, MinimumDegree-1)
	for i := range splitedChild.Datas {
		splitedChild.Datas[i].Key = ""
		splitedChild.Datas[i].Value = ""
	}
	splitedChild.ID = db.CurrentPageNums
	db.CurrentPageNums++
	//copy(splitedChild.Datas[:], child.Datas[MinimumDegree:])
	for i := 0; i < len(splitedChild.Datas); i++ {
		splitedChild.Datas[i].Key = child.Datas[MinimumDegree+i].Key
		splitedChild.Datas[i].Value = child.Datas[MinimumDegree+i].Value
	}
	child.Datas = child.Datas[:MinimumDegree-1]
	splitedChild.IsLeaf = child.IsLeaf

	if !child.IsLeaf {
		splitedChild.Children = make([]uint64, MinimumDegree)
		copy(splitedChild.Children[:], child.Children[MinimumDegree:])
		child.Children = child.Children[:MinimumDegree]
	}

	root.Children = append(root.Children, 0) //append a dummy childID
	copy(root.Children[index+2:], root.Children[index+1:len(root.Children)-1])
	root.Children[index+1] = splitedChild.ID

	root.Datas = append(root.Datas, KVPair{Key: "", Value: ""})
	//copy(root.Datas[index+1:], root.Datas[index:len(root.Datas)-1])
	for i := len(root.Datas) - 1; i >= index+1; i-- {
		root.Datas[i].Key = root.Datas[i-1].Key
		root.Datas[i].Value = root.Datas[i-1].Value
	}

	root.Datas[index].Key = keyToBeMoveUp
	root.Datas[index].Value = valueToBeMoveUp

	err = db.WriteDirtyPage(root.ID, root)
	if err != nil {
		return err
	}

	err = db.WriteDirtyPage(child.ID, child)
	if err != nil {
		return err
	}

	err = db.WriteDirtyPage(splitedChild.ID, splitedChild)
	if err != nil {
		return err
	}

	return nil
}
func SearchForChildIndex(node *Node, key string) int { //-1 if is leaf and key not exist
	index := 0
	for index < len(node.Datas) && key > node.Datas[index].Key {
		index++
	}
	return index
}

func Tranverse(db *DB, node *Node) ([]KVPair, error) {
	if node == nil {
		return []KVPair{}, nil
	}
	if node.IsLeaf {
		return node.Datas, nil
	}

	var increasingKeys []KVPair
	increasingKeys = make([]KVPair, 0)
	for i := 0; i <= len(node.Datas); i++ {
		child, err := db.ReadNodeFromID(node.Children[i])
		if err != nil {
			return []KVPair{}, err
		}
		appendKeys, err := Tranverse(db, child)
		increasingKeys = append(increasingKeys, appendKeys...)
		if i != len(node.Datas) {
			increasingKeys = append(increasingKeys, node.Datas[i])
		}
	}
	return increasingKeys, nil
}

// the deletion starts here
/*func (tree *BTree) Delete(key string) {
	TranverseAndDeleteNode(tree.Root, key)
	if len(tree.Root.Datas)==0{
		if tree.Root.IsLeaf{
			tree.Root=nil
		}else{
			//TODO: diskread child[0]
			tree.Root=tree.Root.Children[0]
		}
		//TODO: disk delete old root
	}
}

func TranverseAndDeleteNode(node *Node, key string) {
	childIndex := SearchForChildIndex(node, key)
	if node.IsLeaf {
		if childIndex == len(node.Datas) || key != node.Datas[childIndex].Key {
			return //TODO: add a return error (key not found)
		} else {
			node.Datas = append(node.Datas[:childIndex], node.Datas[childIndex+1:]...)
			return
		}
	} else {
		if childIndex < len(node.Datas) && node.Datas[childIndex].Key == key {
			if len(node.Children[childIndex].Datas) >= MinimumDegree {
				child := node.Children[childIndex]
				node.Datas[childIndex].Key = child.Datas[len(child.Datas)-1].Key
				TranverseAndDeleteNode(child, child.Datas[len(child.Keys)-1].Key)
				return
			} else if childIndex+1 < len(node.Children) && len(node.Children[childIndex+1].Datas) >= MinimumDegree {
				child := node.Children[childIndex+1]
				node.Datas[childIndex] = child.Datas[0]
				TranverseAndDeleteNode(child, child.Datas[0].Key)
				return
			} else { //merge
				child := node.Children[childIndex]
				sibling := &Node{}

				if childIndex+1 < len(node.Children) {
					sibling = node.Children[childIndex+1]
					child.Datas = append(child.Datas, node.Datas[childIndex])
					child.Datas = append(child.Datas, sibling.Datas...)
					child.Children = append(child.Children, sibling.Children...)
					node.Datas = append(node.Datas[:childIndex], node.Datas[childIndex+1:]...)
					node.Children = append(node.Children[:childIndex], node.Children[childIndex+1:]...)
					//TODO: disk delete sibling
					TranverseAndDeleteNode(child, key)
					return
				} else {
					sibling = node.Children[childIndex-1]
					sibling.Datas = append(sibling.Datas, node.Datas[childIndex])
					sibling.Datas = append(sibling.Datas, child.Datas...)
					sibling.Children = append(sibling.Children, child.Children...)
					node.Datas = append(node.Datas[:childIndex-1], node.Datas[childIndex:]...)
					node.Children = append(node.Children[:childIndex-1], node.Children[childIndex:]...)
					//TODO: disk delete child
					TranverseAndDeleteNode(sibling, key)
					return
				}

			}
		} else if len(node.Children[childIndex].Datas) == MinimumDegree-1 {
			mergeLeft := FillChild(node, childIndex)

			if mergeLeft {
				TranverseAndDeleteNode(node.Children[childIndex-1], key)
			} else {
				TranverseAndDeleteNode(node.Children[childIndex], key)
			}
		} else {
			TranverseAndDeleteNode(node.Children[childIndex], key)
		}
	}

}

//the return value is true if merge left sibling
func FillChild(parent *Node, childIndex int) bool {
	child := parent.Children[childIndex]
	if childIndex+1 < len(parent.Children) && len(parent.Children[childIndex+1].Datas) >= MinimumDegree {
		sibling := parent.Children[childIndex+1]
		child.Datas = append(child.Datas, parent.Datas[childIndex])
		parent.Datas[childIndex] = sibling.Datas[0]
		sibling.Datas = sibling.Datas[1:]
		if !child.IsLeaf{
			child.Children=append(child.Children,sibling.Children[0])
			sibling.Children=sibling.Children[1:]
		}
		return false
	} else if childIndex-1 >= 0 && len(parent.Children[childIndex-1].Keys) >= MinimumDegree {
		sibling := parent.Children[childIndex-1]
		child.Datas = append([]KVPair{parent.Datas[childIndex-1]}, child.Datas...)
		parent.Datas[childIndex-1] = sibling.Datas[len(sibling.Datas)-1]
		sibling.Datas = sibling.Datas[:len(sibling.Datas)-1]
		if !child.IsLeaf{
			child.Children=append([]*Node{sibling.Children[len(sibling.Children)-1]},child.Children...)
			sibling.Children=sibling.Children[:len(sibling.Children)-1]
		}
		return false
	} else if childIndex+1 < len(parent.Children) {
		sibling := parent.Children[childIndex+1]
		child.Datas = append(child.Datas, parent.Datas[childIndex])
		child.Datas = append(child.Datas, sibling.Datas...)
		child.Children = append(child.Children, sibling.Children...)
		parent.Datas = append(parent.Datas[:childIndex], parent.Datas[childIndex+1:]...)
		parent.Children = append(parent.Children[:childIndex+1], parent.Children[childIndex+2:]...)
		//TODO: disk delete sibling
		return false
	} else if childIndex-1 >= 0 {
		sibling := parent.Children[childIndex-1]
		sibling.Datas = append(sibling.Datas, parent.Datas[childIndex-1])
		sibling.Datas = append(sibling.Datas, child.Datas...)
		sibling.Children = append(sibling.Children, child.Children...)
		parent.Datas = append(parent.Datas[:childIndex-1], parent.Datas[childIndex:]...)
		parent.Children = append(parent.Children[:childIndex], parent.Children[childIndex+1:]...)
		//TODO: disk delete child
		return true
	}
	return false //TODO: throw error should not reach here
}*/
