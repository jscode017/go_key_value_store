package go_kvstore

import (
	"errors"
	"os"
	"syscall"
	"time"
)

const (
	PageSize = 4096
)

type DB struct {
	CurrentPageNums uint64
	FileName        string
	File            *os.File
	DirtyPageMap    map[uint64]*DirtyPage
	MmapContent     []byte
}

func (db *DB) Init(fileName string) error {

	err := db.Open(fileName, os.O_RDWR)
	if err != nil {
		return err
	}

	err = db.FileLock(syscall.LOCK_EX, 100)
	if err != nil {
		return err
	}
	db.DirtyPageMap = make(map[uint64]*DirtyPage)
	mmapContent, firstCreate, err := MMap(db.File, syscall.PROT_WRITE|syscall.PROT_READ)
	if err != nil {
		return err
	}
	db.MmapContent = mmapContent

	if firstCreate {
		db.CurrentPageNums = 0
	} else {
		db.CurrentPageNums = uint64(len(db.MmapContent) / PageSize)
	}
	return nil
}
func (db *DB) Open(fileName string, flag int) error {
	file, err := os.OpenFile(fileName, flag|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	db.File = file
	db.FileName = fileName
	return nil
}
func (db *DB) FileLock(how int, timeout int) error {
	for {
		err := syscall.Flock(int(db.File.Fd()), how|syscall.LOCK_NB)
		if err != nil {
			return nil
		} else {
			if err == syscall.EAGAIN {
				time.Sleep(time.Millisecond * time.Duration(timeout))
				continue
			} else {
				return err
			}
		}
	}
}
func (db *DB) FileUnLock() error {
	err := syscall.Flock(int(db.File.Fd()), syscall.LOCK_UN)
	if err != nil {
		return err
	}
	return nil
}
func (db *DB) Close() error {
	err := syscall.Munmap(db.MmapContent)
	if err != nil {
		return err
	}
	err = db.File.Close()
	if err != nil {
		return err
	}
	return nil
}
func (db *DB) ProcessTransaction(fileName, key string) (string, error) {
	err := db.Init(fileName)
	if err != nil {
		return "", err
	}

	value, err := db.Read(key)
	if err != nil {
		return "", err
	}

	return value, nil
}
func (db *DB) Read(key string) (string, error) {

	root, err := db.GetRoot()
	if err != nil {
		return "", nil
	}
	value, err := Search(db, root, key)

	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *DB) GetRoot() (*Node, error) {
	return db.ReadNodeFromID(0)
}
func (db *DB) ReadNodeFromID(id uint64) (*Node, error) {

	bytesFromRoot, hit := db.DirtyPageLookUp(id)
	if !hit {
		if int((id+1)*PageSize) > len(db.MmapContent) {
			return nil, errors.New("key not exsist, node id too large")
		}
		node, err := DiskRead(int(id), db.MmapContent)

		if err != nil {
			return nil, err
		}
		if node == nil {
			return nil, nil
		}

		bytesFromRoot, err = TreeNodeToBytes(node)
		if err != nil {
			return nil, err
		}
		dirtyPage := &DirtyPage{
			Content: bytesFromRoot,
			IsDirty: false,
		}
		db.DirtyPageMap[id] = dirtyPage
	}

	node, err := BytesToTreeNode(bytesFromRoot)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (db *DB) DirtyPageLookUp(id uint64) ([]byte, bool) {
	if content, hit := db.DirtyPageMap[id]; hit {
		return content.Content, true
	} else {
		return []byte{}, false
	}
}

func (db *DB) Write(key, value string) error {
	root, err := db.GetRoot()
	if err != nil {
		return err
	}
	btree := NewTree()
	btree.Root = root

	btree.Insert(db, key, value)
	return nil
}

func (db *DB) WriteDirtyPage(id uint64, node *Node) error {
	bytesFromNode, err := TreeNodeToBytes(node)
	if err != nil {
		return err
	}

	if _, hit := db.DirtyPageMap[id]; hit {
		db.DirtyPageMap[id].IsDirty = true
		db.DirtyPageMap[id].Content = make([]byte, PageSize)
		copy(db.DirtyPageMap[id].Content, bytesFromNode)
	} else {
		dirtyPage := &DirtyPage{
			IsDirty: true,
			Content: make([]byte, PageSize),
		}
		copy(dirtyPage.Content, bytesFromNode)
		db.DirtyPageMap[id] = dirtyPage
	}
	return nil
}
func (db *DB) Commit() error {
	if int(PageSize*db.CurrentPageNums) > len(db.MmapContent) {
		err := db.Extend()
		if err != nil {
			return err
		}
	}
	for id, page := range db.DirtyPageMap {
		if page.IsDirty {
			copy(db.MmapContent[id*PageSize:id*PageSize+PageSize], page.Content)
		}
	}
	return nil
}

func (db *DB) Extend() error {
	err := db.File.Close()
	if err != nil {
		return err
	}

	err = db.Open(db.FileName, os.O_RDWR)
	if err != nil {
		return err
	}

	err = syscall.Ftruncate(int(db.File.Fd()), int64(PageSize*db.CurrentPageNums))
	if err != nil {
		return err
	}

	mmapContent, _, err := MMap(db.File, syscall.PROT_WRITE|syscall.PROT_READ)
	if err != nil {
		return err
	}
	db.MmapContent = mmapContent
	return nil
}
func (db *DB) Clear() error {
	err := db.Close()
	if err != nil {
		return err
	}
	for id, _ := range db.DirtyPageMap {
		delete(db.DirtyPageMap, id)
	}
	return nil
}
