package go_kvstore

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestWrite(t *testing.T) {
	err := os.RemoveAll(filepath.Join("./", "testing0"))
	if err != nil {
		t.Fatal(err)
	}
	db := &DB{}
	err = db.Init("testing0")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Write("k", "s")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Commit()
	if err != nil {
		t.Fatal(err)
	}

	BtreeStructureTest(t, db)

	val, err := db.Read("k")
	if err != nil {
		t.Fatal(err)
	}

	if val != "s" {
		t.Fatal("first insert read error")
	}

	err = db.Write("k", "m")
	if err != nil {
		t.Fatal(err)
	}
	err = db.Commit()
	if err != nil {
		t.Fatal(err)
	}

	BtreeStructureTest(t, db)

	val, err = db.Read("k")
	if err != nil {
		t.Fatal(err)
	}
	if val != "m" {
		t.Fatal("first update read error")
	}

	for i := 0; i < 30; i++ {
		key := strconv.Itoa(i)
		err = db.Write(key, key)
		if err != nil {
			t.Fatal("for loop writing error inserting ", i)
		}
	}

	err = db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	BtreeStructureTest(t, db)
	for i := 0; i < 140; i += 5 {
		key := strconv.Itoa(i)
		val := strconv.Itoa(i + 1)
		err = db.Write(key, val)
		if err != nil {
			t.Fatal("for loop writing error inserting ", i)
		}
	}

	err = db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	BtreeStructureTest(t, db)

	for i := 0; i < 140; i += 5 {
		key := strconv.Itoa(i)
		val, err := db.Read(key)
		if err != nil {
			t.Fatal("for loop error reading ", err, i)
		}

		if val != strconv.Itoa(i+1) {
			t.Fatal("for loop update and read error", key, val)
		}
	}
	err = db.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for i := 200; i <= 1000; i++ {
		key := strconv.Itoa(i)
		val := strconv.Itoa(i)
		err = db.Write(key, val)
		if err != nil {
			t.Fatal("for loop writing error inserting ", i)
		}
	}

	err = db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	BtreeStructureTest(t, db)

	for i := 200; i <= 1000; i++ {
		key := strconv.Itoa(i)
		val, err := db.Read(key)
		if err != nil {
			t.Fatal("for loop error reading ", err, i)
		}

		if val != strconv.Itoa(i) {
			t.Fatal("for loop update and read error", key, val)
		}
	}
	err = db.Clear()
	if err != nil {
		t.Fatal(err)
	}
}

func BtreeStructureTest(t *testing.T, db *DB) {
	root, err := db.GetRoot()
	if err != nil {
		t.Fatal(err)
	}
	kvpairs, err := Tranverse(db, root)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(kvpairs)-1; i++ {
		if kvpairs[i+1].Key <= kvpairs[i].Key {
			t.Fatal("btree structure error")

		}
	}
}
