package database

import (
	lmdb "github.com/bmatsuo/lmdb-go/lmdb"
	"time"
)

// Actually constants
var (
	BLOCKS_BUCKET   = []byte("blocks")
	PROJECTS_BUCKET = []byte("projects")
	EMPTY_VALUE     = []byte("")
)

const (
	dbBlocks   = "blocks"
	dbProjects = "projects"
)

type Database struct {
	*lmdb.Env
	Projects lmdb.DBI
	Builds   lmdb.DBI
	Blocks   lmdb.DBI
	quit     chan struct{}
}

func (db *Database) Init(path string) (err error) {
	setup := []func() error{
		func() error { db.Env, err = lmdb.NewEnv(); return err },
		func() error { return db.SetMapSize(1 << 40) }, // 1TB map size
		func() error { return db.SetMaxDBs(1024) },
		func() error { return db.SetMaxReaders(1024) },
	}
	for _, setupFunc := range setup {
		err = setupFunc()
		if err != nil {
			return
		}
	}
	err = db.Env.Open(path, 0, lmdb.NoReadahead)
	if err != nil {
		db.Env.Close()
		return
	}
	err = db.Env.Update(func(tx *lmdb.Txn) (err error) {
		openDB(tx, &err, &db.Projects, "projects")
		openDB(tx, &err, &db.Builds, "builds")
		openDB(tx, &err, &db.Blocks, "blocks")
		return
	})
	if err != nil {
		return
	}

	// Maintainence Goroutine to resize the database as needed
	go func() {
		defer db.Env.Close()
		for {
			// TODO(james7132): Implement
			select {
			case <-time.After(100 * time.Millisecond):
				continue
			case <-db.quit:
				return
			}
		}
	}()
	return
}

func (db *Database) MakeTxn(tx *lmdb.Txn) Transaction {
	// Directly return a read only slice from shared memory when reading.
	//
	// Avoids a copy when reading from the database. However, will lead to a
	// panic if the retrieved slice is written to.
	tx.RawRead = true

	return Transaction{
		Database: db,
		Txn:      tx,
	}
}

func (db *Database) View(transaction func(Transaction) error) error {
	return db.Env.View(func(tx *lmdb.Txn) error {
		trans := db.MakeTxn(tx)
		return transaction(trans)
	})
}

func (db *Database) Update(transaction func(Transaction) error) error {
	return db.Env.Update(func(tx *lmdb.Txn) error {
		trans := db.MakeTxn(tx)
		return transaction(trans)
	})
}

func (db *Database) Close() {
	db.quit <- struct{}{}
}

func openDB(tx *lmdb.Txn, err *error, db *lmdb.DBI, name string) {
	if err != nil {
		return
	}
	*db, *err = tx.OpenDBI(name, lmdb.Create)
}

func concat(parts ...[]byte) []byte {
	capacity := 0
	for _, part := range parts {
		capacity += len(part)
	}
	out := make([]byte, capacity)
	offset := 0
	for _, part := range parts {
		copy(out[offset:], part)
		offset += len(part)
	}
	return out
}
