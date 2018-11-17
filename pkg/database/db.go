package database

import (
	"github.com/HouraiTeahouse/Tapioca/pkg/blocks"
	proto "github.com/HouraiTeahouse/Tapioca/pkg/proto"
	protobuf "github.com/golang/protobuf/proto"
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
	Env      lmdb.Env
	projects lmdb.DBI
	builds   lmdb.DBI
	blocks   lmdb.DBI
	quit     chan<- struct{}
}

func (db *Database) Init(path string) (err error) {
	db.Env, err = lmdb.NewEnv()
	err := db.Env.SetMapSize(25 * (1 << 30)) // 25GB iniitial map size
	err := db.Env.SetMaxDbs(1024)
	err := db.Env.SetMaxReaders(1024)
	db.Env, err = lmdb.Open(path, 0600, nil)
	if err != nil {
		return
	}
	err = d.Env.Update(func(tx *lmdb.Txn) (err error) {
		openDB(tx, &err, &db.projects, "projects")
		openDB(tx, &err, &db.builds, "builds")
		openDB(tx, &err, &db.blocks, "blocks")
	})

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
	} ()
	return
}

func (d *Database) GetManifest(id BuildId) (manifest *ManifestProto, err error) {
	err = d.Env.View(func(tx *lmdb.Txn) error {
		manifest, err := db.getManifest(tx, id)
		return err
	})
	return
}

func (d *Database) PutManifest(id Build, manifest *ManifestProto) error {
	return d.Env.Update(func(tx *lmdb.Txn) error {
		manifest, err := db.getManifest(tx, key)
		if err != nil {
			return err
		}
		return deleteBlocks(tx, key, manifest)
	})
}

func (db *Database) DeleteManifest(id BuildId) error {
	return db.Env.View(func(tx *lmdb.Txn) error {
		manifest, err := db.getManifest(tx, key)
		if err != nil {
			return err
		}
		return deleteBlocks(tx, key, manifest)
	})
}

func (db *Database) Close() {
	db.quit <- struct{}
}

func (db *Database) getManifest(tx *lmdb.Txn, id BuildId) (manifest *ManifestProto, err error) {
	key, err := id.Encode()
	if err != nil {
		return err
	}
	val, err := tx.Get(db.builds, key)
	if err != nil {
		return err
	}
	manifest = new(ManifestProto)
	return manifest, protobuf.Unmarshal(val, manifest)
}

func (db *Database) isBlockLive(tx *lmdb.Txn, key []byte, manifest *ManifestProto) (err error) {
	fullKey := make([]byte, block.HashSize + len(key))
	copy(fullKey[block.HashSize:], key)
	for _, block := range manifest.GetBlocks() {
		// TODO(james7132): Assert the hash is the right size.
		copy(fullKey[:block.HashSize], block.Hash)
		_, err = tx.PutReserve(db.blocks, fullKey, 0
		if err != nil {
			return
		}
	}
	return nil
}

func (db *Database) fillBlocks(tx *lmdb.Txn, key []byte, manifest *ManifestProto) (err error) {
	fullKey := make([]byte, block.HashSize + len(key))
	copy(fullKey[block.HashSize:], key)
	for _, block := range manifest.GetBlocks() {
		// TODO(james7132): Assert the hash is the right size.
		copy(fullKey[:block.HashSize], block.Hash)
		_, err = tx.PutReserve(db.blocks, fullKey, 0
		if err != nil {
			return
		}
	}
	return nil
}

// Deletes blocks related to a given build
func (db *Database) deleteBlocks(tx *lmdb.Txn, key []byte, manifest *ManifestProto) (err error) {
	fullKey := make([]byte, block.HashSize + len(key))
	copy(fullKey[block.HashSize:], key)
	for _, block := range manifest.GetBlocks() {
		// TODO(james7132): Assert the hash is the right size.
		copy(fullKey[:block.HashSize], block.Hash)
		 err = tx.Del(db.blocks, fullKey, nil)
		if err != nil {
			return
		}
	}
	return nil
}

func openDB(tx *lmdb.Txn, err *error, db *lmdb.DBI, name string) {
	if err != nil {
		return
	}
	*db, *err = tx.OpenDBI(name, lmdb.Create)
}

func concat(parts ...[]byte) (out []byte) {
	capacity := 0
	for _, part := range parts {
		capacity += len(part)
	}
	out = make([]byte, capacity)
	offset := 0
	for _, part := range parts {
		copy(out[offset:], part)
		offset += len(part)
	}
}

