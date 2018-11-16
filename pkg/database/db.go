package database

import (
	"github.com/boltdb/bolt"
	protobuf "github.com/golang/protobuf/proto"
	proto "github.com/HouraiTeahouse/Tapioca/pkg/proto"
)

// Actually constants
var (
  BLOCKS_BUCKET = []byte("manifests")
  MANIFEST_BUCKET = []byte("manifests")
)

type Database struct {
  Db bolt.DB
}

func (d *Database) Init(path string) (err error) {
  d.Db, err = bolt.Open(path, 0600, nil)
  if err != nil {
    return
  }
  err = d.Db.Update(func (tx *bolt.Tx) (err error) {
    createBucket(&err, BLOCKS_BUCKET)
    createBucket(&err, MANIFEST_BUCKET)
  })
  return
}

func (d *Database) GetManifest(key []byte) (manifest *ManifestProto, err, error) {
  err = d.Db.View(func (tx *bolt.Tx) error {
    b, err := tx.Bucket(MANIFEST_BUCKET)
    if err != nil {
      return err
    }
    manifest, err = getManifest(tx, key)
    return err
  })
  return
}

func (d *Database) DeleteManifest(key []byte) error {
  return d.Db.View(func (tx *bolt.Tx) error {
    manifest, err := getManifest(tx, key)
    return nil
  })
}

func getManifest(tx *bolt.Tx, key []byte) (manifest *ManifestProto, err error) {
  var val []byte
  val, err = tx.Get(key)
  if err != nil {
    return
  }
  manifest = new(ManifestProto)
  err = protobuf.Unmarshal(val, manifest)
  return
}

func createBucket(err *error, tx *bolt.Tx, root) {
  if err != nil {
    return
  }
  _, *err = tx.CreateBucket(root)
}
