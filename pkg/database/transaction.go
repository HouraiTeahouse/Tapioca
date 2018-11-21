package database

import (
	"bytes"
	"github.com/HouraiTeahouse/Tapioca/pkg/blocks"
	proto "github.com/HouraiTeahouse/Tapioca/pkg/proto"
	lmdb "github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/bmatsuo/lmdb-go/lmdbscan"
	pb "github.com/golang/protobuf/proto"
)

type Encodable interface {
	Encode() []byte
}

type Transaction struct {
	*Database
	*lmdb.Txn
}

func (tx *Transaction) Get(dbi lmdb.DBI, id Encodable) ([]byte, error) {
	return tx.Txn.Get(dbi, id.Encode())
}

func (tx *Transaction) GetProto(dbi lmdb.DBI, id Encodable, msg pb.Message) error {
	val, err := tx.Get(dbi, id)
	if err != nil {
		return err
	}
	return pb.Unmarshal(val, msg)
}

func (tx *Transaction) PutProto(dbi lmdb.DBI, id Encodable, msg pb.Message) error {
	// Reserve enough space to store the protobuffer
	buf, err := tx.Txn.PutReserve(dbi, id.Encode(), pb.Size(msg), 0)
	if err != nil {
		return err
	}

	// Directly marshal it into the provided buffer. Avoids a potentially
	// expensive copy when writing the encoded buffer into the database.
	//
	// TODO(james7132): Pool these buffers to lower the number of allocations
	buffer := pb.NewBuffer(buf)
	err = buffer.Marshal(msg)
	if err != nil {
		return err
	}
	return nil
}

func (tx *Transaction) Delete(dbi lmdb.DBI, id Encodable) error {
	return tx.Txn.Del(dbi, id.Encode(), nil)
}

func (tx *Transaction) GetBuild(id BuildId) (*proto.ManifestProto, error) {
	manifest := new(proto.ManifestProto)
	return manifest, tx.GetProto(tx.Builds, &id, manifest)
}

func (tx *Transaction) PutBuild(id BuildId, manifest *proto.ManifestProto) error {
	return tx.PutProto(tx.Builds, &id, manifest)
}

func (tx *Transaction) DeleteBuild(id BuildId) error {
	return tx.Delete(tx.Builds, &id)
}

func iterBlocks(manifest *proto.ManifestProto, key []byte, action func([]byte) error) error {
	fullKey := make([]byte, blocks.HashSize+len(key))
	upper := fullKey[:blocks.HashSize]
	lower := fullKey[blocks.HashSize:]
	copy(lower, key)
	for _, block := range manifest.GetBlocks() {
		// TODO(james7132): Assert the hash is the right size.
		copy(upper, block.Hash)
		err := action(fullKey)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tx *Transaction) isBlockLive(key []byte, manifest *proto.ManifestProto) bool {
	scanner := lmdbscan.New(tx.Txn, tx.Blocks)
	defer scanner.Close()

	scanner.Set(key, nil, lmdb.SetRange)
	for scanner.Scan() {
		return bytes.HasPrefix(scanner.Key(), key)
	}
	return false
}

func (tx *Transaction) fillBlocks(key []byte, manifest *proto.ManifestProto) error {
	return iterBlocks(manifest, key, func(key []byte) error {
		_, err := tx.PutReserve(tx.Blocks, key, 0, 0)
		return err
	})
}

// Deletes blocks related to a given build
func (tx *Transaction) deleteBlocks(key []byte, manifest *proto.ManifestProto) error {
	return iterBlocks(manifest, key, func(key []byte) error {
		return tx.Del(tx.Blocks, key, nil)
	})
}
