package database

import (
	"encoding/binary"
	"fmt"
)

var (
	ErrBufferTooSmall = fmt.Errorf("Input buffer too small to decode ID.")

	encoding = binary.BigEndian
)

type ProjectId uint64

func (p ProjectId) Encode() []byte {
	bytes := make([]byte, 8)
	encoding.PutUint64(bytes, uint64(p))
	return bytes
}

func (p *ProjectId) Decode(buf []byte) error {
	if len(buf) < 8 {
		return ErrBufferTooSmall
	}
	*p = ProjectId(encoding.Uint64(buf))
	return nil
}

type BranchId struct {
	Project ProjectId
	Branch  uint16
}

func (b BranchId) Encode() []byte {
	bytes := make([]byte, 10)
	encoding.PutUint64(bytes[0:], uint64(b.Project))
	encoding.PutUint16(bytes[8:], b.Branch)
	return bytes
}

func (p *BranchId) Decode(buf []byte) error {
	if len(buf) < 8 {
		return ErrBufferTooSmall
	}
	p.Project.Decode(buf)
	p.Branch = encoding.Uint16(buf[8:])
	return nil
}

type CommitId [4]byte

func (c *CommitId) Encode() []byte {
	return c[:]
}

func (c *CommitId) Decode(buf []byte) error {
	if len(buf) < 4 {
		return ErrBufferTooSmall
	}
	copy(buf, c[:])
	return nil
}

type Platform uint16

func (p Platform) Encode() []byte {
	bytes := make([]byte, 2)
	encoding.PutUint16(bytes, uint16(p))
	return bytes
}

func (p *Platform) Decode(buf []byte) error {
	if len(buf) < 2 {
		return ErrBufferTooSmall
	}
	*p = Platform(encoding.Uint16(buf[8:]))
	return nil
}

type BuildId struct {
	BranchId
	Commit CommitId
	Platform
}

func (b *BuildId) Encode() []byte {
	bytes := make([]byte, 16)
	encoding.PutUint64(bytes[0:], uint64(b.Project))
	encoding.PutUint16(bytes[8:], b.Branch)
	copy(bytes[10:], b.Commit[:])
	encoding.PutUint16(bytes[14:], uint16(b.Platform))
	return bytes
}

func (b *BuildId) Decode(buf []byte) error {
	if len(buf) < 16 {
		return ErrBufferTooSmall
	}
	err := b.BranchId.Decode(buf[0:])
	if err != nil {
		return err
	}
	err = b.Commit.Decode(buf[10:])
	if err != nil {
		return err
	}
	err = b.Platform.Decode(buf[14:])
	if err != nil {
		return err
	}
	return nil
}
