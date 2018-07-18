/**
 *  author: lim
 *  data  : 18-7-18 下午9:53
 */

package binlog

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/juju/errors"
)

type EveHeader struct {
	Ts      uint32
	EveType uint8
	SvrId   uint32
	EveSize uint32
	LogPos  uint32
	Flags   uint16
}

func (header *EveHeader) Decode(data []byte) error {
	if len(data) < EventHeaderSize {
		return errors.Errorf("header size too short %d, must 19", len(data))
	}

	pos := 1

	header.Ts = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	header.EveType = data[pos]
	pos++

	header.SvrId = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	header.EveSize = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	header.LogPos = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	header.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if header.EveSize < uint32(EventHeaderSize) {
		return errors.Errorf("invalid event size %d, must >= 19", header.EveSize)
	}

	return nil
}

func (header *EveHeader) Dump() string {
	return fmt.Sprintf("type: %d, date: %s, pos: %d, eveSize: %d",
		header.EveType,
		time.Unix(int64(header.Ts), 0).Format(TimeFormat),
		header.LogPos,
		header.EveSize,
	)
}

type Event interface {
	Decode(data []byte) error
	Dump() string
}
