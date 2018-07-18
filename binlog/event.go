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

	encode []byte
}

func (header *EveHeader) Decode(data []byte) error {
	if len(data) < EventHeaderSize {
		return errors.Errorf("header size too short %d, must 19", len(data))
	}

	header.encode = data[1:EventHeaderSize]

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


type GtidEvent struct {
	commitFlag bool
	sig        []byte
	gno        uint64

	LastCommitted uint64
	SeqNum        uint64

	header *EveHeader
	encode []byte
}

func (gtidEve *GtidEvent) Decode(data []byte) error {

	gtidEve.encode = data

	pos := 0
	gtidEve.commitFlag = data[pos] == 1
	pos += 1
	gtidEve.sig = data[pos : pos+16]
	pos += 16
	gtidEve.gno = binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	pos += 1

	gtidEve.LastCommitted = binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	gtidEve.SeqNum = binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8

	return nil
}

func (gtidEve *GtidEvent) Dump() string {
	return fmt.Sprintf("commited flag: %v, last commited: %d, seq num: %d",
		gtidEve.commitFlag,
		gtidEve.LastCommitted,
		gtidEve.SeqNum,
	)
}


type QueryEvent struct {
	schema string
	query string

	header *EveHeader
	encode []byte
}

func (queryEve *QueryEvent) Decode(data []byte) error {
	pos := 0
	pos += 4
	pos += 4
	schemaLen := data[pos]
	pos += 1
	pos += 2
	statusLen := binary.LittleEndian.Uint16(data[pos :pos + 2])
	pos += 2

	pos += int(statusLen)
	queryEve.schema = string(data[pos : pos + int(schemaLen)])
	pos += int(schemaLen)
	pos += 1

	//queryLen := queryEve.header.EveSize - EventHeaderSize - 13 - uint32(statusLen) - uint32(schemaLen)
	queryEve.query = string(data[pos : ])
	queryEve.encode = data

	return nil
}

func (queryEve *QueryEvent) Dump() string {
	return fmt.Sprintf("schema: %s, query: %s", queryEve.schema, queryEve.query)
}