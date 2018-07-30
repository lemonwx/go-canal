/**
 *  author: lim
 *  data  : 18-7-18 下午9:53
 */

package event

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/lemonwx/xsql/mysql"
)

type EveHeader struct {
	Ts      uint32 `json:"event_time"`
	EveType uint8  `json:"event_type"`
	SvrId   uint32 `json:"server_id"`
	EveSize uint32 `json:"event_size"`
	LogPos  uint32 `json:"log_pos"`
	Flags   uint16 `json:"flag"`

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
	Header     *EveHeader
	commitFlag bool
	sig        []byte
	gno        uint64

	LastCommitted uint64
	SeqNum        uint64

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
	Header *EveHeader

	Schema string
	Query  string

	encode []byte
}

func (queryEve *QueryEvent) Decode(data []byte) error {
	pos := 0
	pos += 4
	pos += 4
	schemaLen := data[pos]
	pos += 1
	pos += 2
	statusLen := binary.LittleEndian.Uint16(data[pos : pos+2])
	pos += 2

	pos += int(statusLen)
	queryEve.Schema = string(data[pos : pos+int(schemaLen)])
	pos += int(schemaLen)
	pos += 1

	//queryLen := queryEve.header.EveSize - EventHeaderSize - 13 - uint32(statusLen) - uint32(schemaLen)
	queryEve.Query = string(data[pos:])
	queryEve.encode = data

	return nil
}

func (queryEve *QueryEvent) Dump() string {
	return fmt.Sprintf("Schema: %s, query: %s", queryEve.Schema, queryEve.Query)
}

type TableMapEvent struct {
	Header *EveHeader

	TblId     uint64
	Schema    []byte
	Table     []byte
	FullName  string
	FieldSize uint64
	ColTypes  []byte
}

func (tbl *TableMapEvent) Decode(data []byte) error {
	tbl.TblId = readTblId(data)
	pos := 6

	_ = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	schemaLength := data[pos]
	pos++

	tbl.Schema = data[pos : pos+int(schemaLength)]
	pos += int(schemaLength)

	//skip 0x00
	pos++

	tableLength := data[pos]
	pos++

	tbl.Table = data[pos : pos+int(tableLength)]
	pos += int(tableLength)

	tbl.FullName = fmt.Sprintf("%s.%s", tbl.Schema, tbl.Table)

	//skip 0x00
	pos++

	var n int
	tbl.FieldSize, _, n = mysql.LengthEncodedInt(data[pos:])
	pos += n

	tbl.ColTypes = data[pos : pos+int(tbl.FieldSize)]
	pos += int(tbl.FieldSize)

	mysql.LengthEnodedString(data[pos:])

	return nil
}

func (tbl *TableMapEvent) Dump() string {
	return fmt.Sprintf("Table Id: %d, colType: %v", tbl.TblId, tbl.ColTypes)
}

type FormatDescEvent struct {
	Header *EveHeader

	BinlogVersion uint16
	SvrVersion    []byte
	CreateTime    uint32

	Encoded []byte
}

func (fmtEvent *FormatDescEvent) Decode(data []byte) error {
	fmtEvent.Encoded = data
	pos := 0
	fmtEvent.BinlogVersion = binary.LittleEndian.Uint16(data[pos : pos+2])
	pos += 2
	fmtEvent.SvrVersion = make([]byte, 50)
	copy(fmtEvent.SvrVersion, data[pos:pos+50])
	pos += 50
	fmtEvent.CreateTime = binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4
	//headerSize := data[pos]
	pos += 1
	return nil
}

func (fmtEvent *FormatDescEvent) Dump() string {
	return fmt.Sprintf("%d - %s", fmtEvent.BinlogVersion, fmtEvent.SvrVersion)
}
