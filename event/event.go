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

func GetEventType(eve Event) uint8 {
	switch e := eve.(type) {
	case *GtidEvent:
		return e.Header.EveType
	case *XidEvnet:
		return e.Header.EveType
	case *QueryEvent:
		return e.Header.EveType
	case *FormatDescEvent:
		return e.Header.EveType
	case *PreGtidLogEvent:
		return e.Header.EveType
	case *RotateEvent:
		return e.Header.EveType
	case *RowsEvent:
		return e.Header.EveType
	case *TableMapEvent:
		return e.Header.EveType
	case *StopEvent:
		return e.Header.EveType
	}
	return 0
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
	return fmt.Sprintf("GtidEvent last commited: %d, seq num: %d",
		gtidEve.LastCommitted,
		gtidEve.SeqNum,
	)
}

type XidEvnet struct {
	Header  *EveHeader
	Encoded []byte

	Xid uint64
}

func (xidEve *XidEvnet) Decode(data []byte) error {
	xidEve.Encoded = data
	xidEve.Xid = binary.LittleEndian.Uint64(data)
	return nil
}

func (xidEve *XidEvnet) Dump() string {
	return fmt.Sprintf("XidEvent xid: %d", xidEve.Xid)
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
	return fmt.Sprintf("QueryEvent query: %s", queryEve.Query)
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
	return fmt.Sprintf("TableMapEvent id:%d, schema: %s, table: %s", tbl.TblId, tbl.Schema, tbl.Table)
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
	return fmt.Sprintf("FormatDescEvent %d - %s", fmtEvent.BinlogVersion, fmtEvent.SvrVersion)
}

type RotateEvent struct {
	Header *EveHeader

	Pos        uint64
	NextBinlog string

	Encoded []byte
}

func (rotateEve *RotateEvent) Decode(data []byte) error {
	rotateEve.Encoded = data
	pos := 0
	rotateEve.Pos = binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	rotateEve.NextBinlog = string(data[pos:])
	return nil
}

func (rotateEve *RotateEvent) Dump() string {
	return fmt.Sprintf("RotateEvent next binlog: %s", rotateEve.NextBinlog)
}

type StopEvent struct {
	Header  *EveHeader
	Encoded []byte
}

func (stop *StopEvent) Decode(data []byte) error {
	stop.Encoded = data
	return nil
}

func (stop *StopEvent) Dump() string {
	return fmt.Sprintf("StopEvent")
}

type PreGtidLogEvent struct {
	Header *EveHeader

	Encoded []byte
}

func (preGtid *PreGtidLogEvent) Decode(data []byte) error {
	preGtid.Encoded = data
	pos := 0
	pos += 8
	return nil
}

func (preGtid *PreGtidLogEvent) Dump() string {
	return "PreviousGtidLogEvent"
}
