/**
 *  author: lim
 *  data  : 18-7-19 下午10:22
 */

package binlog

import (
	"fmt"
	"encoding/binary"

	"github.com/lemonwx/xsql/mysql"
	"github.com/lemonwx/log"
	"time"
	"strings"
)

type RowsEvent struct {
	header *EveHeader
	tblId uint64
	flags uint16

	extraDataLen uint16
	extraData []byte

	fieldSize uint64
	bitmap []byte

	encode []byte

	rows []map[int]interface{}

	dumper *Dumper
}

func (re *RowsEvent) Decode(data []byte) error {
	re.encode = data

	re.tblId = readTblId(data)
	pos := 6

	re.flags = binary.LittleEndian.Uint16(data[pos: pos + 2])
	pos += 2
	re.extraDataLen = binary.LittleEndian.Uint16(data[pos: pos + 2])
	pos += 2

	re.extraData = data[pos : pos + int(re.extraDataLen / 8)]
	pos += int(re.extraDataLen / 8)

	var size int
	re.fieldSize, _, size = mysql.LengthEncodedInt(data[pos:])
	pos += size

	size = int((re.fieldSize + 7) / 8)
	re.bitmap = data[pos: pos + size]
	pos += size

	re.ReadRows(data[pos:])

	return nil
}

func (re *RowsEvent) Dump() string {
	eveType := ""
	switch re.header.EveType {
	case WRITE_ROWS_EVENT_V2:
		eveType = "WRITE_ROWS_EVENT_V2"
	}

	rows := []string{}
	for _, row := range re.rows {
		for i := 0; i < len(row); i += 1 {
			rows = append(rows,  fmt.Sprintf("@%d=%v", i, row[i]))
		}
	}
	ret := fmt.Sprintf("%s table: %d, field_size: %d, rows: %v",
		eveType, re.tblId, re.fieldSize,
		strings.Join(rows, ", "),
	)

	return ret
}

func readTblId(data []byte) uint64 {
	tblEncode := make([]byte, 8)
	copy(tblEncode, data[:6])
	tblId := binary.LittleEndian.Uint64(tblEncode)
	return tblId
}

func (re *RowsEvent) ReadRows(data []byte) {
	fieldTypes := re.dumper.tables[re.tblId].colTypes
	re.rows = make([]map[int]interface{}, 0)
	pos := 0

	for pos < len(data) {
		row := make(map[int]interface{})
		nullMaskSize := int((BitCount(re.bitmap) + 7) >> 3)
		nullMask := data[pos: pos+nullMaskSize]
		pos += nullMaskSize

		nullbitIndex := 0
		for idx := 0; idx < int(re.fieldSize); idx += 1 {
			if BitGet(re.bitmap, uint8(idx)) {
				row[int(idx)] = nil
			}

			if (uint32(nullMask[nullbitIndex/8])>>uint32(nullbitIndex%8)) & 0x01 > 0 {
				row[int(idx)] = nil
			} else {
				fieldType := fieldTypes[idx]
				//log.Debug(fieldType)
				switch  fieldType {
				case mysql.MYSQL_TYPE_LONGLONG:
					row[idx] = binary.LittleEndian.Uint64(data[pos: pos + 8])
					pos += 8
				case mysql.MYSQL_TYPE_LONG:
					row[idx] = binary.LittleEndian.Uint32(data[pos: pos+4])
					pos += 4
				case mysql.MYSQL_TYPE_STRING:
					bin, _, size, _ := mysql.LengthEnodedString(data[pos:])
					row[idx] = string(bin)
					pos += size
				case mysql.MYSQL_TYPE_TIMESTAMP2:
					dateBinary := binary.BigEndian.Uint32(data[pos: pos + 4])
					date := time.Unix(int64(dateBinary), 0).Format("2006-01-02 15:04:05")
					pos = pos + 4
					misSec := BigEndianUint24(data[pos:pos+3])
					pos += 3
					row[idx] = fmt.Sprintf("%s.%d", date, misSec)
				case mysql.MYSQL_TYPE_DATE:
					dateBin := LittleEndianUint24(data[pos: pos + 3])
					if dateBin == 0 {
						row[idx] = nil
					} else {
						row[idx] = fmt.Sprintf("%04d-%02d-%02d", dateBin/(16*32), dateBin/32%16, dateBin%32)
					}
					pos += 3
				default:
					log.Debug(fieldType)
				}
			}
			nullbitIndex += 1
		}
		re.rows = append(re.rows, row)
	}

	log.Debug(re.rows)
}

func BigEndianUint24(data []uint8) uint64 {
	a, b, c := uint64(data[0]), uint64(data[1]), uint64(data[2])
	res := (a << 16) | (b << 8) | c
	return res
}

func LittleEndianUint24(data []uint8) uint64 {
	a, b, c := uint64(data[0]), uint64(data[1]), uint64(data[2])
	res := a | (b << 8) | (c << 16)
	return res
}
