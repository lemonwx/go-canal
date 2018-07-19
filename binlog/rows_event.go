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
	return fmt.Sprintf("table: %d, field_size: %d", re.tblId, re.fieldSize)
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
	pos := uint64(0)

	for pos < uint64(len(data)) {
		row := make(map[int]interface{})
		nullMaskSize := ( re.fieldSize + 7 + 2 ) >> 3
		nullMask := data[pos: pos+nullMaskSize]
		pos += nullMaskSize

		nullbitIndex := 0
		for idx := uint64(0); idx < re.fieldSize; idx += 1 {
			if isBitSet(re.bitmap, idx) {
				row[int(idx)] = nil
			}

			if (uint32(nullMask[nullbitIndex/8])>>uint32(nullbitIndex%8))&0x01 > 0 {
				row[int(idx)] = nil
			} else {
				fieldType := fieldTypes[idx]
				switch  fieldType {
				case mysql.MYSQL_TYPE_LONGLONG:
					row[int(idx)] = data[pos: pos+8]
					pos += 8
				case mysql.MYSQL_TYPE_LONG:
					row[int(idx)] = data[pos: pos+4]
					pos += 4
				case mysql.MYSQL_TYPE_STRING:
					var size int
					row[int(idx)], _, size, _ = mysql.LengthEnodedString(data[pos:])
					pos += uint64(size)
				case mysql.MYSQL_TYPE_TIMESTAMP2:
					log.Debug(data[pos:])
					row[int(idx)] = data[pos: pos + 7]
					pos += 7
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

func isBitSet(bitmap []byte, i uint64) bool {
	return bitmap[i>>3]&(1<<(uint(i)&7)) > 0
}