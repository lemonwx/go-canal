/**
 *  author: lim
 *  data  : 18-7-23 下午10:36
 */

package binlog

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/lemonwx/xsql/mysql"
	"github.com/lemonwx/log"
)

type Field struct {
	fieldName string
	fieldType int
	unsigned bool
	encoded []uint8
}

type Table struct {
	schema string `json:"schema"`
	table string	`json:"table"`
	tableId uint64	`json:"tableId"`
	fields []*Field	`json:"fields"`
}

type InformationSchema struct {
	tbs map[string]*Table
	dumper *Dumper
}

func NewInformationSchema(dumper *Dumper) *InformationSchema{
	return &InformationSchema{
		tbs : make(map[string]*Table),
		dumper: dumper,
	}
}

func (meta *InformationSchema) parseFromLocal() {

}

func (meta *InformationSchema) parseMeta() error {
	sql := "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE from information_schema.COLUMNS " +
		"where table_schema not in('mysql', 'information_schema', 'performance_schema', 'sys');"
	ret, err :=meta.dumper.Execute(mysql.COM_QUERY, []byte(sql))
	if err != nil {
		return errors.Trace(err)
	}

	log.Debug(ret.FieldNames)

	for _, row := range ret.RowDatas {
		pos := 0
		schema, _, size, _ := mysql.LengthEnodedString(row[pos:])
		pos += size
		table, _, size, _ := mysql.LengthEnodedString(row[pos:])
		pos += size

		field := &Field{encoded: row}
		fullTbName := fmt.Sprintf("%s.%s", schema, table)
		if tb, ok := meta.tbs[fullTbName]; ok {
			tb.fields = append(tb.fields, field)
		} else {
			tb := &Table{
				schema: string(schema),
				table: string(schema),
				fields:[]*Field{field},
			}

			meta.tbs[fullTbName] = tb
		}
	}

	log.Debug(meta)
	return nil
}

