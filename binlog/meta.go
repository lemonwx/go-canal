/**
 *  author: lim
 *  data  : 18-7-23 下午10:36
 */

package binlog

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/lemonwx/log"
	"github.com/lemonwx/xsql/mysql"
	"strings"
)

type Field struct {
	fieldName        string
	fieldType        string
	encodedfieldType uint8
	unsigned         bool
	encoded          []uint8
}

func (f Field) String() string {
	return fmt.Sprintf("%s", f.fieldName)
}

func (f *Field) Decode() {
	pos := 0
	fNameBin, _, size, err := mysql.LengthEnodedString(f.encoded[pos:])
	if err != nil {
		log.Error(err)
	}
	pos += size
	f.fieldName = string(fNameBin)
	fTypeBIn, _, _, err := mysql.LengthEnodedString(f.encoded[pos:])
	f.fieldType = string(fTypeBIn)
	f.unsigned = strings.Contains(f.fieldType, "unsigned")
}

type Table struct {
	schema   string   `json:"schema"`
	table    string   `json:"table"`
	tableId  uint64   `json:"tableId"`
	fields   []*Field `json:"fields"`
	complete bool
}

func (table *Table) setupEncodedFieldType(types []byte) error {
	if len(table.fields) != len(types) {
		return errors.New("len(table.fields) != len(types)")
	}

	for idx, field := range table.fields {
		field.encodedfieldType = types[idx]
	}
	return nil
}

type InformationSchema struct {
	tbs    map[string]*Table
	dumper *Listener
}

func NewInformationSchema(dumper *Listener) *InformationSchema {
	return &InformationSchema{
		tbs:    make(map[string]*Table),
		dumper: dumper,
	}
}

func (meta *InformationSchema) GetTable(tbName string) (*Table, bool) {
	table, ok := meta.tbs[tbName]
	return table, ok
}

func (meta *InformationSchema) parseFromLocal() {

}

func (meta *InformationSchema) parseMeta(schema, table string) error {
	sql := "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE from information_schema.COLUMNS " +
		"where table_schema not in('mysql', 'information_schema', 'performance_schema', 'sys');"

	if len(schema) != 0 {
		sql += fmt.Sprintf(" and TABLE_SCHEMA = %s", schema)
	}
	if len(table) != 0 {
		sql += fmt.Sprintf(" and TABLE_NAME = %s", table)
	}
	ret, err := meta.dumper.Execute(mysql.COM_QUERY, []byte(sql))
	if err != nil {
		return errors.Trace(err)
	}

	for _, row := range ret.RowDatas {
		pos := 0
		schema, _, size, _ := mysql.LengthEnodedString(row[pos:])
		pos += size
		table, _, size, _ := mysql.LengthEnodedString(row[pos:])
		pos += size

		field := &Field{encoded: row[pos:]}
		field.Decode()
		fullTbName := fmt.Sprintf("%s.%s", schema, table)
		if tb, ok := meta.tbs[fullTbName]; ok {
			tb.fields = append(tb.fields, field)
		} else {
			tb := &Table{
				schema: string(schema),
				table:  string(table),
				fields: []*Field{field},
			}

			meta.tbs[fullTbName] = tb
		}
	}

	for tbname, table := range meta.tbs {
		log.Debugf("%v: %v", tbname, table)
	}

	//log.Debug(meta)
	return nil
}
