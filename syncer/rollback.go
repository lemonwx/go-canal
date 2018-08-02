/**
 *  author: lim
 *  data  : 18-8-2 下午9:24
 */

package syncer

import "fmt"

type Field struct {
	Name string
	Val  string
}

func (field *Field) String() string {
	return fmt.Sprintf("%s=%s", field.Name, field.Val)
}

type RollbackArgs struct {
	Schema string
	Table  string
	Fields []*Field
}

func (arg *RollbackArgs) String() string {
	return fmt.Sprintf("schema: %s table: %s %v", arg.Schema, arg.Table, arg.Fields)
}
