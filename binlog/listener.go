/**
 *  author: lim
 *  data  : 18-7-17 下午10:39
 */

package binlog

import (
	"github.com/lemonwx/xsql/node"
	"github.com/lemonwx/xsql/mysql"
	"fmt"
)

const (
	DEFAULT_SCHEMA = "information_schema"
)

type BinlogDumper struct {
	*node.Node
}

func NewBinlogDumper (host string, port int, user, password string) *BinlogDumper{
	node := node.NewNode(host, port, user, password, DEFAULT_SCHEMA, 0)
	err := node.Connect()
	if err != nil {
		fmt.Println(err)
	}
	return &BinlogDumper{node}
}

func (dumper *BinlogDumper) Start() {
	ret, err := dumper.Execute(mysql.COM_QUERY, []byte("show master status"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(ret.FieldNames)
}

