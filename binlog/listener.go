/**
 *  author: lim
 *  data  : 18-7-17 下午10:39
 */

package binlog

import (
	"encoding/binary"

	"github.com/juju/errors"
	"github.com/lemonwx/log"
	"github.com/lemonwx/xsql/mysql"
	"github.com/lemonwx/xsql/node"
)

const (
	DEFAULT_SCHEMA = "information_schema"
)

type Dumper struct {
	*node.Node
	meta   *InformationSchema
	tables map[uint64]*TableMapEvent
}

func NewBinlogDumper(host string, port int, user, password string) *Dumper {
	node := node.NewNode(host, port, user, password, DEFAULT_SCHEMA, 0)
	return &Dumper{Node: node, tables: map[uint64]*TableMapEvent{}}
}

func (dumper *Dumper) getFileAndPos() (string, uint32, error) {
	return "mysql-bin.000001", 4, nil
}

func (dumper *Dumper) writeDumpCmd() error {

	logName, logPos, err := dumper.getFileAndPos()
	if err != nil {
		return errors.Trace(err)
	}

	data := make([]byte, 4+1+4+2+4+len(logName))

	pos := 4
	data[pos] = mysql.COM_BINLOG_DUMP
	pos++

	binary.LittleEndian.PutUint32(data[pos:], logPos)
	pos += 4

	binary.LittleEndian.PutUint16(data[pos:], 0)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], 123456789)
	pos += 4

	copy(data[pos:], logName)

	dumper.SetPktSeq(0)
	dumper.WritePacket(data)

	return nil
}

func (dumper *Dumper) Init() error {

	err := dumper.Connect()
	if err != nil {
		return errors.Trace(err)
	}

	_, err = dumper.Execute(mysql.COM_QUERY, []byte("set @master_binlog_checksum= @@global.binlog_checksum"))
	if err != nil {
		return errors.Trace(err)
	}

	_, err = dumper.Execute(mysql.COM_QUERY, []byte("show master status"))
	if err != nil {
		return errors.Trace(err)
	}

	// 确定 dump 开始的文件和位置后, 全量同步一次 元数据
	// 若在 show master status 之前元数据有变化, 则全量可以同步到
	// 若在 show master statsu 之后元数据有变化, 则可以通过binlog 增量同步到
	meta := NewInformationSchema(dumper)
	meta.parseMeta("", "")
	dumper.meta = meta

	if err = dumper.writeDumpCmd(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (dumper *Dumper) Start() error {

	for {
		pkt, err := dumper.ReadPacket()
		if err != nil {
			return errors.Trace(err)
			break
		}

		switch pkt[0] {
		case mysql.ERR_HEADER:
			log.Debug(pkt[1:])
		case mysql.OK_HEADER:
			header := &EveHeader{}
			header.Decode(pkt)
			//log.Debug(header.Dump(), pkt)

			dumper.parseEvent(header, pkt[EventHeaderSize:])
		}
	}
	return nil
}

func (dumper *Dumper) parseEvent(header *EveHeader, data []byte) (Event, error) {
	data = data[:len(data)-4]
	var eve Event
	switch header.EveType {
	case GTID_LOG_EVENT:
		eve = &GtidEvent{header: header}
	case QUERY_EVENT:
		eve = &QueryEvent{header: header}
	case TABLE_MAP_EVENT:
		eve = &TableMapEvent{header: header}
	case WRITE_ROWS_EVENT_V2, DELETE_ROWS_EVENT_V2:
		eve = &RowsEvent{header: header, dumper: dumper}
	case XID_EVENT:
		log.Debug("xid event", data)
	default:
		log.Debug(header.EveType)
	}

	if eve != nil {
		eve.Decode(data)
		log.Debug(eve.Dump())
	}

	if tbl, ok := eve.(*TableMapEvent); ok {
		dumper.tables[tbl.tblId] = tbl
		dumper.syncBinlogAndIfSchema(tbl)
		//log.Debug(dumper.meta.tbs[tbl.fullName])
	}

	if re, ok := eve.(*RowsEvent); ok {
		table := dumper.meta.tbs[re.table.fullName]
		if rbSqls, err := re.RollBack(table.fields); err != nil {
			log.Errorf("re: %v rollback failed: %v", re, err)
		} else {
			log.Debug("ROLLBACK: ")
			for _, sql := range rbSqls {
				log.Debug(sql)
			}
		}
	}

	if _, ok := eve.(*QueryEvent); ok {
		// create / drop / alter should sync with meta
	}

	return nil, nil
}

func (dumper *Dumper) syncBinlogAndIfSchema(tbl *TableMapEvent) {

}
