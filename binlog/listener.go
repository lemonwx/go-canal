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
	"github.com/lemonwx/go-canal/event"
)

const (
	DEFAULT_SCHEMA = "information_schema"
)

type Dumper struct {
	*node.Node
	meta   *InformationSchema
	tables map[uint64]*event.TableMapEvent
	curTblEve *event.TableMapEvent
}

func NewBinlogDumper(host string, port int, user, password string) *Dumper {
	node := node.NewNode(host, port, user, password, DEFAULT_SCHEMA, 0)
	return &Dumper{Node: node, tables: map[uint64]*event.TableMapEvent{}}
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
			header := &event.EveHeader{}
			header.Decode(pkt)
			//log.Debug(header.Dump(), pkt)

			dumper.parseEvent(header, pkt[event.EventHeaderSize:])
		}
	}
	return nil
}

func (dumper *Dumper) parseEvent(header *event.EveHeader, data []byte) (event.Event, error) {
	data = data[:len(data)-4]
	var eve event.Event
	switch header.EveType {
	case event.GTID_LOG_EVENT:
		eve = &event.GtidEvent{Header: header}
	case event.QUERY_EVENT:
		eve = &event.QueryEvent{Header: header}
	case event.TABLE_MAP_EVENT:
		eve = &event.TableMapEvent{Header: header}
	case event.WRITE_ROWS_EVENT_V2, event.DELETE_ROWS_EVENT_V2:
		log.Debug(dumper.curTblEve)
		eve = &event.RowsEvent{Header: header, Table: dumper.curTblEve}
	case event.XID_EVENT:
		log.Debug("xid event", data)
	default:
		log.Debug(header.EveType)
	}

	if eve != nil {
		eve.Decode(data)
		log.Debug(eve.Dump())
	}

	if tbl, ok := eve.(*event.TableMapEvent); ok {
		dumper.tables[tbl.TblId] = tbl
		dumper.syncBinlogAndIfSchema(tbl)
		dumper.curTblEve = tbl
	}

	if re, ok := eve.(*event.RowsEvent); ok {
		table := dumper.meta.tbs[re.Table.FullName]
		fieldNames := make([]string, 0, len(table.fields))

		for _, field := range table.fields {
			fieldNames = append(fieldNames, field.fieldName)
		}

		if rbSqls, err := re.RollBack(fieldNames); err != nil {
			log.Errorf("re: %v rollback failed: %v", re, err)
		} else {
			log.Debug("ROLLBACK: ")
			for _, sql := range rbSqls {
				log.Debug(sql)
			}
		}
	}

	if _, ok := eve.(*event.QueryEvent); ok {
		// create / drop / alter should sync with meta
	}

	return nil, nil
}

func (dumper *Dumper) syncBinlogAndIfSchema(tbl *event.TableMapEvent) {

}
