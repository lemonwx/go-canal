/**
 *  author: lim
 *  data  : 18-7-17 下午10:39
 */

package binlog

import (
	"encoding/binary"

	"github.com/juju/errors"
	"github.com/lemonwx/go-canal/event"
	"github.com/lemonwx/log"
	"github.com/lemonwx/xsql/mysql"
	"github.com/lemonwx/xsql/node"
)

const (
	DEFAULT_SCHEMA = "information_schema"
)

type Listener struct {
	*node.Node
	meta      *InformationSchema
	tables    map[uint64]*event.TableMapEvent
	curTblEve *event.TableMapEvent
}

func NewBinlogListener(host string, port int, user, password string) *Listener {
	node := node.NewNode(host, port, user, password, DEFAULT_SCHEMA, 0)
	return &Listener{Node: node, tables: map[uint64]*event.TableMapEvent{}}
}

func (listener *Listener) getFileAndPos() (string, uint32, error) {
	return "mysql-bin.000001", 4, nil
}

func (listener *Listener) writeDumpCmd() error {

	logName, logPos, err := listener.getFileAndPos()
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

	listener.SetPktSeq(0)
	listener.WritePacket(data)

	return nil
}

func (listener *Listener) Init() error {

	err := listener.Connect()
	if err != nil {
		return errors.Trace(err)
	}

	_, err = listener.Execute(mysql.COM_QUERY, []byte("set @master_binlog_checksum= @@global.binlog_checksum"))
	if err != nil {
		return errors.Trace(err)
	}

	_, err = listener.Execute(mysql.COM_QUERY, []byte("show master status"))
	if err != nil {
		return errors.Trace(err)
	}

	// 确定 dump 开始的文件和位置后, 全量同步一次 元数据
	// 若在 show master status 之前元数据有变化, 则全量可以同步到
	// 若在 show master statsu 之后元数据有变化, 则可以通过binlog 增量同步到
	meta := NewInformationSchema(listener)
	meta.parseMeta("", "")
	listener.meta = meta

	if err = listener.writeDumpCmd(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (listener *Listener) Start() error {

	for {
		pkt, err := listener.ReadPacket()
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

			listener.parseEvent(header, pkt[event.EventHeaderSize:])
		}
	}
	return nil
}

func (listener *Listener) parseEvent(header *event.EveHeader, data []byte) (event.Event, error) {
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
		log.Debug(listener.curTblEve)
		eve = &event.RowsEvent{Header: header, Table: listener.curTblEve}
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
		listener.tables[tbl.TblId] = tbl
		listener.syncBinlogAndIfSchema(tbl)
		listener.curTblEve = tbl
	}

	if re, ok := eve.(*event.RowsEvent); ok {
		table := listener.meta.tbs[re.Table.FullName]
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

func (listener *Listener) syncBinlogAndIfSchema(tbl *event.TableMapEvent) {

}
