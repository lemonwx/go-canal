/**
 *  author: lim
 *  data  : 18-8-2 下午9:24
 */

package syncer

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lemonwx/go-canal/event"
	"github.com/lemonwx/log"
)

var (
	db *sql.DB
)

type Field struct {
	Name string
	Val  string
}

func (field *Field) String() string {
	return fmt.Sprintf("%s=%s", field.Name, field.Val)
}

type RollbackArg struct {
	Schema string
	Table  string
	Fields []*Field
	Ts     time.Time // if current event beforr than Ts 则已经完整过滤到了可能的 binlog, 停止扫描
	Te     time.Time // if last event after than Te 则当前已经同步到了需要的 binlog
}

func (arg *RollbackArg) String() string {
	return fmt.Sprintf("schema: %s table: %s %v", arg.Schema, arg.Table, arg.Fields)
}

func (syncer *JsonSyncer) Get(arg *RollbackArg) ([]event.Event, error) {
	startIdx := len(syncer.streamer.Events) - 1
	startEve := syncer.streamer.Events[startIdx]
	startEveTs := event.GetEventTime(startEve)
	log.Debugf("now sync to %s", startEveTs)

	if startEveTs.Before(arg.Te) {
		return nil, fmt.Errorf("startEvt's time: %s before than arg.Te: %s, "+
			"has not sync the binlog needed by this command", startEveTs, arg.Te)
	}

	log.Debugf("start: %s", event.GetEventTime(syncer.streamer.Events[startIdx]))
	log.Debugf("end  : %s", event.GetEventTime(syncer.streamer.Events[1]))

	events := make([]event.Event, 0, 10)
	getTrx := false
	v := arg.Fields[0]

	for idx := startIdx; idx >= 0; idx -= 1 {
		eve := syncer.streamer.Events[idx]
		curTs := event.GetEventTime(eve)

		if curTs.After(arg.Te) {
			continue
		}

		if curTs.Before(arg.Ts) {
			log.Debugf("get %d events, scan finish !!!", len(events))
			return events, nil
		}

		if e, ok := eve.(*event.RowsEvent); ok {
			if string(e.Table.Table) == arg.Table && string(e.Table.Schema) == arg.Schema {
				for _, row := range e.Rows {
					if strconv.FormatUint(row[0].(uint64), 10) == v.Val {
						getTrx = true
					}
				}
			}
		}
		events = append(events, eve)

		if _, ok := eve.(*event.GtidEvent); ok {
			if getTrx {
				log.Debug("get trx and get gtid event finish, break")
				break
			} else {
				// now grep an complete trx, but any row equal with args,
				//     so clear the slice and expect next trx will be matched
				log.Debugf("scan an complete trx, but rows not equal, so clear and continue")
				events = events[:0]
			}
		}
	}
	return events, nil
}

func (syncer *JsonSyncer) initDB() error {
	var err error
	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		syncer.User, syncer.Password, syncer.Host, syncer.Port))
	if err != nil {
		return err
	}
	return nil
}

func (syncer *JsonSyncer) getFieldName(schema, table string) ([]string, error) {
	if db == nil {
		err := syncer.initDB()
		if err != nil {
			return nil, err
		}
	}

	rows, err := db.Query("select column_name from information_schema.columns where "+
		"table_schema=? and table_name=?", schema, table)
	if err != nil {
		return nil, err
	}

	cols := make([]string, 0, 8)
	for rows.Next() {
		col := ""
		err := rows.Scan(&col)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}

	return cols, nil
}

func (syncer *JsonSyncer) Rollback(arg *RollbackArg) error {
	eves, err := syncer.Get(arg)
	if err != nil {
		log.Debug(err)
		return err
	}

	if len(eves) == 0 {
		return fmt.Errorf("no events to rollback")
	}

	size := len(eves)
	if _, ok := eves[0].(*event.XidEvnet); !ok {
		return fmt.Errorf("scan first event must be XidEvent, but get: %v", eves[0].Dump())
	}
	if _, ok := eves[size-1].(*event.GtidEvent); !ok {
		return fmt.Errorf("scan last event must be GtidEvent, but get: %v", eves[size-1].Dump())
	}

	fieldsMap := map[uint64][]string{}
	stmts := []*stmt{}

	for _, eve := range eves {
		if e, ok := eve.(*event.RowsEvent); ok {
			cols := []string{}
			ok := false
			cols, ok = fieldsMap[e.TblId]

			if !ok {
				var err error
				cols, err = syncer.getFieldName(string(e.Table.Schema), string(e.Table.Table))
				if err != nil {
					return err
				}
				fieldsMap[e.TblId] = cols
			}

			log.Debug(e.RollBack(cols))
			sql, vals, err := e.RollBack(cols)
			if err != nil {
				return err
			}

			s := &stmt{sql, vals}
			stmts = append(stmts, s)
		}
	}

	if len(stmts) != 0 {
		syncer.execute(stmts)
	} else {
		return fmt.Errorf("general %s sqls to execute", len(stmts))
	}
	return nil
}

type stmt struct {
	sql  string
	vals [][]interface{}
}

func (syncer *JsonSyncer) execute(stmts []*stmt) error {
	if db == nil {
		err := syncer.initDB()
		if err != nil {
			return err
		}
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, stmt := range stmts {
		log.Debug(stmt.sql, stmt.vals)
		s, err := tx.Prepare(stmt.sql)
		log.Debug(s, err)
		if err != nil {
			tx.Rollback()
			return err
		}

		for _, val := range stmt.vals {
			_, err := s.Exec(val...)
			log.Debug(err)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	return tx.Commit()
}
