/**
 *  author: lim
 *  data  : 18-7-25 下午8:22
 */

package syncer

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	bsync "sync"
	"time"

	"github.com/juju/errors"
	"github.com/lemonwx/go-canal/binlog"
	"github.com/lemonwx/go-canal/event"
	"github.com/lemonwx/log"
)

type BinlogStreamer struct {
	Events []event.Event
	bsync.RWMutex
}

func (streamer *BinlogStreamer) append(eve event.Event) {
	streamer.Lock()
	streamer.Events = append(streamer.Events, eve)
	streamer.Unlock()
}

type JsonEntry struct {
	EventName string
	EventType uint8

	Encoded []byte
}

type JsonSyncer struct {
	streamer   *BinlogStreamer
	ch         chan event.Event
	syncFlag   bool
	syncTimes  time.Duration
	syncCounts int

	curWriter io.Writer
	CurPos    binlog.Pos
}

func NewFileWriter(fileName string) (io.Writer, error) {
	return os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
}

func NewJsonSyncer(ch chan event.Event) *JsonSyncer {
	syncer := &JsonSyncer{
		ch: ch,
		streamer: &BinlogStreamer{
			Events: make([]event.Event, 0, 1024),
		},
		syncFlag: true,
	}
	return syncer
}

func (syncer *JsonSyncer) SetupChan(ch chan event.Event) {
	syncer.ch = ch
}

func (syncer *JsonSyncer) Sync(eve event.Event) error {
	encoded, err := json.Marshal(eve)
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}

	Type := event.GetEventType(eve)
	entry := JsonEntry{event.EventName[Type], Type, encoded}
	data, err := json.Marshal(&entry)
	if err != nil {
		return errors.Trace(err)
	}
	writeFmt := ""

	switch entry.EventType {
	case event.FORMAT_DESCRIPTION_EVENT:
		writeFmt = "\n\t%s,\n"
	case event.ROTATE_EVENT:
		rotate := eve.(*event.RotateEvent)
		if rotate.Header.Ts == 0 {
			if syncer.curWriter, err = NewFileWriter(event.BASE_BINLOG_PATH + rotate.NextBinlog); err != nil {
				return errors.Trace(err)
			}
		}
		if rotate.Header.Ts == 0 {
			writeFmt = "[\n\t%s,\n"
		} else {
			writeFmt = "\n\t%s\n]"
		}
	case event.STOP_EVENT:
		writeFmt = "\n\t%s\n]"
	default:
		writeFmt = "\n\t%s,\n"
	}

	_, err = fmt.Fprintf(syncer.curWriter, writeFmt, data)
	return err
}

func (syncer *JsonSyncer) Start() {
	log.Debug("Syncer start")
	for {
		eve := <-syncer.ch
		syncer.streamer.append(eve)
		syncer.Sync(eve)
	}
}

func (syncer *JsonSyncer) CountSync() error {
	// 积攒 N 个binlog 后做一次同步
	for {
		syncer.streamer.RLock()
		if len(syncer.streamer.Events)%syncer.syncCounts == 0 {
			_, err := json.Marshal(syncer.streamer)
			syncer.streamer.RUnlock()
			if err != nil {
				return errors.Trace(err)
			}
			return nil
		} else {
			syncer.streamer.RUnlock()
		}
	}
}

func (syncer *JsonSyncer) TimingSync() {
	// 定时同步 binlog event
	for {
		time.Sleep(syncer.syncTimes * time.Second)
		//syncer.Sync()
	}
}

func (syncer *JsonSyncer) StartSync() {
	// format event 开始一个新的 json 文件, 对应一个binlog文件
	// rotate event 结束一个 json 文件
	// 接收到一个事件后 根据配置的同步规则 同步到当前的 json 文件中
}

func (syncer *JsonSyncer) FullSync() {

}

func (syncer *JsonSyncer) IncrementSync() {

}

func (syncer *JsonSyncer) LoadFromJson() error {
	return nil
}

func (syncer *JsonSyncer) GetByPk(schema, table string, field string, val []byte) {
	// db.tb.id=1 根据这些信息得到一个 row event, 再由业务侧决定 回滚 还是 其他用途

}

func NewJsonSyncerFromLocalFile(startFile string) (*JsonSyncer, error) {
	js := &JsonSyncer{
		streamer: &BinlogStreamer{
			Events: make([]event.Event, 0, 1024),
		},
	}

	fileName := startFile
	for {
		log.Debugf("parse binlog from %s", fileName)
		data, err := ioutil.ReadFile(event.BASE_BINLOG_PATH + fileName)
		if err != nil {
			log.Debugf("read file: %v failed, return", fileName)
			break
		}

		entrys := []JsonEntry{}
		err = json.Unmarshal(data, &entrys)
		if err != nil {
			return nil, errors.Trace(fmt.Errorf("parse [%s] failed: %v", fileName, err))
		}

		for idx, entry := range entrys {
			eve, err := DecodeFromJson(entry)
			if err != nil {
				return nil, errors.Trace(err)
			}

			js.streamer.append(eve)

			if idx == len(entrys)-1 {
				if _, ok := eve.(*event.StopEvent); ok {
					to := strings.Split(fileName, ".")
					nextIdx, err := strconv.ParseUint(to[len(to)-1], 10, 64)
					if err != nil {
						return nil, err
					}
					fileName = fmt.Sprintf("mysql-bin.%06d", nextIdx+1)
				} else if rotate, ok := eve.(*event.RotateEvent); ok {
					log.Debugf("get next binlog %s from rotate event", rotate.NextBinlog)
					fileName = rotate.NextBinlog
				} else {
					return nil, fmt.Errorf("last event should be RotateEvent, but recv: %v", eve)
				}
			}
		}
	}
	js.CurPos = binlog.Pos{fileName, 4}
	return js, nil
}

func (syncer *JsonSyncer) RemoveBinlogGtNow(filename string) error {
	fs, err := ioutil.ReadDir(event.BASE_BINLOG_PATH)
	if err != nil {
		return errors.Trace(err)
	}

	for _, f := range fs {
		if f.Name() >= filename {
			if err = os.Remove(event.BASE_BINLOG_PATH + f.Name()); err != nil {
				return errors.Trace(err)
			}
			log.Debugf("rm file: %v success", f.Name())
		}
	}

	return nil
}

func DecodeFromJson(entry JsonEntry) (event.Event, error) {
	var eve event.Event

	switch entry.EventType {
	case event.ROTATE_EVENT:
		eve = &event.RotateEvent{}
	case event.FORMAT_DESCRIPTION_EVENT:
		eve = &event.FormatDescEvent{}
	case event.QUERY_EVENT:
		eve = &event.QueryEvent{}
	case event.WRITE_ROWS_EVENT_V2, event.UPDATE_ROWS_EVENT_V2, event.DELETE_ROWS_EVENT_V2:
		eve = &event.RowsEvent{}
	case event.TABLE_MAP_EVENT:
		eve = &event.TableMapEvent{}
	case event.GTID_LOG_EVENT:
		eve = &event.GtidEvent{}
	case event.XID_EVENT:
		eve = &event.XidEvnet{}
	case event.STOP_EVENT:
		eve = &event.StopEvent{}

	default:
		eve = &event.FormatDescEvent{}
	}

	if err := json.Unmarshal(entry.Encoded, &eve); err != nil {
		return nil, errors.Trace(err)
	}
	return eve, nil
}
