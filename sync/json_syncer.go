/**
 *  author: lim
 *  data  : 18-7-25 下午8:22
 */

package sync

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	bsync "sync"
	"time"

	"github.com/juju/errors"
	"github.com/lemonwx/go-canal/event"
	"github.com/lemonwx/log"
	"strconv"
	"strings"
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

func (syncer *JsonSyncer) Sync(eve event.Event) error {
	encoded, err := json.Marshal(eve)
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}

	entry := JsonEntry{EventType: event.GetEventType(eve), Encoded: encoded}
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
			if syncer.curWriter, err = NewFileWriter(rotate.NextBinlog); err != nil {
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
		data, err := ioutil.ReadFile(fileName)
		if err != nil {
			return nil, err
		}

		entrys := []JsonEntry{}
		err = json.Unmarshal(data, &entrys)
		if err != nil {
			if err.Error() == "unexpected end of JSON input" {
				// 异常退出, 没有及时写入到 json 文件, 则从该 json 文件对应的 binlog 位置 重新同步 binlog
				return js, err
			}
			return nil, err
		}
		for idx, entry := range entrys {
			eve, err := DecodeFromJson(entry)
			if err != nil {
				return nil, err
			}

			js.streamer.append(eve)
			log.Debug(entry.EventType)

			if idx == len(entrys)-1 {
				if _, ok := eve.(*event.StopEvent); ok {
					to := strings.Split(fileName, ".")
					nextIdx, err := strconv.ParseUint(to[len(to)-1], 10, 64)
					if err != nil {
						return nil, err
					}

					fileName = fmt.Sprintf("../cmd/%s/mysql-bin.%06d", event.BASE_BINLOG_PATH, nextIdx+1)
				} else if rotate, ok := eve.(*event.RotateEvent); ok {
					fileName = "../cmd/" + rotate.NextBinlog
				} else {
					return nil, fmt.Errorf("last event should be RotateEvent, but recv: %v", eve)
				}
			}
		}
	}

	return js, nil
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
		return nil, err
	}
	return eve, nil
}
