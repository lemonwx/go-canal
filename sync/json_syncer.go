/**
 *  author: lim
 *  data  : 18-7-25 下午8:22
 */

package sync

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	bsync "sync"
	"time"

	"github.com/juju/errors"
	"github.com/lemonwx/go-canal/event"
	"github.com/lemonwx/log"
	"io/ioutil"
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
	Event   event.Event
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

func NewJsonSyncerFromReader(fileName string) (*JsonSyncer, error) {
	js := &JsonSyncer{}
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	entrys := []JsonEntry{}

	err = json.Unmarshal(data, &entrys)
	if err != nil {
		return nil, err
	}
	for _, entry := range entrys {
		log.Debug(entry)
	}

	return js, nil
}

func (syncer *JsonSyncer) Sync(eve event.Event) error {
	encoded, err := json.Marshal(eve)
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}
	entry := JsonEntry{Eve: eve, Encoded: encoded}
	switch e := eve.(type) {
	case *event.FormatDescEvent:
		data, _ := json.Marshal(&entry)
		fmt.Fprintf(syncer.curWriter, "\n\t%s,\n", data)
	case *event.RotateEvent:
		if e.Header.Ts == 0 {
			syncer.curWriter, err = NewFileWriter(e.NextBinlog)
			if err != nil {
				return errors.Trace(err)
			}
		}
		data, err := json.Marshal(&entry)
		if err != nil {
			return errors.Trace(err)
		}
		if e.Header.Ts == 0 {
			fmt.Fprintf(syncer.curWriter, "[\n\t%s,\n", data)
		} else {
			fmt.Fprintf(syncer.curWriter, "\n\t%s\n]", data)
		}
	default:
		data, _ := json.Marshal(&entry)
		fmt.Fprintf(syncer.curWriter, "\n\t%s,\n", data)
	}
	return nil
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
