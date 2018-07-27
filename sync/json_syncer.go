/**
 *  author: lim
 *  data  : 18-7-25 下午8:22
 */

package sync

import (
	"encoding/json"
	"io/ioutil"
	"os"
	bsync "sync"
	"time"

	"github.com/juju/errors"
	"github.com/lemonwx/go-canal/binlog"
	"github.com/lemonwx/go-canal/event"
	"github.com/lemonwx/log"
)

type BinlogStreamer struct {
	Events []event.Event `json:"events"`
	bsync.RWMutex
}

func (streamer *BinlogStreamer) append(eve event.Event) {
	streamer.Lock()
	streamer.Events = append(streamer.Events, eve)
	streamer.Unlock()
}

type JsonSyncer struct {
	fileName   string
	streamer   *BinlogStreamer
	ch         chan event.Event
	syncFlag   bool
	syncTimes  time.Duration
	syncCounts int
}

func NewJsonSyncer(fileName string, ch chan event.Event) *JsonSyncer {
	syncer := &JsonSyncer{
		fileName: fileName,
		ch:       ch,
		streamer: &BinlogStreamer{
			Events: make([]event.Event, 0, 1024),
		},
		syncFlag: true,
	}
	return syncer
}

func (syncer *JsonSyncer) Start() {
	log.Debug("Syncer start")
	for {
		eve := <-syncer.ch
		syncer.streamer.append(eve)
		if syncer.syncFlag {
			// 每接收到 binlog evnet 向文件中全量同步一次
			syncer.Sync()
		}
	}
}

func (syncer *JsonSyncer) Sync() error {
	// 同步 所有的binlog event 到文件中
	syncer.streamer.RLock()
	encode, err := json.Marshal(syncer.streamer)
	syncer.streamer.RUnlock()

	if err != nil {
		return errors.Trace(err)
	}

	return ioutil.WriteFile(syncer.fileName, encode, os.ModePerm)
}

func (syncer *JsonSyncer) CountSync() error {
	// 积攒 N 个binlog 后做一次同步
	for {
		syncer.streamer.RLock()
		if len(syncer.streamer.Events)%syncer.syncCounts == 0 {
			encode, err := json.Marshal(syncer.streamer)
			syncer.streamer.RUnlock()
			if err != nil {
				return errors.Trace(err)
			}
			return ioutil.WriteFile(syncer.fileName, encode, os.ModePerm)
		} else {
			syncer.streamer.RUnlock()
		}
	}

}

func (syncer *JsonSyncer) TimingSync() {
	// 定时同步 binlog event
	for {
		time.Sleep(syncer.syncTimes * time.Second)
		syncer.Sync()
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

func (syncer *JsonSyncer) GetByPk(schema, table string, field binlog.Field, val []byte) {
	// db.tb.id=1 根据这些信息得到一个 row event, 再由业务侧决定 回滚 还是 其他用途

}
