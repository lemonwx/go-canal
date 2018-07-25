/**
 *  author: lim
 *  data  : 18-7-25 下午8:22
 */

package sync

import (
	"encoding/json"
	"github.com/juju/errors"
	"github.com/lemonwx/go-canal/event"
	"github.com/lemonwx/log"
	"io/ioutil"
	"os"
)

type BinlogStreamer struct {
	Events []event.Event `json:"events"`
}

func (streamer *BinlogStreamer) append(eve event.Event) {
	streamer.Events = append(streamer.Events, eve)
}

type JsonSyncer struct {
	fileName string
	streamer *BinlogStreamer
	ch       chan event.Event
}

func NewJsonSyncer(fileName string, ch chan event.Event) *JsonSyncer {
	syncer := &JsonSyncer{
		fileName: fileName,
		ch:       ch,
		streamer: &BinlogStreamer{
			Events: make([]event.Event, 0, 1024),
		},
	}
	return syncer
}

func (syncer *JsonSyncer) Start() {
	log.Debug("Syncer start")
	for {
		eve := <-syncer.ch
		syncer.streamer.append(eve)
		syncer.Sync()
	}
}

func (syncer *JsonSyncer) Sync() error {
	//jsonEncode, err := json.Marshal(eve)
	encode, err := json.Marshal(syncer.streamer)
	if err != nil {
		return errors.Trace(err)
	}

	return ioutil.WriteFile(syncer.fileName, encode, os.ModePerm)
}
