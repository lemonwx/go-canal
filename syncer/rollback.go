/**
 *  author: lim
 *  data  : 18-8-2 下午9:24
 */

package syncer

import (
	"fmt"
	"github.com/lemonwx/go-canal/event"
	"github.com/lemonwx/log"
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
}

func (arg *RollbackArg) String() string {
	return fmt.Sprintf("schema: %s table: %s %v", arg.Schema, arg.Table, arg.Fields)
}

func (syncer *JsonSyncer) Get(arg *RollbackArg) ([]event.Event, error) {
	startIdx := len(syncer.streamer.Events) - 1
	events := make([]event.Event, 0, 10)
	for idx := startIdx; idx >= 0; idx -= 1 {
		events = append(events, syncer.streamer.Events[idx])
	}

	return events, nil
}

func (syncer *JsonSyncer) Rollback(arg *RollbackArg) error {
	eves, err := syncer.Get(arg)
	if err != nil {
		return err
	}

	for _, eve := range eves {
		log.Debug(eve.Dump())
	}

	return nil
}
