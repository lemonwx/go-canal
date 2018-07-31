/**
 *  author: lim
 *  data  : 18-7-30 下午11:28
 */

package sync

import (
	"testing"
)

func TestNewJsonSyncerFromReader(t *testing.T) {
	syncer, err := NewJsonSyncerFromLocalFile("../cmd/binlog/mysql-bin.000001")
	if err != nil {
		t.Error(err)
	}

	for _, entry := range syncer.streamer.Events {
		t.Log(entry.Dump())
	}
}
