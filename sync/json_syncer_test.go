/**
 *  author: lim
 *  data  : 18-7-30 下午11:28
 */

package sync

import (
	"encoding/json"
	"github.com/lemonwx/go-canal/event"
	"io/ioutil"
	"testing"
)

func TestNewJsonSyncerFromReader(t *testing.T) {
	entrys := []JsonEntry{}
	data, _ := ioutil.ReadFile("../cmd/mysql-bin.000001")

	err := json.Unmarshal(data, &entrys)
	t.Log(err)
	for _, e := range entrys {
		v := event.TableMapEvent{}
		json.Unmarshal(e.Encoded, &v)
		t.Log(v)
	}
}
