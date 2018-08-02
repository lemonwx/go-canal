/**
 *  author: lim
 *  data  : 18-7-25 下午7:37
 */

package syncer

import (
	"github.com/lemonwx/go-canal/event"
)

type Syncer interface {
	Sync(event event.Event) error
}
