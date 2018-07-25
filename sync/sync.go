/**
 *  author: lim
 *  data  : 18-7-25 下午7:37
 */

package sync

import ()

type Syncer interface {
	Sync() error
}
