/**
 *  author: lim
 *  data  : 18-7-25 下午9:43
 */

package config

import "testing"

func TestReadConfig(t *testing.T) {
	cfg, err := ReadConfig()
	if err != nil {
		t.Log(err)
	}
	t.Log(cfg)
}
