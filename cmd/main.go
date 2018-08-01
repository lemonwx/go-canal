/**
 *  author: lim
 *  data  : 18-7-17 下午10:36
 */

package main

import (
	"os"

	"github.com/juju/errors"
	"github.com/lemonwx/go-canal/binlog"
	"github.com/lemonwx/go-canal/event"
	"github.com/lemonwx/go-canal/server"
	"github.com/lemonwx/go-canal/sync"
	"github.com/lemonwx/log"
)

const (
	host     string = "172.17.0.2"
	port     int    = 5518
	user     string = "root"
	password string = "root"

	svrHost = "localhost"
	svrPort = 1236

	syncFlag = true
)

var (
	eveBufSize uint32           = 100
	ch         chan event.Event = make(chan event.Event, eveBufSize)
	pos        binlog.Pos       = binlog.Pos{"mysql-bin.000001", 4}
)

func setupJsonSyncer() {
	syncer, err := sync.NewJsonSyncerFromLocalFile("mysql-bin.000001")
	if err != nil {
		log.Errorf("load binlog from json failed: %v", errors.ErrorStack(err))
		panic(err)
	}

	syncer.SetupChan(ch)
	pos = syncer.CurPos
	go syncer.Start()

}

func setupBinlogLis() {
	dumper := binlog.NewBinlogListener(host, port, user, password)
	if err := dumper.Init(pos); err != nil {
		log.Errorf("Init binlog dumer failed: %v", err)
	}
	go dumper.Start(ch)
}

func setupSvr() {
	svr, err := server.NewServer(svrHost, svrPort)
	if err != nil {
		log.Errorf("New Server failed:%v", err)
		panic(err)
	}

	svr.Serve()
}

func main() {
	log.NewDefaultLogger(os.Stdout)
	log.SetLevel(log.DEBUG)

	setupJsonSyncer()
	setupBinlogLis()
	setupSvr()
}
