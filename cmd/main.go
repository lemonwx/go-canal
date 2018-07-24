/**
 *  author: lim
 *  data  : 18-7-17 下午10:36
 */

package main

import (
	"os"

	"github.com/lemonwx/go-canal/binlog"
	"github.com/lemonwx/go-canal/server"
	"github.com/lemonwx/log"
)

const (
	host     string = "172.17.0.2"
	port     int    = 5518
	user     string = "root"
	password string = "root"

	svrHost = "localhost"
	svrPort = 1236
)

func main() {
	// binlog listener start
	// server start
	log.NewDefaultLogger(os.Stdout)
	log.SetLevel(100)
	dumper := binlog.NewBinlogDumper(host, port, user, password)
	if err := dumper.Init(); err != nil {
		log.Error(err)
	}
	dumper.Start()

	svr, err := server.NewServer(svrHost, svrPort)
	if err != nil {
		panic(err)
	}

	svr.Serve()

}
