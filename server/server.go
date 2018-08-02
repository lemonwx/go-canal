/**
 *  author: lim
 *  data  : 18-7-17 下午10:56
 */

package server

import (
	"bytes"
	"fmt"
	"net"
	"sync"

	"github.com/lemonwx/go-canal/syncer"
	"github.com/lemonwx/log"
)

type Server struct {
	host     string
	port     int
	listener net.Listener
	sync.RWMutex
	running bool
	syncer  syncer.Syncer
}

func NewServer(host string, port int, syncer syncer.Syncer) (*Server, error) {
	s := new(Server)
	s.host = host
	s.port = port
	s.syncer = syncer

	var err error
	s.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", s.host, s.port))
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Server) Serve() error {
	s.running = true
	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		go s.onConn(conn)
	}
	return nil
}

func (s *Server) onConn(conn net.Conn) error {
	for {
		request, err := NewRequest(conn)
		if err != nil {
			return err
		}

		reply := s.handleRequest(request)

		if _, err := reply.WriteTo(conn); err != nil {
			return err
		}

	}
	return nil
}

func (s *Server) chkArgs(args [][]byte) (*syncer.RollbackArg, error) {

	arg := &syncer.RollbackArg{}

	if len(args) != 3 {
		return nil, fmt.Errorf("args size must be 3")
	}

	pointIdx := bytes.Index(args[0], []byte("."))
	if pointIdx == -1 {
		return nil, fmt.Errorf("first args must be scheam.table")
	}
	arg.Schema = string(args[0][:pointIdx])
	arg.Table = string(args[0][pointIdx+1:])

	for idx := 1; idx <= 2; idx += 1 {
		equalIdx := bytes.Index(args[idx], []byte("="))
		if equalIdx == -1 {
			return nil, fmt.Errorf("field args must be field=val")
		}
		field := &syncer.Field{}
		field.Name = string(args[idx][:equalIdx])
		field.Val = string(args[idx][equalIdx+1:])
		arg.Fields = append(arg.Fields, field)
	}
	return arg, nil
}

func (s *Server) handleRequest(request *Request) Reply {

	arg, err := s.chkArgs(request.Arguments)
	if err != nil {
		return &ErrorReply{message: err.Error()}
	}

	switch request.Command {
	case "GET":
		s.syncer.Get(arg)
	case "ROLLBACK":
		s.syncer.Rollback(arg)
	default:
		return &ErrorReply{message: "unsupported command"}
	}

	return &StatusReply{
		code: "OK",
	}
}
