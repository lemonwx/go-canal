/**
 *  author: lim
 *  data  : 18-7-17 下午10:56
 */

package server

import (
	"fmt"
	"net"
	"sync"
)

type Server struct {
	host     string
	port     int
	listener net.Listener
	sync.RWMutex
	running bool
}

func NewServer(host string, port int) (*Server, error) {
	s := new(Server)
	s.host = host
	s.port = port

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

		fmt.Print(request.Command, request.Arguments)

	}
	return nil
}
