package main

import (
	"flag"
	"fmt"
	"net"
	"time"
)

type Cmd struct {
	shutdownChannel chan bool
	flags           struct {
		flagParallel int64
		flagDuration time.Duration
		flagPort     int64
		flagPath     string
	}
}

func main() {
	cmd := &Cmd{
		shutdownChannel: make(chan bool, 5),
	}
	flag.Int64Var(&cmd.flags.flagParallel, "parallel", 10, "set number of parallel requests")
	flag.DurationVar(&cmd.flags.flagDuration, "duration", 10*time.Second, "set run duration")
	flag.Int64Var(&cmd.flags.flagPort, "port", 0, "set port to connect to")
	flag.StringVar(&cmd.flags.flagPath, "path", "", "set unix socket path to connect to")
	flag.Parse()

	if cmd.flags.flagPort == 0 && cmd.flags.flagPath == "" {
		fmt.Printf("ERROR: must specify either port or path\n")
		return
	}

	for i := 0; i < int(cmd.flags.flagParallel); i++ {
		go cmd.run()
	}

	time.Sleep(cmd.flags.flagDuration)
	cmd.shutdownChannel <- true
}

func (cmd *Cmd) run() {
	conn := cmd.getConnection()
	for {
		select {
		case <-cmd.shutdownChannel:
			cmd.shutdownChannel <- true
			if conn != nil {
				conn.Close()
				conn = nil
			}
			return
		default:
			if conn == nil {
				conn = cmd.getConnection()
			}
			if conn == nil {
				return
			}
			query := cmd.randomQuery()
			_, err := fmt.Fprintf(conn, "%s\n", query)
			if err != nil {
				if conn != nil {
					conn.Close()
					conn = nil
				}
				fmt.Printf("ERROR: %s\n", err.Error())
				conn = cmd.getConnection()
			}
		}
	}
}

func (cmd *Cmd) getConnection() net.Conn {
	conn, err := net.DialTimeout("unix", cmd.flags.flagPath, 60*time.Second)
	if err != nil {
		fmt.Printf("ERROR: %s\n", err.Error())
		return nil
	}
	return conn
}

func (cmd *Cmd) randomQuery() string {
	return "GET status\nKeepAlive: on\n"
}
