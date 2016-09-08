package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"reflect"
)

func assertEq(exp, got interface{}) error {
	if !reflect.DeepEqual(exp, got) {
		return fmt.Errorf("\nWanted \n%#v\nGot\n%#v", exp, got)
	}
	return nil
}

func StartMockLivestatusSource(address string) {
	startedChannel := make(chan bool)
	go func() {
		l, err := net.Listen("tcp", "127.0.0.1:50050")
		if err != nil {
			panic(err.Error())
		}
		defer l.Close()
		startedChannel <- true
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			req, err := ParseRequest(conn)
			if err != nil {
				panic(err.Error())
			}

			dat, err := ioutil.ReadFile("../t/data/" + req.Table + ".json")
			if err != nil {
				panic("could not read file: " + err.Error())
			}
			conn.Write([]byte(dat))
			conn.Close()
		}
	}()
	<-startedChannel
	return
}
