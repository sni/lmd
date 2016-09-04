package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ReResponseHeader = regexp.MustCompile(`^(\d+)\s+(\d+)$`)

type DataTable struct {
	Table *Table
	Data  [][]interface{}
	Refs  map[string][][]interface{}
}

type Peer struct {
	Name            string
	Id              string
	Source          string
	Lock            sync.RWMutex
	Tables          map[string]DataTable
	Status          map[string]interface{}
	ErrorCount      int
	waitGroup       *sync.WaitGroup
	shutdownChannel chan bool
}

// send query to remote livestatus and returns unmarshaled result
func NewPeer(config *Connection, waitGroup *sync.WaitGroup, shutdownChannel chan bool) *Peer {
	p := Peer{
		Name:            config.Name,
		Id:              config.Id,
		Source:          config.Source,
		Tables:          make(map[string]DataTable),
		Status:          make(map[string]interface{}),
		ErrorCount:      0,
		waitGroup:       waitGroup,
		shutdownChannel: shutdownChannel,
	}
	p.Status["LastUpdate"] = time.Now()
	p.Status["LastQuery"] = time.Now()
	p.Status["LastError"] = "connecting..."
	p.Status["ProgramStart"] = 0
	return &p
}

// create initial objects
func (p *Peer) Start() (_, err error) {
	go func() {
		defer p.waitGroup.Done()
		p.waitGroup.Add(1)
		log.Infof("[%s] starting connection", p.Name)
		p.UpdateLoop()
	}()

	return
}

func (p *Peer) UpdateLoop() {
	for {
		ok := p.InitAllTables()

		// TODO: implement timer loop which refreshes completly every full minute including timeperiods
		// also implement idle_interval update (maybe one minute) and the normal update interval
		// refresh hosts / services every few seconds but only update the ones having a last_check date greater
		// last update or is_executing flag
		c := time.Tick(5e8 * time.Nanosecond)
		for {
			select {
			case <-p.shutdownChannel:
				log.Infof("stopping peer %s", p.Name)
				return
			case <-c:
				lastUpdate := p.Status["LastUpdate"].(time.Time)
				if time.Now().Add(-1 * time.Duration(GlobalConfig.Updateinterval) * time.Second).After(lastUpdate) {
					if !ok {
						ok = p.InitAllTables()
					} else {
						ok = p.UpdateAllTables()
					}
				}
				break
			}
		}
	}
}

func (p *Peer) InitAllTables() bool {
	var err error
	p.Status["LastUpdate"] = time.Now()
	t1 := time.Now()
	for _, t := range Objects.Tables {
		_, err = p.CreateObjectByType(t)
		if err != nil {
			duration := time.Since(t1)
			p.ErrorCount++
			if p.Status["LastError"] == "" || p.Status["LastError"] == "connecting..." {
				log.Warnf("[%s] site is offline: %s", p.Name, err.Error())
			} else {
				log.Infof("[%s] fetching initial objects still failing after %s: %s", p.Name, duration.String(), err.Error())
			}
			p.Status["LastError"] = err.Error()
			return false
		}
	}
	if p.Status["LastError"] != "" && p.Status["LastError"] != "connecting..." {
		log.Infof("[%s] site is back online", p.Name)
	}
	p.Status["LastError"] = ""
	p.Status["ProgramStart"] = p.Tables["status"].Data[0][p.Tables["status"].Table.ColumnsIndex["program_start"]]
	p.ErrorCount = 0
	duration := time.Since(t1)
	log.Infof("[%s] objects created in: %s", p.Name, duration.String())
	return true
}

func (p *Peer) UpdateAllTables() bool {
	t1 := time.Now()
	var err error
	p.Status["LastUpdate"] = time.Now()
	for _, t := range Objects.Tables {
		_, err = p.UpdateObjectByType(t)
		if err != nil {
			p.Status["LastError"] = err.Error()
			p.ErrorCount++
			duration := time.Since(t1)
			// give site some time to recover
			if p.ErrorCount >= 3 {
				log.Warnf("[%s] site went offline: %s", p.Name, err.Error())
				return false
			}
			log.Infof("[%s] updating objects failed (retry %d) after: %s: %s", p.Name, p.ErrorCount, duration.String(), err.Error())
			return true
		}
	}
	if p.Status["ProgramStart"] != p.Tables["status"].Data[0][p.Tables["status"].Table.ColumnsIndex["program_start"]] {
		log.Infof("[%s] site has been restarted, recreating objects", p.Name)
		return p.InitAllTables()
	}
	if err != nil && p.ErrorCount > 3 {
		return false
	}
	p.Status["LastError"] = ""
	duration := time.Since(t1)
	log.Infof("[%s] update complete in: %s", p.Name, duration.String())
	return true
}

// send query to remote livestatus and returns unmarshaled result
func (p *Peer) Query(table *Table, columns *[]string) (result [][]interface{}, err error) {
	connType := "unix"
	if strings.Contains(p.Source, ":") {
		connType = "tcp"
	}
	conn, err := net.DialTimeout(connType, p.Source, time.Duration(GlobalConfig.NetTimeout)*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Duration(GlobalConfig.NetTimeout) * time.Second))

	query := fmt.Sprintf("GET %s\nOutputFormat: json\nResponseHeader: fixed16\nColumns: %s\n\n", table.Name, strings.Join(*columns, " "))
	log.Tracef("[%s] send to: %s", p.Name, p.Source)
	log.Debugf("[%s] query: %s", p.Name, query)
	fmt.Fprintf(conn, "%s", query)

	var buf bytes.Buffer
	io.Copy(&buf, conn)

	log.Tracef("[%s] result: %s", p.Name, string(buf.Bytes()))

	resBytes := buf.Bytes()
	if len(resBytes) < 16 {
		err = errors.New("uncomplete response header: " + string(resBytes))
		return
	}
	header := resBytes[0:15]
	resBytes = resBytes[16:]

	matched := ReResponseHeader.FindStringSubmatch(string(header))
	if len(matched) != 3 {
		log.Errorf("[%s] uncomplete response header: %s", p.Name, string(header))
		return
	}
	resCode, _ := strconv.Atoi(matched[1])

	if resCode != 200 {
		log.Errorf("[%s] bad response: %s", p.Name, string(resBytes))
		return
	}
	err = json.Unmarshal(resBytes, &result)
	if err != nil {
		log.Errorf("[%s] json string: %s", p.Name, string(buf.Bytes()))
		log.Errorf("[%s] json error:", p.Name, err.Error())
		return
	}

	return
}

// send command query to remote livestatus and returns unmarshaled result
func (p *Peer) Command(command *string) (err error) {
	connType := "unix"
	if strings.Contains(p.Source, ":") {
		connType = "tcp"
	}
	conn, err := net.DialTimeout(connType, p.Source, time.Duration(GlobalConfig.NetTimeout)*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Duration(GlobalConfig.NetTimeout) * time.Second))

	log.Debugf("[%s] send to: %s", p.Name, p.Source)
	log.Debugf("[%s] command: %s", p.Name, *command)
	fmt.Fprintf(conn, "%s\n", *command)

	return
}

// create initial objects
func (p *Peer) CreateObjectByType(table *Table) (_, err error) {
	keys := []string{}
	for _, col := range table.Columns {
		if col.Update != RefUpdate && col.Update != RefNoUpdate {
			keys = append(keys, col.Name)
		}
	}
	res, err := p.Query(table, &keys)
	if err != nil {
		return
	}
	// expand references, create a hash entry for each reference type, ex.: hosts
	// and put an array containing the references (using the same index as the original row)
	refs := make(map[string][][]interface{})
	for _, refNum := range table.RefColCacheIndexes {
		refCol := table.Columns[refNum]
		fieldName := refCol.Name
		fieldIndex := refCol.RefColIndex
		refs[fieldName] = make([][]interface{}, len(res))
		RefByName := make(map[string][]interface{})
		for _, rowRef := range p.Tables[fieldName].Data {
			RefByName[rowRef[fieldIndex].(string)] = rowRef
		}
		for i, row := range res {
			refs[fieldName][i] = RefByName[row[refCol.RefIndex].(string)]
		}
	}
	p.Lock.Lock()
	p.Tables[table.Name] = DataTable{Table: table, Data: res, Refs: refs}
	p.Status["LastUpdate"] = time.Now()
	p.Lock.Unlock()
	return
}

// update objects
// assuming we get the objects always in the same order, we can just iterate over the index and update the fields
func (p *Peer) UpdateObjectByType(table *Table) (_, err error) {
	if len(table.DynamicColCacheNames) == 0 {
		return
	}
	res, err := p.Query(table, &table.DynamicColCacheNames)
	if err != nil {
		return
	}
	p.Lock.Lock()
	data := p.Tables[table.Name].Data
	for i, row := range res {
		for j, k := range table.DynamicColCacheIndexes {
			data[i][k] = row[j]
		}
	}
	p.Lock.Unlock()
	return
}
