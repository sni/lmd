package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nu7hatch/gouuid"
)

type Nodes struct {
	NodeIPs          []string
	OwnIP            string
	ID               string
	onlineIPs        []string
	Backends         []string
	assignedBackends []string
	HTTPClient       *http.Client
	AddressPattern   string
	waitGroupInit    *sync.WaitGroup
	shutdownChannel  chan bool
	loopInterval     int
	heartbeatTimeout int
}

func (n *Nodes) Initialize() {
	//Default values
	if n.loopInterval == 0 {
		n.loopInterval = 10
	}
	if n.heartbeatTimeout == 0 {
		n.heartbeatTimeout = 3
	}

	//Generate identifier
	ownIdentifier := &n.ID
	*ownIdentifier = strconv.FormatInt(time.Now().Unix(), 10)
	u, err := uuid.NewV4()
	if err == nil {
		*ownIdentifier += ":" + u.String()
	}

	//Start loop
	n.Start()

}

func (n *Nodes) Start() {
	//Nothing to do if running in single mode
	if !n.IsClustered() {
		return
	}

	//Wait for own listener(s) to initialize
	n.waitGroupInit.Wait()

	//Send first heartbeat (who am I) and wait for it to finish
	n.heartbeat()

	//Start loop in background
	go func() {
		n.loop()
	}()

}

func (n *Nodes) IsClustered() bool {
	return len(n.NodeIPs) > 1
}

func (n *Nodes) loop() {
	interval := n.loopInterval
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	for {

		select {
		case <-n.shutdownChannel:
			ticker.Stop()
			return
		case <-ticker.C:
			n.heartbeat()

		}
	}
}

func (n *Nodes) heartbeat() {
	//Send request to all nodes
	//First heartbeat (initializing) detects and assigns own ip.
	wg := sync.WaitGroup{}
	ownIdentifier := n.ID
	if ownIdentifier == "" {
		panic("own node id undefined")
	}
	var newOnlineIPs []string
	newOnlineIPs = append(newOnlineIPs, n.OwnIP)
	initializing := n.OwnIP == ""
	for _, node := range n.NodeIPs {
		if !initializing && node == n.OwnIP {
			//Skip this node unless we're initializing
			continue
		}
		requestData := make(map[string]interface{})
		requestData["identifier"] = ownIdentifier
		log.Debugf("pinging node %s...", node)
		wg.Add(1)
		go func(wg *sync.WaitGroup, node string) {
			callback := func(responseData interface{}) {
				//Parse response
				dataMap, ok := responseData.(map[string]interface{})
				log.Tracef("got response from %s", node)
				if !ok {
					return
				}
				responseIdentifier := dataMap["identifier"]
				//Check whose response it is
				if responseIdentifier == ownIdentifier {
					if initializing {
						n.OwnIP = node
						log.Debugf("identified this node as %s", node)
					}
				} else {
					newOnlineIPs = append(newOnlineIPs, node)
					log.Debugf("found partner node: %s", node)
				}
				wg.Done()
			}
			n.SendQuery(node, "ping", requestData, callback)
		}(&wg, node)
	}
	timeout := n.heartbeatTimeout
	if waitTimeout(&wg, time.Duration(timeout)*time.Second) {
		//Not all nodes have responded, but that's ok
		log.Debugf("node timeout")
		if initializing && n.OwnIP == "" {
			//This node has not responded
			//This is an error. At this point, we don't know who we are.
			//This could happen if our ip is missing from the config
			//or if the local firewall is blocking traffic.
			panic("timeout while initializing nodes (own ip missing from config?)")
		}
	}

	//Check if list of nodes has changed
	nodesUnchanged := len(newOnlineIPs) == len(n.onlineIPs)
	if initializing {
		nodesUnchanged = false
	}
	if nodesUnchanged {
		for i, v := range n.onlineIPs {
			if newOnlineIPs[i] != v {
				nodesUnchanged = false
				break
			}
		}
	}
	if !nodesUnchanged {
		log.Debugf("list of available partner nodes has changed")
		n.onlineIPs = newOnlineIPs
		n.redistribute()
	}

}

func (n *Nodes) redistribute() {
	//Nodes and backends
	numberBackends := len(n.Backends)
	allNodes := n.NodeIPs
	nodeOnline := make([]bool, len(allNodes))
	numberAllNodes := len(n.NodeIPs)
	numberAvailableNodes := 0
	ownIndex := -1
	for i, node := range allNodes {
		isOnline := false
		for _, otherNode := range n.onlineIPs {
			if otherNode == node {
				isOnline = true
			}
		}
		if node == n.OwnIP {
			ownIndex = i
		}
		if node == n.OwnIP || isOnline {
			//availableNodes[i] = node
			nodeOnline[i] = true
			numberAvailableNodes++
		}
	}

	//Assign items to nodes
	assignedNumberBackends := make([]int, numberAllNodes) //for each node
	if numberAvailableNodes >= numberBackends {
		//No node has more than one backend
		for i, _ := range allNodes {
			if nodeOnline[i] {
				assignedNumberBackends[i] = 1
			}
		}
	} else {
		first := true
		for i, _ := range allNodes {
			if !nodeOnline[i] {
				continue
			}
			numberPerNode := numberBackends / numberAvailableNodes
			if first {
				first = false
				if numberBackends%numberAvailableNodes != 0 {
					numberPerNode++
				}
			}
			assignedNumberBackends[i] = numberPerNode
		}
	}

	//List backends that we're responsible for
	assignedBackends := make([][]string, numberAllNodes) //for each node
	distributedCount := 0
	numberAssignedBackends := 0
	for i, number := range assignedNumberBackends {
		numberAssignedBackends += number
		if number != 0 {
			list := make([]string, number)
			for i := 0; i < number; i++ {
				list[i] = n.Backends[distributedCount+i]
			}
			distributedCount += number
			assignedBackends[i] = list
		}
	}
	if numberAssignedBackends != numberBackends {
		panic(fmt.Sprintf("programmer error: assigned %d out of %d backends", numberAssignedBackends, numberBackends))
	}
	ourBackends := assignedBackends[ownIndex]

	//Determine backends this node is now (not anymore) responsible for
	var addBackends []string
	var rmvBackends []string
	for _, backend := range n.Backends {
		//Check if assigned now
		assignedNow := false
		for _, newBackend := range ourBackends {
			if newBackend == backend {
				assignedNow = true
			}
		}

		//Check if assigned previously
		assignedBefore := false
		for _, oldBackend := range n.assignedBackends {
			if oldBackend == backend {
				assignedBefore = true
			}
		}

		//Compare
		if assignedNow && !assignedBefore {
			addBackends = append(addBackends, backend)
		} else if assignedBefore && !assignedNow {
			rmvBackends = append(rmvBackends, backend)
		}
	}

	//Store assigned backends
	n.assignedBackends = ourBackends

	//Start/stop backends
	for _, oldBackend := range rmvBackends {
		peer := DataStore[oldBackend]
		peer.Stop() //TODO fix (this should not stop the http server)
		//TODO peer.Clear()
	}
	for _, newBackend := range addBackends {
		peer := DataStore[newBackend]
		peer.Start()
	}

}

// URL returns the HTTP address of the specified node.
func (n *Nodes) URL(node string) string {
	//Insanity check
	exists := false
	for _, currentNode := range n.NodeIPs {
		if currentNode == node {
			exists = true
		}
	}
	if !exists {
		panic(fmt.Sprintf("requested node does not exist: %s", node))
	}
	if n.AddressPattern == "" {
		panic(fmt.Sprintf("could not determine node address pattern"))
	}

	//Address
	listenAddress := n.AddressPattern //http://*:1234
	nodeAddress := strings.Replace(listenAddress, "*", node, 1)

	return nodeAddress
}

func (n *Nodes) SendQuery(node string, name string, parameters map[string]interface{}, callback func(interface{})) error {
	//Prepare request data
	requestData := make(map[string]interface{})
	for key, value := range parameters {
		requestData[key] = value
	}
	requestData["_name"] = name //requested function

	//Encode request data
	contentType := "application/json"
	rawRequest, err := json.Marshal(requestData)
	if err != nil {
		return err
	}

	//Send node request
	nodeURL := n.URL(node)
	res, err := n.HTTPClient.Post(nodeURL, contentType, bytes.NewBuffer(rawRequest))
	if err != nil {
		log.Tracef("error sending query (%s) to node (%s): %s", name, node, err.Error())
		return err
	}
	if res.StatusCode != 200 {
		err := errors.New(fmt.Sprintf("node request failed: %s (code %d)", name, res.StatusCode))
		log.Tracef("%s", err.Error())
		return err
	}

	//Trigger callback
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	responseData := make(map[string]interface{})
	if err := decoder.Decode(&responseData); err != nil {
		//Parsing response failed
		log.Tracef("%s", err.Error())
		return err
	}
	go func() {
		log.Tracef("calling callback for query (%s)", name)
		callback(responseData)
	}()

	return nil
}
