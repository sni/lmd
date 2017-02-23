package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nu7hatch/gouuid"
)

// Nodes is the cluster management object.
type Nodes struct {
	PeerMap          *map[string]Peer
	NodeIPs          []string
	AddressPattern   string
	OwnIP            string
	ID               string
	HTTPClient       *http.Client
	WaitGroupInit    *sync.WaitGroup
	ShutdownChannel  chan bool
	loopInterval     int
	heartbeatTimeout int
	backends         []string
	onlineIPs        []string
	assignedBackends []string
	nodeBackends     map[string][]string
	stopChannel      chan bool
}

// NewNodes creates a new cluster manager.
func NewNodes(ips []string, pattern string, waitGroupInit *sync.WaitGroup, shutdownChannel chan bool) *Nodes {
	n := &Nodes{
		AddressPattern:  pattern,
		WaitGroupInit:   waitGroupInit,
		ShutdownChannel: shutdownChannel,
		stopChannel:     make(chan bool),
	}
	n.PeerMap = &DataStore
	for id := range *n.PeerMap {
		n.backends = append(n.backends, id)
	}
	for _, ip := range ips {
		n.NodeIPs = append(n.NodeIPs, ip)
	}
	n.HTTPClient = netClient

	return n
}

// IsClustered checks if cluster mode is enabled.
func (n *Nodes) IsClustered() bool {
	return len(n.NodeIPs) > 1
}

// Initialize generates the node's identifier and identifies this node.
// In single mode, it starts all peers.
// In cluster mode, the peers are started later, while the loop is running.
func (n *Nodes) Initialize() {
	// Default values
	if n.loopInterval == 0 {
		n.loopInterval = 10
	}
	if n.heartbeatTimeout == 0 {
		n.heartbeatTimeout = 3
	}

	// Generate identifier
	ownIdentifier := &n.ID
	*ownIdentifier = strconv.FormatInt(time.Now().Unix(), 10)
	u, err := uuid.NewV4()
	if err == nil {
		*ownIdentifier += ":" + u.String()
	}

	// Wait for own listener(s) to initialize
	n.WaitGroupInit.Wait()

	// Start all peers in single mode
	if !n.IsClustered() {
		for _, peer := range *n.PeerMap {
			peer.Start()
		}
	}

	// Send first ping (detect own ip) and wait for it to finish
	// This needs to be done before the loop is started.
	if n.IsClustered() {
		n.checkNodeAvailability()
	}

}

// Start starts the loop that periodically checks which nodes are online.
// Does nothing in single mode.
func (n *Nodes) Start() {
	// Do nothing in single mode
	if !n.IsClustered() {
		return
	}

	// Start loop in background
	go func() {
		n.loop()
	}()
}

// Stop stops the loop.
// Partner nodes won't be pinged automatically anymore.
func (n *Nodes) Stop() {
	n.stopChannel <- true
}

// loop triggers periodic checks until stopped.
func (n *Nodes) loop() {
	interval := n.loopInterval
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	for {

		select {
		case <-n.ShutdownChannel:
			ticker.Stop()
			return
		case <-n.stopChannel:
			ticker.Stop()
			return
		case <-ticker.C:
			n.checkNodeAvailability()

		}
	}
}

// checkNodeAvailability pings all partner nodes to determine which ones are online.
// When called for the first time during initialization, it also identifies this node.
func (n *Nodes) checkNodeAvailability() {
	// Send request to all nodes
	// First ping (initializing) detects and assigns own ip.
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
			// Skip this node unless we're initializing
			continue
		}
		requestData := make(map[string]interface{})
		requestData["identifier"] = ownIdentifier
		log.Tracef("pinging node %s...", node)
		wg.Add(1)
		go func(wg *sync.WaitGroup, node string) {
			callback := func(responseData interface{}) {
				// Parse response
				dataMap, ok := responseData.(map[string]interface{})
				log.Tracef("got response from %s", node)
				if !ok {
					return
				}
				responseIdentifier := dataMap["identifier"]
				// Check whose response it is
				if responseIdentifier == ownIdentifier {
					if initializing {
						n.OwnIP = node
						log.Debugf("identified this node as %s", node)
					}
				} else {
					newOnlineIPs = append(newOnlineIPs, node)
					log.Tracef("found partner node: %s", node)
				}
				wg.Done()
			}
			n.SendQuery(node, "ping", requestData, callback)
		}(&wg, node)
	}
	timeout := n.heartbeatTimeout
	if waitTimeout(&wg, time.Duration(timeout)*time.Second) {
		// Not all nodes have responded, but that's ok
		log.Tracef("node timeout")
		if initializing && n.OwnIP == "" {
			// This node has not responded
			// This is an error. At this point, we don't know who we are.
			// This could happen if our ip is missing from the config
			// or if the local firewall is blocking traffic.
			panic("timeout while initializing nodes (own ip missing from config?)")
		}
	}

	// Check if list of nodes has changed
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

	// Redistribute backends
	if !nodesUnchanged {
		log.Tracef("list of available partner nodes has changed")
		n.onlineIPs = newOnlineIPs
		n.redistribute()
	}

}

// redistribute assigns the peers to the available nodes.
// It starts peers assigned to this node and stops other peers.
func (n *Nodes) redistribute() {
	// Nodes and backends
	numberBackends := len(n.backends)
	allNodes := n.NodeIPs
	nodeOnline := make([]bool, len(allNodes))
	numberAllNodes := len(allNodes)
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
			// availableNodes[i] = node
			nodeOnline[i] = true
			numberAvailableNodes++
		}
	}

	// Assign items to nodes
	assignedNumberBackends := make([]int, numberAllNodes) // for each node
	if numberAvailableNodes >= numberBackends {
		// No node has more than one backend
		for i := range allNodes {
			if nodeOnline[i] {
				assignedNumberBackends[i] = 1
			}
		}
	} else {
		first := true
		for i := range allNodes {
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

	// List backends that we're responsible for
	assignedBackends := make([][]string, numberAllNodes) // for each node
	nodeBackends := make(map[string][]string)
	distributedCount := 0
	numberAssignedBackends := 0
	for i, number := range assignedNumberBackends {
		numberAssignedBackends += number
		if number != 0 { // number == 0 means no backends for that node
			list := make([]string, number)
			for i := 0; i < number; i++ {
				list[i] = n.backends[distributedCount+i]
			}
			distributedCount += number
			assignedBackends[i] = list
			nodeBackends[allNodes[i]] = list
		}
	}
	n.nodeBackends = nodeBackends
	ourBackends := assignedBackends[ownIndex]

	// Determine backends this node is now (not anymore) responsible for
	var addBackends []string
	var rmvBackends []string
	for _, backend := range n.backends {
		// Check if assigned now
		assignedNow := false
		for _, newBackend := range ourBackends {
			if newBackend == backend {
				assignedNow = true
			}
		}

		// Check if assigned previously
		assignedBefore := false
		for _, oldBackend := range n.assignedBackends {
			if oldBackend == backend {
				assignedBefore = true
			}
		}

		// Compare
		if assignedNow && !assignedBefore {
			addBackends = append(addBackends, backend)
		} else if assignedBefore && !assignedNow {
			rmvBackends = append(rmvBackends, backend)
		}
	}

	// Store assigned backends
	n.assignedBackends = ourBackends

	// Start/stop backends
	for _, oldBackend := range rmvBackends {
		peer := (*n.PeerMap)[oldBackend]
		peer.Stop()
		peer.Clear()
	}
	for _, newBackend := range addBackends {
		peer := (*n.PeerMap)[newBackend]
		peer.Start()
	}

}

// URL returns the HTTP address of the specified node.
func (n *Nodes) URL(node string) string {
	// Check if node exists
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

	// Address
	listenAddress := n.AddressPattern // http://*:1234
	nodeAddress := strings.Replace(listenAddress, "*", node, 1)

	return nodeAddress
}

// IsOurBackend checks if backend is managed by this node.
func (n *Nodes) IsOurBackend(backend string) bool {
	ourBackends := n.assignedBackends
	for _, ourBackend := range ourBackends {
		if ourBackend == backend {
			return true
		}
	}
	return false
}

// SendQuery sends a query to a node.
// It will be sent as http request; name is the api function to be called.
// The returned data will be passed to the callback.
func (n *Nodes) SendQuery(node string, name string, parameters map[string]interface{}, callback func(interface{})) error {
	// Prepare request data
	requestData := make(map[string]interface{})
	for key, value := range parameters {
		requestData[key] = value
	}
	requestData["_name"] = name // requested function

	// Encode request data
	contentType := "application/json"
	rawRequest, err := json.Marshal(requestData)
	if err != nil {
		return err
	}

	// Send node request
	nodeURL := n.URL(node)
	res, err := n.HTTPClient.Post(nodeURL, contentType, bytes.NewBuffer(rawRequest))
	if err != nil {
		log.Tracef("error sending query (%s) to node (%s): %s", name, node, err.Error())
		return err
	}

	// Read response data
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	var responseData interface{}
	if err := decoder.Decode(&responseData); err != nil {
		// Parsing response failed
		log.Tracef("%s", err.Error())
		return err
	}

	// Abort on error
	if res.StatusCode != 200 {
		var err error
		m, _ := responseData.(map[string]interface{})
		if v, ok := m["error"]; ok {
			err = fmt.Errorf(v.(string))
		} else {
			err = fmt.Errorf("node request failed: %s (code %d)", name, res.StatusCode)
		}
		log.Tracef("%s", err.Error())
		return err
	}

	// Trigger callback
	go func() {
		log.Tracef("calling callback for query (%s)", name)
		callback(responseData)
	}()

	return nil
}
