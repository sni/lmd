package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/nu7hatch/gouuid"
)

var reNodeAddress = regexp.MustCompile(`^(https?)?(://)?(.*?)(:(\d+))?(/.*)?$`)

// Nodes is the cluster management object.
type Nodes struct {
	PeerMap          *map[string]Peer
	ID               string
	HTTPClient       *http.Client
	WaitGroupInit    *sync.WaitGroup
	ShutdownChannel  chan bool
	loopInterval     int
	heartbeatTimeout int
	backends         []string
	thisNode         *NodeAddress
	nodeAddresses    []*NodeAddress
	onlineNodes      []*NodeAddress
	assignedBackends []string
	nodeBackends     map[string][]string
	stopChannel      chan bool
}

// NodeAddress contains the ip of a node (plus url/port, if necessary)
type NodeAddress struct {
	id   string
	ip   string
	port int
	url  string
	isMe bool
}

// HumanIdentifier returns the node address.
// If the node has been discovered, its id is prepended.
func (a *NodeAddress) HumanIdentifier() string {
	var human string
	human = fmt.Sprintf("%s:%d", a.ip, a.port)
	if a.id != "" {
		human = fmt.Sprintf("[%s] %s", a.id, human)
	}
	return human // :)
}

// NewNodes creates a new cluster manager.
func NewNodes(addresses []string, listen string, waitGroupInit *sync.WaitGroup, shutdownChannel chan bool) *Nodes {
	n := &Nodes{
		WaitGroupInit:   waitGroupInit,
		ShutdownChannel: shutdownChannel,
		stopChannel:     make(chan bool),
	}
	n.PeerMap = &DataStore
	n.HTTPClient = netClient
	for id := range *n.PeerMap {
		n.backends = append(n.backends, id)
	}
	partsListen := reNodeAddress.FindStringSubmatch(listen)
	for _, address := range addresses {
		parts := reNodeAddress.FindStringSubmatch(address)
		nodeAddress := &NodeAddress{}
		var ip, port, url string
		if parts[1] != "" && parts[2] != "" { // "http", "://"
			// HTTP address
			ip = parts[3]
			port = parts[5]
			if port == "" {
				port = partsListen[5]
			}
			url = address
		} else {
			// IP or TCP address
			ip = parts[3]
			port = parts[5]
			if port == "" {
				port = partsListen[5]
			}
			url = partsListen[1] + partsListen[2] // "http://"
			url += ip + ":" + port
		}
		if url[len(url)-1] != '/' {
			url += "/" // url must end in a slash
		}
		nodeAddress.ip = ip
		nodeAddress.port, _ = strconv.Atoi(port)
		nodeAddress.url = url
		for _, otherNodeAddress := range n.nodeAddresses {
			if otherNodeAddress.url == nodeAddress.url {
				log.Fatalf("Duplicate node url: %s", nodeAddress.url)
			}
		}
		n.nodeAddresses = append(n.nodeAddresses, nodeAddress)
	}

	return n
}

// IsClustered checks if cluster mode is enabled.
func (n *Nodes) IsClustered() bool {
	return len(n.nodeAddresses) > 1
}

// Node returns the NodeAddress object for the specified node id.
func (n *Nodes) Node(id string) NodeAddress {
	var nodeAddress NodeAddress
	for _, otherNodeAddress := range n.nodeAddresses {
		if otherNodeAddress.id != "" && otherNodeAddress.id == id {
			nodeAddress = *otherNodeAddress
			break
		}
	}
	if nodeAddress.id == "" {
		// Not found
		nodeAddress.id = id
	}
	return nodeAddress
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
		for id := range *n.PeerMap {
			peer := (*n.PeerMap)[id]
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
	// Send ping to all nodes
	// First ping (initializing) detects and assigns own address.
	wg := sync.WaitGroup{}
	ownIdentifier := n.ID
	if ownIdentifier == "" {
		panic("not initialized")
	}
	var newOnlineNodes []*NodeAddress
	initializing := n.thisNode == nil
	if !initializing {
		newOnlineNodes = append(newOnlineNodes, n.thisNode)
	}
	for _, node := range n.nodeAddresses {
		if !initializing && node.isMe {
			// Skip this node unless we're initializing
			continue
		}
		requestData := make(map[string]interface{})
		requestData["identifier"] = ownIdentifier
		log.Tracef("pinging node %s...", node.HumanIdentifier())
		wg.Add(1)
		go func(wg *sync.WaitGroup, node *NodeAddress) {
			callback := func(responseData interface{}) {
				// Parse response
				dataMap, ok := responseData.(map[string]interface{})
				log.Tracef("got response from %s", node.HumanIdentifier())
				if !ok {
					return
				}

				// Node id
				responseIdentifier := dataMap["identifier"].(string)
				if node.id == "" {
					node.id = responseIdentifier
				} else if node.id != responseIdentifier {
					log.Infof("partner node %s restarted", node.HumanIdentifier())
					delete(n.nodeBackends, node.id)
					node.id = responseIdentifier
				}

				// Check whose response it is
				if responseIdentifier == ownIdentifier {
					// This node
					if initializing {
						n.thisNode = node
						node.isMe = true
						newOnlineNodes = append(newOnlineNodes, node)
						log.Debugf("identified this node as %s", node.HumanIdentifier())
					}
				} else {
					// Partner node
					newOnlineNodes = append(newOnlineNodes, node)
					log.Tracef("discovered partner node: %s", node.HumanIdentifier())
				}
				wg.Done()
			}
			n.SendQuery(*node, "ping", requestData, callback)
		}(&wg, node)
	}

	// Handle timeout
	timeout := n.heartbeatTimeout
	if waitTimeout(&wg, time.Duration(timeout)*time.Second) {
		// Not all nodes have responded, but that's ok
		log.Tracef("node timeout")
		if initializing && n.thisNode == nil {
			// This node has not responded
			// This is an error. At this point, we don't know who we are.
			// This could happen if our ip is missing from the config
			// or if the local firewall is blocking traffic.
			panic("timeout while initializing nodes (own ip missing from config?)")
		}
	}

	// Redistribute backends
	n.onlineNodes = newOnlineNodes
	n.redistribute()

}

// redistribute assigns the peers to the available nodes.
// It starts peers assigned to this node and stops other peers.
func (n *Nodes) redistribute() {
	// Nodes and backends
	numberBackends := len(n.backends)
	allNodes := n.nodeAddresses
	nodeOnline := make([]bool, len(allNodes))
	numberAllNodes := len(allNodes)
	numberAvailableNodes := 0
	ownIndex := -1
	for i, node := range allNodes {
		isOnline := false
		for _, otherNode := range n.onlineNodes {
			if otherNode.url == node.url {
				isOnline = true
			}
		}
		if node.isMe {
			ownIndex = i
		}
		if isOnline {
			nodeOnline[i] = true
			numberAvailableNodes++
		}
	}

	// Assign items to nodes
	assignedNumberBackends := make([]int, numberAllNodes) // for each node
	if numberAvailableNodes >= numberBackends {
		// No node has more than one backend
		for i := range allNodes {
			if !nodeOnline[i] {
				continue
			}
			assignedNumberBackends[i] = 1
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
			id := allNodes[i].id
			nodeBackends[id] = list
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
func (n *Nodes) SendQuery(node NodeAddress, name string, parameters map[string]interface{}, callback func(interface{})) error {
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
	if node.url == "" {
		log.Fatalf("uninitialized node address provided to SendQuery %s", node.id)
	}
	url := node.url + "query"
	res, err := n.HTTPClient.Post(url, contentType, bytes.NewBuffer(rawRequest))
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
