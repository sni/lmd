package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var reNodeAddress = regexp.MustCompile(`^(https?)?(://)?(.*?)(:(\d+))?(/.*)?$`)

// Nodes is the cluster management object.
type Nodes struct {
	noCopy           noCopy
	ID               string
	HTTPClient       *http.Client
	WaitGroupInit    *sync.WaitGroup
	ShutdownChannel  chan bool
	loopInterval     int
	heartbeatTimeout int
	backends         []string
	thisNode         *NodeAddress
	nodeAddresses    NodeAddressList
	onlineNodes      NodeAddressList
	assignedBackends []string
	nodeBackends     map[string][]string
	stopChannel      chan bool
	lmd              *LMDInstance
}

// NodeAddress contains the ip of a node (plus url/port, if necessary)
type NodeAddress struct {
	id   string
	ip   string
	port int
	url  string
	isMe bool
}

// String returns the node address.
// If the node has been discovered, its id is prepended.
func (a *NodeAddress) String() string {
	addr := fmt.Sprintf("%s:%d", a.ip, a.port)
	if a.id != "" {
		return fmt.Sprintf("[%s] %s", a.id, addr)
	}
	return addr
}

// NodeAddressList is a list of nodeaddresses
type NodeAddressList []*NodeAddress

// String returns the stringified node address list.
func (a *NodeAddressList) String() string {
	s := []string{}
	for _, l := range *a {
		s = append(s, l.String())
	}
	sort.Strings(s)
	return strings.Join(s, ",")
}

// NewNodes creates a new cluster manager.
func NewNodes(lmd *LMDInstance, addresses []string, listen string) *Nodes {
	n := &Nodes{
		WaitGroupInit:   lmd.waitGroupInit,
		ShutdownChannel: lmd.shutdownChannel,
		stopChannel:     make(chan bool),
		nodeBackends:    make(map[string][]string),
		lmd:             lmd,
	}
	tlsConfig := getMinimalTLSConfig(lmd.Config)
	n.HTTPClient = NewLMDHTTPClient(tlsConfig, "")
	for i := range lmd.Config.Connections {
		n.backends = append(n.backends, lmd.Config.Connections[i].ID)
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
func (n *Nodes) Node(id string) *NodeAddress {
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
	return &nodeAddress
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
	*ownIdentifier += ":" + generateUUID()

	// Wait for own listener(s) to initialize
	n.WaitGroupInit.Wait()

	// Start all peers in single mode
	if !n.IsClustered() {
		n.lmd.PeerMapLock.RLock()
		for id := range n.lmd.PeerMap {
			peer := n.lmd.PeerMap[id]
			if peer.StatusGet(Paused).(bool) {
				peer.Start()
			}
		}
		n.lmd.PeerMapLock.RUnlock()
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
		defer n.lmd.logPanicExit()
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
	var newOnlineNodes NodeAddressList
	initializing := n.thisNode == nil
	if !initializing {
		newOnlineNodes = append(newOnlineNodes, n.thisNode)
	}
	redistribute := false
	for _, node := range n.nodeAddresses {
		if !initializing && node.isMe {
			// Skip this node unless we're initializing
			continue
		}
		requestData := make(map[string]interface{})
		requestData["identifier"] = ownIdentifier
		requestData["peers"] = strings.Join(n.nodeBackends[node.id], ";")
		log.Tracef("pinging node %s...", node)
		wg.Add(1)
		go func(wg *sync.WaitGroup, node *NodeAddress) {
			defer n.lmd.logPanicExit()
			isOnline, forceRedistribute := n.sendPing(node, initializing, requestData)
			if forceRedistribute {
				redistribute = true
			}
			if isOnline {
				newOnlineNodes = append(newOnlineNodes, node)
			}
			wg.Done()
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
	if redistribute || n.onlineNodes.String() != newOnlineNodes.String() {
		n.onlineNodes = newOnlineNodes
		n.redistribute()
	}
}

// redistribute assigns the peers to the available nodes.
// It starts peers assigned to this node and stops other peers.
func (n *Nodes) redistribute() {
	// Nodes and backends
	numberBackends := len(n.backends)
	ownIndex, nodeOnline, numberAllNodes, numberAvailableNodes := n.getOnlineNodes()
	allNodes := n.nodeAddresses
	log.Infof("redistributing peers within cluster, %d/%d nodes online", numberAvailableNodes, numberAllNodes)

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
		for i := range allNodes {
			if !nodeOnline[i] {
				continue
			}
			numberPerNode := numberBackends / numberAvailableNodes
			if numberBackends%numberAvailableNodes != 0 {
				numberPerNode++
			}
			assignedNumberBackends[i] = numberPerNode
		}
	}

	// List backends that we're responsible for
	assignedBackends := make([][]string, numberAllNodes) // for each node
	nodeBackends := make(map[string][]string)
	distributedCount := 0
	for i, number := range assignedNumberBackends {
		if number <= 0 {
			continue
		}
		list := make([]string, 0)
		for j := 0; j < number; j++ {
			if len(n.backends) > distributedCount+j {
				if n.backends[distributedCount+j] != "" {
					list = append(list, n.backends[distributedCount+j])
				}
			}
		}
		distributedCount += number
		assignedBackends[i] = list
		id := allNodes[i].id
		nodeBackends[id] = list
	}
	n.nodeBackends = nodeBackends
	ourBackends := assignedBackends[ownIndex]

	n.updateBackends(ourBackends)
}

func (n *Nodes) updateBackends(ourBackends []string) {
	// append sub peers
	n.lmd.PeerMapLock.RLock()
	for id := range n.lmd.PeerMap {
		p := n.lmd.PeerMap[id]
		if p.ParentID == "" {
			continue
		}
		for _, id := range ourBackends {
			if id == p.ParentID {
				ourBackends = append(ourBackends, p.ID)
				break
			}
		}
	}
	n.lmd.PeerMapLock.RUnlock()

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
	n.lmd.PeerMapLock.RLock()
	for _, oldBackend := range rmvBackends {
		peer := n.lmd.PeerMap[oldBackend]
		peer.Stop()
		peer.ClearData(true)
	}
	for _, newBackend := range addBackends {
		peer := n.lmd.PeerMap[newBackend]
		peer.Start()
	}
	n.lmd.PeerMapLock.RUnlock()
}

func (n *Nodes) getOnlineNodes() (ownIndex int, nodeOnline []bool, numberAllNodes int, numberAvailableNodes int) {
	allNodes := n.nodeAddresses
	numberAllNodes = len(allNodes)
	numberAvailableNodes = 0
	nodeOnline = make([]bool, numberAllNodes)
	ownIndex = -1
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
	return
}

// IsOurBackend checks if backend is managed by this node.
func (n *Nodes) IsOurBackend(backend string) bool {
	if !n.lmd.nodeAccessor.IsClustered() {
		return true
	}
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
func (n *Nodes) SendQuery(node *NodeAddress, name string, parameters map[string]interface{}, callback func(interface{})) error {
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
		return fmt.Errorf("json: %w", err)
	}

	// Send node request
	if node.url == "" {
		log.Fatalf("uninitialized node address provided to SendQuery %s", node.id)
	}
	url := node.url + "query"
	ctx := context.Background()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(rawRequest))
	req.Header.Set("Content-Type", contentType)
	res, err := n.HTTPClient.Do(req)
	if err != nil {
		log.Debugf("error sending query (%s) to node (%s): %s", name, node, err.Error())
		return fmt.Errorf("httpclient: %w", err)
	}

	// Read response data
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	var responseData interface{}
	if err := decoder.Decode(&responseData); err != nil {
		// Parsing response failed
		log.Tracef("%s", err.Error())
		return fmt.Errorf("decoder.Decode: %w", err)
	}

	// Abort on error
	if res.StatusCode != http.StatusOK {
		var err error
		m, _ := responseData.(map[string]interface{})
		if v, ok := m["error"]; ok {
			err = fmt.Errorf("%s", v.(string))
		} else {
			err = fmt.Errorf("node request failed: %s (code %d)", name, res.StatusCode)
		}
		log.Tracef("%s", err.Error())
		return err
	}

	// Trigger callback
	go func() {
		defer n.lmd.logPanicExit()
		log.Tracef("calling callback for query (%s)", name)
		callback(responseData)
	}()

	return nil
}

func generateUUID() (uuid string) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		fmt.Println("Error: ", err)
		return
	}

	uuid = fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])

	return
}

func (n *Nodes) sendPing(node *NodeAddress, initializing bool, requestData map[string]interface{}) (isOnline bool, forceRedistribute bool) {
	done := make(chan bool)
	ownIdentifier := n.ID
	err := n.SendQuery(node, "ping", requestData, func(responseData interface{}) {
		defer func() { done <- true }()
		// Parse response
		dataMap, ok := responseData.(map[string]interface{})
		log.Tracef("got response from %s", node)
		if !ok {
			return
		}

		// Node id
		responseIdentifier := dataMap["identifier"].(string)
		if node.id == "" {
			node.id = responseIdentifier
		} else if node.id != responseIdentifier {
			log.Infof("partner node %s restarted", node)
			delete(n.nodeBackends, node.id)
			node.id = responseIdentifier
			forceRedistribute = true
		}

		// check version
		versionMismatch := false
		if _, exists := dataMap["version"]; !exists {
			versionMismatch = true
		} else {
			v := dataMap["version"].(string)
			if v != Version() {
				versionMismatch = true
			}
		}
		if versionMismatch {
			log.Debugf("version mismatch with node %s, deactivating", node)
			forceRedistribute = true
			delete(n.nodeBackends, node.id)
		}

		// Check whose response it is
		if responseIdentifier == ownIdentifier {
			// This node
			if initializing {
				n.thisNode = node
				node.isMe = true
				isOnline = true
				log.Debugf("identified this node as %s", node)
			}
		} else if !versionMismatch {
			// Partner node
			isOnline = true
			log.Tracef("discovered partner node: %s", node)

			// receive and update remote peer list
			if _, exists := dataMap["peers"]; exists && dataMap["peers"] != nil {
				peers := dataMap["peers"].([]interface{})
				nodeList := []string{}
				for _, id := range peers {
					nodeList = append(nodeList, id.(string))
				}
				n.nodeBackends[node.id] = nodeList
			}
		}
	})
	if err != nil {
		log.Debugf("node sendquery failed: %e", err)
	}

	<-done
	return
}
