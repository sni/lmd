package lmd

// PeerStatusKey contains the different keys for the Peer.Status map.
type PeerStatusKey int32

// available keys for the peer status map.
const (
	_ PeerStatusKey = iota
	PeerKey
	PeerName
	PeerState
	PeerAddr
	CurPeerAddrNum
	LastUpdate
	LastFullUpdate
	LastFullHostUpdate
	LastFullServiceUpdate
	LastTimeperiodUpdateMinute
	LastQuery
	LastError
	LastOnline
	LastPid
	ProgramStart
	BytesSend
	BytesReceived
	Queries
	ResponseTime
	Idling
	Paused
	Section
	PeerParent
	ThrukVersion
	SubKey
	SubName
	SubAddr
	SubType
	SubPeerStatus
	ConfigTool
	ThrukExtras
	ForceFull
	LastHTTPRequestSuccessful
)

// statusSetLocked updates a peer status entry and takes care about the locking.
func (p *Peer) statusSetLocked(key PeerStatusKey, value interface{}) {
	p.lock.Lock()
	p.statusSet(key, value)
	p.lock.Unlock()
}

// statusGetLocked returns peer status entry and takes care about the locking.
func (p *Peer) statusGetLocked(key PeerStatusKey) interface{} {
	p.lock.RLock()
	value := p.statusGet(key)
	p.lock.RUnlock()

	return value
}

// statusGet retrieves peer status entry.
func (p *Peer) statusGet(key PeerStatusKey) interface{} {
	switch key {
	case PeerKey:
		return p.ID
	case PeerName:
		return p.Name
	case PeerState:
		return p.PeerState
	case PeerAddr:
		return p.PeerAddr
	case CurPeerAddrNum:
		return p.CurPeerAddrNum
	case LastUpdate:
		return p.LastUpdate
	case LastFullUpdate:
		return p.LastFullUpdate
	case LastFullHostUpdate:
		return p.LastFullHostUpdate
	case LastFullServiceUpdate:
		return p.LastFullServiceUpdate
	case LastTimeperiodUpdateMinute:
		return p.LastTimeperiodUpdateMinute
	case LastQuery:
		return p.LastQuery
	case LastError:
		return p.LastError
	case LastOnline:
		return p.LastOnline
	case LastPid:
		return p.LastPid
	case ProgramStart:
		return p.ProgramStart
	case BytesSend:
		return p.BytesSend
	case BytesReceived:
		return p.BytesReceived
	case Queries:
		return p.Queries
	case ResponseTime:
		return p.ResponseTime
	case Idling:
		return p.Idling
	case Paused:
		return p.Paused
	case Section:
		return p.Section
	case PeerParent:
		return p.PeerParent
	case ThrukVersion:
		return p.ThrukVersion
	case SubKey:
		return p.SubKey
	case SubName:
		return p.SubName
	case SubAddr:
		return p.SubAddr
	case SubType:
		return p.SubType
	case SubPeerStatus:
		return p.SubPeerStatus
	case ConfigTool:
		return p.ConfigTool
	case ThrukExtras:
		return p.ThrukExtras
	case ForceFull:
		return p.ForceFull
	case LastHTTPRequestSuccessful:
		return p.LastHTTPRequestSuccessful
	}

	log.Panicf("unknown peer status key: %#v", key)

	return nil
}

// statusSet updates a status map.
func (p *Peer) statusSet(key PeerStatusKey, value interface{}) {
	switch key {
	case PeerKey:
		p.ID = *interface2string(value)
	case PeerName:
		p.Name = *interface2string(value)
	case PeerState:
		if v, ok := value.(PeerStatus); ok {
			p.PeerState = v
		} else {
			log.Panicf("got unknown peer state: %#v", value)
		}
	case PeerAddr:
		p.PeerAddr = *interface2string(value)
	case CurPeerAddrNum:
		p.CurPeerAddrNum = interface2int(value)
	case LastUpdate:
		p.LastUpdate = interface2float64(value)
	case LastFullUpdate:
		p.LastFullUpdate = interface2float64(value)
	case LastFullHostUpdate:
		p.LastFullHostUpdate = interface2float64(value)
	case LastFullServiceUpdate:
		p.LastFullServiceUpdate = interface2float64(value)
	case LastTimeperiodUpdateMinute:
		p.LastTimeperiodUpdateMinute = interface2int(value)
	case LastQuery:
		p.LastQuery = interface2float64(value)
	case LastError:
		p.LastError = *interface2string(value)
	case LastOnline:
		p.LastOnline = interface2float64(value)
	case LastPid:
		p.LastPid = interface2int(value)
	case ProgramStart:
		p.ProgramStart = interface2int64(value)
	case BytesSend:
		p.BytesSend = interface2int64(value)
	case BytesReceived:
		p.BytesReceived = interface2int64(value)
	case Queries:
		p.Queries = interface2int64(value)
	case ResponseTime:
		p.ResponseTime = interface2float64(value)
	case Idling:
		p.Idling = interface2bool(value)
	case Paused:
		p.Paused = interface2bool(value)
	case Section:
		p.Section = *interface2string(value)
	case PeerParent:
		p.PeerParent = *interface2string(value)
	case ThrukVersion:
		p.ThrukVersion = interface2float64(value)
	case SubKey:
		p.SubKey = interface2stringlist(value)
	case SubName:
		p.SubName = interface2stringlist(value)
	case SubAddr:
		p.SubAddr = interface2stringlist(value)
	case SubType:
		p.SubType = interface2stringlist(value)
	case SubPeerStatus:
		p.SubPeerStatus = interface2mapinterface(value)
	case ConfigTool:
		p.ConfigTool = interface2string(value)
	case ThrukExtras:
		p.ThrukExtras = interface2string(value)
	case ForceFull:
		p.ForceFull = interface2bool(value)
	case LastHTTPRequestSuccessful:
		p.LastHTTPRequestSuccessful = interface2bool(value)
	default:
		log.Panicf("unknown peer status key: %#v", key)
	}
}
