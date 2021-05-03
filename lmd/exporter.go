package main

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/user"
	"strings"
	"sync"
	"time"
)

type Exporter struct {
	user       *user.User
	group      *user.Group
	tar        *tar.Writer
	exportTime time.Time
	config     *Config
}

// export peer data to tarball containing json files
func exportData(file string) (err error) {
	localConfig := finalFlagsConfig(true)
	log.Infof("starting export to %s", file)

	if len(localConfig.Connections) == 0 {
		return fmt.Errorf("no connections defined")
	}

	ex := &Exporter{}
	err = ex.Export(file, localConfig)

	return
}

func (ex *Exporter) Export(file string, localConfig *Config) (err error) {
	ex.config = localConfig
	ex.initPeers(ex.config)

	userinfo, err := user.Current()
	if err != nil {
		return fmt.Errorf("failed to fetch user info: %s", err)
	}
	ex.user = userinfo

	groupinfo, err := user.LookupGroupId(userinfo.Gid)
	if err != nil {
		return fmt.Errorf("failed to fetch group info: %s", err)
	}
	ex.group = groupinfo

	tarball, err := os.Create(file)
	if err != nil {
		return fmt.Errorf("failed to create tarball: %s", err)
	}
	defer tarball.Close()

	gzipWriter := gzip.NewWriter(tarball)
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	ex.tar = tarWriter
	ex.exportTime = time.Now()

	err = ex.exportPeers()
	if err != nil {
		return err
	}

	return
}

func (ex *Exporter) exportPeers() (err error) {
	err = ex.addDir("sites/")
	if err != nil {
		return
	}
	PeerMapLock.RLock()
	defer PeerMapLock.RUnlock()
	for _, id := range PeerMapOrder {
		p := PeerMap[id]
		if p.HasFlag(MultiBackend) {
			continue
		}
		log.Debugf("exporting %s (%s)", p.Name, p.ID)
		err = ex.addDir(fmt.Sprintf("sites/%s/", p.ID))
		if err != nil {
			return
		}
		total := int64(0)
		written := int64(0)
		written, err = ex.addTable(p, Objects.Tables[TableSites])
		if err != nil {
			return
		}
		total += written
		for _, t := range Objects.Tables {
			switch {
			case t.PassthroughOnly:
				continue
			case t.Name == TableBackends:
				continue
			case t.Name == TableSites:
				continue
			case t.Name == TableColumns:
				continue
			case t.Virtual != nil:
				continue
			default:
				written, err = ex.addTable(p, t)
				if err != nil {
					return
				}
				total += written
			}
		}
		log.Infof("exported %10s (%5s), used space: %8d kb", p.Name, p.ID, total/1024)
	}
	return
}

func (ex *Exporter) addDir(name string) (err error) {
	err = ex.tar.WriteHeader(&tar.Header{
		Name:    name,
		Mode:    DefaultDirPerm,
		ModTime: ex.exportTime,
		Uid:     interface2int(ex.user.Uid),
		Uname:   ex.user.Username,
		Gid:     interface2int(ex.user.Gid),
		Gname:   ex.group.Name,
	})
	return
}

func (ex *Exporter) addTable(p *Peer, t *Table) (written int64, err error) {
	logWith(p).Debugf("exporting table: %s", t.Name.String())
	req := &Request{
		Table:           t.Name,
		Columns:         ex.exportableColumns(p, t),
		ColumnsHeaders:  true,
		ResponseFixed16: true,
		OutputFormat:    OutputFormatJSON,
		Backends:        []string{p.ID},
	}
	err = req.ExpandRequestedBackends()
	if err != nil {
		return
	}
	req.SetRequestColumns()
	res, err := NewResponse(req)
	if err != nil {
		return
	}

	buf, err := res.Buffer()
	if err != nil {
		return
	}

	header := &tar.Header{
		Name:    fmt.Sprintf("sites/%s/%s.json", p.ID, t.Name.String()),
		Size:    int64(buf.Len()),
		Mode:    DefaultFilePerm,
		ModTime: ex.exportTime,
		Uid:     interface2int(ex.user.Uid),
		Uname:   ex.user.Username,
		Gid:     interface2int(ex.user.Gid),
		Gname:   ex.group.Name,
	}

	err = ex.tar.WriteHeader(header)
	if err != nil {
		return
	}
	written, err = io.Copy(ex.tar, buf)
	if err != nil {
		return
	}
	return
}

func (ex *Exporter) initPeers(localConfig *Config) {
	log.Debugf("starting peers")
	waitGroupPeers := &sync.WaitGroup{}
	shutdownChannel := make(chan bool)
	defer close(shutdownChannel)
	nodeAccessor = NewNodes(localConfig, []string{}, "", waitGroupPeers, shutdownChannel)

	for i := range localConfig.Connections {
		c := localConfig.Connections[i]
		p := NewPeer(localConfig, &c, nil, shutdownChannel)
		log.Debugf("creating peer: %s", p.Name)
		PeerMapLock.Lock()
		PeerMap[p.ID] = p
		PeerMapOrder = append(PeerMapOrder, p.ID)
		PeerMapLock.Unlock()
		waitGroupPeers.Add(1)
		go func() {
			// make sure we log panics properly
			defer logPanicExitPeer(p)
			err := p.InitAllTables()
			if err != nil {
				logWith(p).Warnf("failed to initialize peer: %s", err)
			}
			logWith(p).Debugf("peer ready")
			waitGroupPeers.Done()
		}()
	}

	log.Infof("waiting for all peers to connect and initialize")
	waitGroupPeers.Wait()

	PeerMapLock.RLock()
	defer PeerMapLock.RUnlock()
	hasSubPeers := false
	for _, id := range PeerMapOrder {
		p := PeerMap[id]
		if p.ParentID == "" {
			continue
		}
		hasSubPeers = true
		waitGroupPeers.Add(1)
		go func() {
			// make sure we log panics properly
			defer logPanicExitPeer(p)
			err := p.InitAllTables()
			if err != nil {
				logWith(p).Warnf("failed to initialize peer: %s", err)
			}
			logWith(p).Debugf("peer ready")
			waitGroupPeers.Done()
		}()
	}

	if hasSubPeers {
		log.Infof("waiting for all federated peers to connect and initialize")
	}
	waitGroupPeers.Wait()
	log.Infof("all peers ready for export")
}

// exportableColumns generates list of columns to export
func (ex *Exporter) exportableColumns(p *Peer, t *Table) (columns []string) {
	for _, col := range t.Columns {
		if ex.isExportColumn(p, col) {
			columns = append(columns, col.Name)
		}
	}
	return
}

// isExportColumn returns true if column should be exported
func (ex *Exporter) isExportColumn(p *Peer, col *Column) bool {
	if col.Optional != NoFlags && !p.HasFlag(col.Optional) {
		return false
	}
	if col.Table.Name == TableSites {
		return true
	}
	if col.Table.Name == TableStatus {
		return true
	}
	if col.StorageType == RefStore {
		return false
	}
	if col.StorageType == VirtualStore {
		return false
	}
	if strings.HasSuffix(col.Name, "_lc") {
		return false
	}
	if col.Name == "custom_variables" {
		return false
	}
	return true
}
