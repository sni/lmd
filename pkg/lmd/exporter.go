package lmd

import (
	"archive/tar"
	"compress/gzip"
	"context"
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
	lmd        *Daemon
}

// export peer data to tarball containing json files.
func exportData(lmd *Daemon) (err error) {
	file := lmd.flags.flagExport
	localConfig := lmd.finalFlagsConfig(true)
	lmd.Config = localConfig
	log.Infof("starting export to %s", file)

	if len(localConfig.Connections) == 0 {
		return fmt.Errorf("no connections defined")
	}

	ex := &Exporter{
		lmd: lmd,
	}
	err = ex.Export(file)

	return err
}

func (ex *Exporter) Export(file string) (err error) {
	ex.initPeers(context.TODO())

	userinfo, err := user.Current()
	if err != nil {
		return fmt.Errorf("failed to fetch user info: %s", err.Error())
	}
	ex.user = userinfo

	groupinfo, err := user.LookupGroupId(userinfo.Gid)
	if err != nil {
		return fmt.Errorf("failed to fetch group info: %s", err.Error())
	}
	ex.group = groupinfo

	tarball, err := os.Create(file)
	if err != nil {
		return fmt.Errorf("failed to create tarball: %s", err.Error())
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

	return nil
}

func (ex *Exporter) exportPeers() (err error) {
	err = ex.addDir("sites/")
	if err != nil {
		return err
	}
	for _, peer := range ex.lmd.peerMap.Peers() {
		if peer.hasFlag(MultiBackend) {
			continue
		}
		log.Debugf("exporting %s (%s)", peer.Name, peer.ID)
		err = ex.addDir(fmt.Sprintf("sites/%s/", peer.ID))
		if err != nil {
			return err
		}
		var total int64
		var written int64
		written, err = ex.addTable(peer, Objects.Tables[TableSites])
		if err != nil {
			return err
		}
		total += written
		for _, table := range Objects.Tables {
			switch {
			case table.passthroughOnly:
				continue
			case table.name == TableBackends:
				continue
			case table.name == TableSites:
				continue
			case table.name == TableColumns:
				continue
			case table.virtual != nil:
				continue
			default:
				written, err = ex.addTable(peer, table)
				if err != nil {
					return err
				}
				total += written
			}
		}
		log.Infof("exported %10s (%5s), used space: %8d kb", peer.Name, peer.ID, total/1024)
	}

	return nil
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

	return err
}

func (ex *Exporter) addTable(peer *Peer, table *Table) (written int64, err error) {
	logWith(peer).Debugf("exporting table: %s", table.name.String())
	req := &Request{
		Table:           table.name,
		Columns:         ex.exportableColumns(peer, table),
		ColumnsHeaders:  true,
		ResponseFixed16: true,
		OutputFormat:    OutputFormatJSON,
		Backends:        []string{peer.ID},
		lmd:             ex.lmd,
	}
	err = req.ExpandRequestedBackends()
	if err != nil {
		return 0, err
	}
	req.SetRequestColumns()
	res, _, err := NewResponse(context.TODO(), req, nil)
	if err != nil {
		return 0, err
	}

	buf, err := res.Buffer()
	if err != nil {
		return 0, err
	}

	header := &tar.Header{
		Name:    fmt.Sprintf("sites/%s/%s.json", peer.ID, table.name.String()),
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
		return 0, fmt.Errorf("tar error: %s", err.Error())
	}
	written, err = io.Copy(ex.tar, buf)
	if err != nil {
		return 0, fmt.Errorf("io error: %s", err.Error())
	}

	return written, nil
}

func (ex *Exporter) initPeers(ctx context.Context) {
	log.Debugf("starting peers")
	waitGroupPeers := &sync.WaitGroup{}
	shutdownChannel := make(chan bool)
	defer close(shutdownChannel)
	ex.lmd.nodeAccessor = NewNodes(ex.lmd, []string{}, "")

	for i := range ex.lmd.Config.Connections {
		c := ex.lmd.Config.Connections[i]
		peer := NewPeer(ex.lmd, &c)
		log.Debugf("creating peer: %s", peer.Name)
		ex.lmd.peerMap.Add(peer)
		waitGroupPeers.Add(1)
		go func() {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)
			err := peer.initAllTables(ctx)
			if err != nil {
				logWith(peer).Warnf("failed to initialize peer: %s", err)
			}
			logWith(peer).Debugf("peer ready")
			waitGroupPeers.Done()
		}()
	}

	log.Infof("waiting for all peers to connect and initialize")
	waitGroupPeers.Wait()

	hasSubPeers := false
	for _, peer := range ex.lmd.peerMap.Peers() {
		if peer.parentID == "" {
			continue
		}
		hasSubPeers = true
		waitGroupPeers.Add(1)
		go func() {
			// make sure we log panics properly
			defer logPanicExitPeer(peer)
			err := peer.initAllTables(ctx)
			if err != nil {
				logWith(peer).Warnf("failed to initialize peer: %s", err)
			}
			logWith(peer).Debugf("peer ready")
			waitGroupPeers.Done()
		}()
	}

	if hasSubPeers {
		log.Infof("waiting for all federated peers to connect and initialize")
	}
	waitGroupPeers.Wait()
	log.Infof("all peers ready for export")
}

// exportableColumns generates list of columns to export.
func (ex *Exporter) exportableColumns(p *Peer, t *Table) (columns []string) {
	for _, col := range t.columns {
		if ex.isExportColumn(p, col) {
			columns = append(columns, col.Name)
		}
	}

	return columns
}

// isExportColumn returns true if column should be exported.
func (ex *Exporter) isExportColumn(p *Peer, col *Column) bool {
	if col.Optional != NoFlags && !p.hasFlag(col.Optional) {
		return false
	}
	if col.Table.name == TableSites {
		return true
	}
	if col.Table.name == TableStatus {
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
