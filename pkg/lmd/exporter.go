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
func exportData(lmd *Daemon) (exportedCount int, err error) {
	file := lmd.flags.flagExport
	localConfig := lmd.finalFlagsConfig(true)
	lmd.Config = localConfig
	log.Infof("starting export to %s", file)

	if len(localConfig.Connections) == 0 {
		return 0, fmt.Errorf("no connections defined")
	}

	ex := &Exporter{
		lmd: lmd,
	}
	exportedCount, err = ex.Export(file)

	return exportedCount, err
}

func (ex *Exporter) Export(file string) (exportedCount int, err error) {
	ex.initPeers(context.TODO())

	userinfo, err := user.Current()
	if err != nil {
		return 0, fmt.Errorf("failed to fetch user info: %s", err.Error())
	}
	ex.user = userinfo

	groupinfo, err := user.LookupGroupId(userinfo.Gid)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch group info: %s", err.Error())
	}
	ex.group = groupinfo

	tarball, err := os.Create(file)
	if err != nil {
		return 0, fmt.Errorf("failed to create tarball: %s", err.Error())
	}
	defer tarball.Close()

	gzipWriter := gzip.NewWriter(tarball)
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	ex.tar = tarWriter
	ex.exportTime = time.Now()

	exportedCount, err = ex.exportPeers()
	if err != nil {
		return 0, err
	}

	return exportedCount, nil
}

func (ex *Exporter) exportPeers() (exportedCount int, err error) {
	err = ex.addDir("sites/")
	if err != nil {
		return 0, err
	}
	exportedCount = 0
	for _, peer := range ex.lmd.peerMap.Peers() {
		if peer.hasFlag(MultiBackend) {
			continue
		}
		log.Debugf("exporting %s (%s)", peer.Name, peer.ID)
		err = ex.addDir(fmt.Sprintf("sites/%s/", peer.ID))
		if err != nil {
			return 0, err
		}
		var total int64
		var written int64
		written, err = ex.addTable(peer, Objects.Tables[TableSites])
		if err != nil {
			return 0, err
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
					return 0, err
				}
				total += written
			}
		}
		log.Infof("exported %10s (%5s), used space: %8d kb", peer.Name, peer.ID, total/1024)
		exportedCount++
	}

	return exportedCount, nil
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
	req.ExpandRequestedBackends()
	req.SetRequestColumns()
	res, _, _, err := NewResponse(context.TODO(), req, nil)
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
	ex.lmd.nodeAccessor = NewNodes(ex.lmd, []string{}, "")

	for i := range ex.lmd.Config.Connections {
		c := ex.lmd.Config.Connections[i]
		peer := NewPeer(ex.lmd, &c)
		log.Debugf("creating peer: %s", peer.Name)
		ex.lmd.peerMap.Add(peer)
		peer.Start(ctx)
	}

	log.Infof("waiting for all peers to connect and initialize")
	ex.waitForPeerMapReady()
	log.Infof("all peers ready for export (%d peers)", len(ex.lmd.peerMap.Peers()))
}

func (ex *Exporter) waitForPeerMapReady() {
	maxAttempts := 30
	minimumSleepTime := 10 * time.Second
	prevCount := 0
	// peer count should not change from previous run, and all peers should have saved data
	// this increments the stable count, anything else resets it back to zero
	stableAttempts := 0
	targetStableAttempts := 3

	sleepTime := time.Duration(ex.lmd.Config.UpdateInterval) * time.Second
	sleepTime = max(sleepTime, minimumSleepTime)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		time.Sleep(sleepTime)

		peers := ex.lmd.peerMap.Peers()
		currentCount := len(peers)
		allReady := true

		for _, peer := range peers {
			if peer.hasFlag(MultiBackend) {
				continue
			}
			if peer.data.Load() == nil {
				allReady = false

				break
			}
			if peer.peerState.Get() != PeerStatusUp {
				allReady = false

				break
			}
		}

		if currentCount == prevCount && allReady {
			stableAttempts++
			if stableAttempts >= targetStableAttempts {
				return
			}
		} else {
			prevCount = currentCount
			stableAttempts = 0
		}

		readyCount := 0
		for _, p := range peers {
			if p.data.Load() != nil {
				readyCount++
			}
		}
		log.Debugf("peer discovery attempt %d: %d peers (%d ready, %d stable)", attempt, currentCount, readyCount, stableAttempts)
	}

	log.Warnf("peer discovery did not fully stabilize after %d attempts, proceeding with %d peers", maxAttempts, len(ex.lmd.peerMap.Peers()))
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
