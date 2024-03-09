package lmd

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
)

var reImportFileTable = regexp.MustCompile(`/([a-z]+)\.json$`)

func initializePeersWithImport(lmd *Daemon, importFile string) (err error) {
	stat, err := os.Stat(importFile)
	if err != nil {
		return fmt.Errorf("cannot read %s: %s", importFile, err.Error())
	}

	lmd.PeerMapLock.RLock()
	peerSize := len(lmd.PeerMapOrder)
	lmd.PeerMapLock.RUnlock()
	if peerSize > 0 {
		log.Warnf("reload from import file is not possible")

		return nil
	}

	var peers []*Peer
	switch mode := stat.Mode(); {
	case mode.IsDir():
		peers, err = importPeersFromDir(lmd, importFile)
	case mode.IsRegular():
		peers, err = importPeersFromTar(lmd, importFile)
	}
	if err != nil {
		return fmt.Errorf("import failed: %s", err.Error())
	}

	PeerMapNew := make(map[string]*Peer)
	PeerMapOrderNew := make([]string, 0)
	for i := range peers {
		peer := peers[i]

		// finish peer import
		err = peer.data.SetReferences()
		if err != nil {
			return fmt.Errorf("failed to set references: %s", err.Error())
		}

		PeerMapNew[peer.ID] = peer
		PeerMapOrderNew = append(PeerMapOrderNew, peer.ID)
	}

	lmd.PeerMapLock.Lock()
	lmd.PeerMapOrder = PeerMapOrderNew
	lmd.PeerMap = PeerMapNew
	lmd.PeerMapLock.Unlock()

	if len(PeerMapOrderNew) == 0 {
		return fmt.Errorf("failed to find any useable data")
	}

	log.Infof("imported %d peers successfully", len(PeerMapOrderNew))

	lmd.nodeAccessor = NewNodes(lmd, []string{}, "")

	return nil
}

// importPeersFromDir imports all peers recursively for given folder.
func importPeersFromDir(lmd *Daemon, folder string) (peers []*Peer, err error) {
	folder = strings.TrimRight(folder, "/")
	files, err := os.ReadDir(folder)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %s", folder, err.Error())
	}
	if _, err := os.Stat(folder + "/backends.json"); err == nil {
		return importPeerFromDir(peers, folder, lmd)
	}
	for _, f := range files {
		if f.IsDir() {
			peer, err := importPeersFromDir(lmd, fmt.Sprintf("%s/%s", folder, f.Name()))
			if err != nil {
				return nil, err
			}
			peers = append(peers, peer...)
		}
	}

	return
}

// importPeerFromDir imports single peer from folder which must contain all required json files.
func importPeerFromDir(peers []*Peer, folder string, lmd *Daemon) ([]*Peer, error) {
	folder = strings.TrimRight(folder, "/")
	files, err := os.ReadDir(folder)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %s", folder, err.Error())
	}
	peers, err = importPeerFromFile(peers, folder+"/backends.json", lmd)
	if err != nil {
		return nil, fmt.Errorf("import error in file %s: %s", folder+"/backends.json", err.Error())
	}
	peers, err = importPeerFromFile(peers, folder+"/status.json", lmd)
	if err != nil {
		return nil, fmt.Errorf("import error in file %s: %s", folder+"/status.json", err.Error())
	}
	for _, file := range files {
		fInfo, err := file.Info()
		if err != nil {
			return nil, fmt.Errorf("import error in file %s: %s", file.Name(), err.Error())
		}
		if fInfo.Mode().IsRegular() {
			if strings.Contains(file.Name(), "backends.json") {
				continue
			}
			if strings.Contains(file.Name(), "status.json") {
				continue
			}
			peers, err = importPeerFromFile(peers, folder+"/"+file.Name(), lmd)
			if err != nil {
				return nil, fmt.Errorf("import error in file %s: %s", folder+"/"+file.Name(), err.Error())
			}
		}
	}

	return peers, nil
}

// importPeerFromFile imports next file.
func importPeerFromFile(peers []*Peer, filename string, lmd *Daemon) ([]*Peer, error) {
	log.Debugf("reading %s", filename)
	matches := reImportFileTable.FindStringSubmatch(filename)
	if len(matches) != 2 {
		log.Warnf("no idea what to do with file: %s", filename)
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("read error: %s", err.Error())
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("read error: %s", err.Error())
	}
	table, rows, columns, err := importReadFile(matches[1], file, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("import error: %s", err.Error())
	}
	log.Debugf("adding %d %s rows", len(rows), table.Name.String())

	return importData(peers, table, rows, columns, lmd)
}

// importPeersFromTar imports all peers from tarball.
func importPeersFromTar(lmd *Daemon, tarFile string) (peers []*Peer, err error) {
	file, err := os.Open(tarFile)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %s", tarFile, err.Error())
	}
	defer file.Close()

	gzf, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("gzip error %s: %s", tarFile, err.Error())
	}

	tarReader := tar.NewReader(gzf)
	for {
		header, err := tarReader.Next()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("gzip/tarball error %s: %s", tarFile, err.Error())
		}

		switch header.Typeflag {
		case tar.TypeDir:
		case tar.TypeReg:
			peers, err = importPeerFromTar(peers, header, tarReader, lmd)
			if err != nil {
				return nil, fmt.Errorf("gzip/tarball error %s in file %s: %s", tarFile, header.Name, err.Error())
			}
		default:
			return nil, fmt.Errorf("gzip/tarball error %s: unsupported type in file %s: %c", tarFile, header.Name, header.Typeflag)
		}
	}

	return peers, nil
}

// importPeerFromTar imports next file from tarball.
func importPeerFromTar(peers []*Peer, header *tar.Header, tarReader io.Reader, lmd *Daemon) ([]*Peer, error) {
	filename := header.Name
	log.Debugf("reading %s", filename)
	matches := reImportFileTable.FindStringSubmatch(filename)
	if len(matches) != 2 {
		return nil, fmt.Errorf("no idea what to do with file: %s", filename)
	}
	table, rows, columns, err := importReadFile(matches[1], tarReader, header.Size)
	if err != nil {
		return nil, fmt.Errorf("import error: %s", err.Error())
	}

	return importData(peers, table, rows, columns, lmd)
}

// importData creates/extends peer from given table data.
func importData(peers []*Peer, table *Table, rows ResultSet, columns []string, lmd *Daemon) ([]*Peer, error) {
	var peer *Peer
	if len(peers) > 0 {
		peer = peers[len(peers)-1]
	}

	colIndex := make(map[string]int)
	for i, col := range columns {
		colIndex[col] = i
	}

	if table.Name == TableBackends {
		if len(rows) != 1 {
			return peers, fmt.Errorf("wrong number of site rows, expected 1 but got: %d", len(rows))
		}

		// new peer export starting
		con := &Connection{
			Name:    interface2stringNoDedup(rows[0][colIndex["peer_name"]]),
			ID:      interface2stringNoDedup(rows[0][colIndex["peer_key"]]),
			Source:  []string{interface2stringNoDedup(rows[0][colIndex["addr"]])},
			Section: interface2stringNoDedup(rows[0][colIndex["section"]]),
			Flags:   interface2stringlist(rows[0][colIndex["flags"]]),
		}
		peer = NewPeer(lmd, con)
		peers = append(peers, peer)
		logWith(peer).Infof("restoring peer id %s", peer.ID)

		peer.PeerState = PeerStatus(interface2int(rows[0][colIndex["status"]]))
		peer.LastUpdate = interface2float64(rows[0][colIndex["last_update"]])
		peer.LastError = interface2stringNoDedup(rows[0][colIndex["last_error"]])
		peer.LastOnline = interface2float64(rows[0][colIndex["last_online"]])
		peer.Queries = interface2int64(rows[0][colIndex["queries"]])
		peer.ResponseTime = interface2float64(rows[0][colIndex["response_time"]])
		peer.data = NewDataStoreSet(peer)

		flags := NoFlags
		flags.Load(con.Flags)
		atomic.StoreUint32(&peer.Flags, uint32(flags))
	}
	if table.Virtual != nil {
		return peers, nil
	}

	if peer != nil && peer.isOnline() {
		store := NewDataStore(table, peer)
		store.DataSet = peer.data
		columnsList := ColumnList{}
		for _, name := range columns {
			col := store.GetColumn(name)
			if col == nil {
				return peers, fmt.Errorf("unknown column: %s", name)
			}
			if col.Index < 0 && col.StorageType == LocalStore {
				return peers, fmt.Errorf("bad column: %s in table %s", name, table.Name.String())
			}
			columnsList = append(columnsList, col)
		}

		err := store.InsertData(rows, columnsList, false)
		if err != nil {
			return peers, fmt.Errorf("failed to insert data: %s", err.Error())
		}
		peer.data.Set(table.Name, store)
	}

	return peers, nil
}

// importReadFile returns table, data and columns from json file.
func importReadFile(tableName string, tarReader io.Reader, size int64) (table *Table, rows ResultSet, columns []string, err error) {
	for _, t := range Objects.Tables {
		if strings.EqualFold(t.Name.String(), tableName) {
			table = t
		}
	}
	if table == nil {
		return nil, nil, nil, fmt.Errorf("no table found by name: %s", tableName)
	}
	data := make([]byte, 0, size)
	buf := bytes.NewBuffer(data)
	read, err := io.Copy(buf, tarReader)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, nil, nil, fmt.Errorf("read error: %s", err.Error())
	}
	if read != size {
		return nil, nil, nil, fmt.Errorf("expected size %d but got %d", size, read)
	}
	rows, err = NewResultSet(buf.Bytes())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse error: %s", err.Error())
	}
	if len(rows) == 0 {
		return nil, nil, nil, fmt.Errorf("missing column header")
	}
	columns = []string{}
	columnRow := rows[0]
	rows = rows[1:]
	for i := range columnRow {
		col := interface2stringNoDedup(columnRow[i])
		columns = append(columns, col)
	}

	return table, rows, columns, nil
}
