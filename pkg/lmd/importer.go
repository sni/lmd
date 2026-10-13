package lmd

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
)

func initializePeersWithImport(lmd *Daemon, importFile string) (err error) {
	stat, err := os.Stat(importFile)
	if err != nil {
		return fmt.Errorf("cannot read %s: %s", importFile, err.Error())
	}

	peerSize := len(lmd.peerMap.Peers())
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

	newPeers := make([]*Peer, 0, len(peers))
	for i := range peers {
		peer := peers[i]

		// finish peer import
		data := peer.data.Load()
		err = data.setReferences()
		if err != nil {
			return fmt.Errorf("failed to set references: %s", err.Error())
		}

		newPeers = append(newPeers, peer)
	}

	lmd.peerMap.Add(newPeers...)

	if len(newPeers) == 0 {
		return fmt.Errorf("failed to find any useable data")
	}

	log.Infof("imported %d peers successfully", len(newPeers))

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

	return peers, nil
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

var reImportFileTable = regexp.MustCompile(`/([a-z]+)\.json$`)

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
	log.Debugf("adding %d %s rows", len(rows), table.name.String())

	return importData(peers, table, rows, columns, lmd)
}

// scanImportTar calls fn callback for every folder and regular file in the tarball.
func scanImportTar(tarFile string, callbackFn func(header *tar.Header, tarReader io.Reader) error) error {
	file, err := os.Open(tarFile)
	if err != nil {
		return fmt.Errorf("cannot read %s: %s", tarFile, err.Error())
	}
	defer file.Close()

	gzf, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("gzip error %s: %s", tarFile, err.Error())
	}

	tarReader := tar.NewReader(gzf)
	for {
		header, err := tarReader.Next()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fmt.Errorf("gzip/tarball error %s: %s", tarFile, err.Error())
		}

		switch header.Typeflag {
		case tar.TypeDir, tar.TypeReg:
			if err := callbackFn(header, tarReader); err != nil {
				return fmt.Errorf("gzip/tarball error %s in entry %s: %s", tarFile, header.Name, err.Error())
			}
		default:
			return fmt.Errorf("gzip/tarball error %s: unsupported type in file %s: %c", tarFile, header.Name, header.Typeflag)
		}
	}

	return nil
}

var reImportTarFile = regexp.MustCompile(`^sites/([^/]+)/([a-z]+)\.json$`)

// importTarFileNames returns peer id and table name for a file inside an export archive.
func importTarFileNames(filename string) (peerID, table string, err error) {
	matches := reImportTarFile.FindStringSubmatch(filename)
	if len(matches) != 3 {
		return "", "", fmt.Errorf("unexpected file in export archive, expected sites/<peer_id>/<table>.json: %s", filename)
	}

	return matches[1], matches[2], nil
}

var reImportTarFolder = regexp.MustCompile(`^sites/([^/]+)/?$`)

// importTarFolderID returns the peer id for a peer folder inside an export archive.
func importTarFolderID(foldername string) (peerID string, ok bool) {
	matches := reImportTarFolder.FindStringSubmatch(foldername)
	if len(matches) != 2 {
		return "", false
	}

	return matches[1], true
}

// importPeersFromTar imports all peers from tarball.
func importPeersFromTar(lmd *Daemon, tarFile string) (peers []*Peer, err error) {
	backendsTableName := Objects.Tables[TableBackends].name.String()
	discoveredPeerFolderIDs := make(map[string]struct{})
	peersByID := make(map[string]*Peer)

	// first pass: create all peers from their backends.json
	err = scanImportTar(tarFile, func(header *tar.Header, tarReader io.Reader) error {
		if header.Typeflag == tar.TypeDir {
			if peerID, ok := importTarFolderID(header.Name); ok {
				discoveredPeerFolderIDs[peerID] = struct{}{}
			}

			return nil
		}
		peerID, tableName, err2 := importTarFileNames(header.Name)
		if err2 != nil {
			return err2
		}
		discoveredPeerFolderIDs[peerID] = struct{}{}
		if tableName != backendsTableName {
			return nil
		}
		newPeers, err2 := importPeerFromTar(nil, header, tarReader, lmd)
		if err2 != nil {
			return err2
		}
		if len(newPeers) != 1 {
			return fmt.Errorf("failed to restore peer from %s", header.Name)
		}
		if peersByID[peerID] != nil {
			return fmt.Errorf("duplicate peer id: %s", peerID)
		}
		peersByID[peerID] = newPeers[0]
		peers = append(peers, newPeers[0])

		return nil
	})
	if err != nil {
		return nil, err
	}

	for peerID := range discoveredPeerFolderIDs {
		if _, ok := peersByID[peerID]; !ok {
			return nil, fmt.Errorf("folder sites/%s/ does not contain a %s.json in %s", peerID, backendsTableName, tarFile)
		}
	}

	if len(peers) == 0 {
		return nil, fmt.Errorf("no peer found while importing, possibly missing %s.json in %s", backendsTableName, tarFile)
	}

	// second pass: import all other tables
	err = scanImportTar(tarFile, func(header *tar.Header, tarReader io.Reader) error {
		if header.Typeflag == tar.TypeDir {
			return nil
		}
		peerID, tableName, err2 := importTarFileNames(header.Name)
		if err2 != nil {
			return err2
		}
		if tableName == backendsTableName {
			return nil
		}
		peer := peersByID[peerID]
		if peer == nil {
			return fmt.Errorf("no %s.json found for peer %s", backendsTableName, peerID)
		}
		_, err2 = importPeerFromTar([]*Peer{peer}, header, tarReader, lmd)

		return err2
	})
	if err != nil {
		return nil, err
	}

	return peers, nil
}

// importPeerFromTar imports next file from tarball.
func importPeerFromTar(peers []*Peer, header *tar.Header, tarReader io.Reader, lmd *Daemon) ([]*Peer, error) {
	filename := header.Name
	log.Debugf("reading %s", filename)
	_, tableName, err := importTarFileNames(filename)
	if err != nil {
		return nil, err
	}
	table, rows, columns, err := importReadFile(tableName, tarReader, header.Size)
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

	if table.name == TableBackends {
		if len(rows) != 1 {
			return peers, fmt.Errorf("wrong number of site rows, expected 1 but got: %d", len(rows))
		}

		// new peer import starting
		con := &Connection{
			Name:    interface2stringNoDedup(rows[0][colIndex["peer_name"]]),
			ID:      interface2stringNoDedup(rows[0][colIndex["peer_key"]]),
			Source:  []string{interface2stringNoDedup(rows[0][colIndex["addr"]])},
			Section: interface2stringNoDedup(rows[0][colIndex["section"]]),
			Flags:   interface2stringListNoDedup(rows[0][colIndex["flags"]]),
		}
		peer = NewPeer(lmd, con)
		peers = append(peers, peer)
		logWith(peer).Infof("restoring peer id %s", peer.ID)

		peer.peerState.Set(PeerStatus(interface2int8(rows[0][colIndex["status"]])))
		peer.lastUpdate.Set(interface2float64(rows[0][colIndex["last_update"]]))
		peer.lastError.Set(interface2stringNoDedup(rows[0][colIndex["last_error"]]))
		peer.lastOnline.Set(interface2float64(rows[0][colIndex["last_online"]]))
		peer.queries.Store(interface2int64(rows[0][colIndex["queries"]]))
		peer.responseTime.Set(interface2float64(rows[0][colIndex["response_time"]]))
		peer.data.Store(NewDataStoreSet(peer))

		flags := NoFlags
		flags.Load(con.Flags)
		atomic.StoreUint32(&peer.flags, uint32(flags))
	}
	if table.virtual != nil {
		return peers, nil
	}

	if peer == nil {
		return peers, fmt.Errorf("missing backends.json before table %s", table.name.String())
	}

	if peer.isOnline() {
		store := NewDataStore(table, peer)
		store.dataSet = peer.data.Load()
		columnsList := ColumnList{}
		for _, name := range columns {
			col := store.GetColumn(name)
			if col == nil {
				return peers, fmt.Errorf("unknown column: %s", name)
			}
			if col.Index < 0 && col.StorageType == LocalStore {
				return peers, fmt.Errorf("bad column: %s in table %s", name, table.name.String())
			}
			columnsList = append(columnsList, col)
		}

		err := store.insertData(context.TODO(), rows, columnsList, false)
		if err != nil {
			return peers, fmt.Errorf("failed to insert data: %s", err.Error())
		}
		data := peer.data.Load()
		data.setTable(table.name, store)
	}

	return peers, nil
}

// importReadFile returns table, data and columns from json file.
func importReadFile(tableName string, tarReader io.Reader, size int64) (table *Table, rows ResultSet, columns []string, err error) {
	for _, t := range Objects.Tables {
		if strings.EqualFold(t.name.String(), tableName) {
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
