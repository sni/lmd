package main

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
)

var reImportFileTable = regexp.MustCompile(`/([a-z]+)\.json$`)

func initializePeersWithImport(lmd *LMDInstance, importFile string) (err error) {
	stat, err := os.Stat(importFile)
	if err != nil {
		return fmt.Errorf("cannot read %s: %s", importFile, err)
	}

	lmd.PeerMapLock.RLock()
	peerSize := len(lmd.PeerMapOrder)
	lmd.PeerMapLock.RUnlock()
	if peerSize > 0 {
		log.Warnf("reload from import file is not possible")
		return
	}

	var peers []*Peer
	switch mode := stat.Mode(); {
	case mode.IsDir():
		peers, err = importPeersFromDir(lmd, importFile)
	case mode.IsRegular():
		peers, err = importPeersFromTar(lmd, importFile)
	}
	if err != nil {
		return fmt.Errorf("import failed: %s", err)
	}

	PeerMapNew := make(map[string]*Peer)
	PeerMapOrderNew := make([]string, 0)
	for i := range peers {
		p := peers[i]

		// finish peer import
		err = p.data.SetReferences()
		if err != nil {
			return fmt.Errorf("failed to set references: %s", err)
		}

		PeerMapNew[p.ID] = p
		PeerMapOrderNew = append(PeerMapOrderNew, p.ID)
	}

	lmd.PeerMapLock.Lock()
	lmd.PeerMapOrder = PeerMapOrderNew
	lmd.PeerMap = PeerMapNew
	lmd.PeerMapLock.Unlock()

	log.Infof("imported %d peers successfully", len(PeerMapOrderNew))

	lmd.nodeAccessor = NewNodes(lmd, []string{}, "")
	return
}

// importPeersFromDir imports all peers recursively for given folder
func importPeersFromDir(lmd *LMDInstance, folder string) (peers []*Peer, err error) {
	folder = strings.TrimRight(folder, "/")
	files, err := os.ReadDir(folder)
	if err != nil {
		err = fmt.Errorf("cannot read %s: %s", folder, err)
		return
	}
	if _, err := os.Stat(folder + "/sites.json"); err == nil {
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

// importPeerFromDir imports single peer from folder which must contain all required json files
func importPeerFromDir(peers []*Peer, folder string, lmd *LMDInstance) ([]*Peer, error) {
	folder = strings.TrimRight(folder, "/")
	files, err := os.ReadDir(folder)
	if err != nil {
		err = fmt.Errorf("cannot read %s: %s", folder, err)
		return nil, err
	}
	peers, err = importPeerFromFile(peers, folder+"/sites.json", lmd)
	if err != nil {
		return nil, fmt.Errorf("import error in file %s: %s", folder+"/sites.json", err)
	}
	for _, f := range files {
		fInfo, err := f.Info()
		if err != nil {
			return nil, fmt.Errorf("import error in file %s: %s", f.Name(), err)
		}
		if fInfo.Mode().IsRegular() {
			if strings.Contains(f.Name(), "sites.json") {
				continue
			}
			peers, err = importPeerFromFile(peers, folder+"/"+f.Name(), lmd)
			if err != nil {
				return nil, fmt.Errorf("import error in file %s: %s", folder+"/"+f.Name(), err)
			}
		}
	}
	return peers, nil
}

// importPeerFromFile imports next file
func importPeerFromFile(peers []*Peer, filename string, lmd *LMDInstance) ([]*Peer, error) {
	log.Debugf("reading %s", filename)
	matches := reImportFileTable.FindStringSubmatch(filename)
	if len(matches) != 2 {
		log.Warnf("no idea what to do with file: %s", filename)
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("read error: %s", err)
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("read error: %s", err)
	}
	table, rows, columns, err := importReadFile(matches[1], file, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("import error: %s", err)
	}
	log.Debugf("adding %d %s rows", len(rows), table.Name.String())
	return importData(peers, table, rows, columns, lmd)
}

// importPeersFromTar imports all peers from tarball
func importPeersFromTar(lmd *LMDInstance, tarFile string) (peers []*Peer, err error) {
	f, err := os.Open(tarFile)
	if err != nil {
		err = fmt.Errorf("cannot read %s: %s", tarFile, err)
		return
	}
	defer f.Close()

	gzf, err := gzip.NewReader(f)
	if err != nil {
		err = fmt.Errorf("gzip error %s: %s", tarFile, err)
		return
	}

	tarReader := tar.NewReader(gzf)
	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("gzip/tarball error %s: %s", tarFile, err)
		}

		switch header.Typeflag {
		case tar.TypeDir:
		case tar.TypeReg:
			peers, err = importPeerFromTar(peers, header, tarReader, lmd)
			if err != nil {
				return nil, fmt.Errorf("gzip/tarball error %s in file %s: %s", tarFile, header.Name, err)
			}
		default:
			return nil, fmt.Errorf("gzip/tarball error %s: unsupported type in file %s: %c", tarFile, header.Name, header.Typeflag)
		}
	}

	return
}

// importPeerFromTar imports next file from tarball
func importPeerFromTar(peers []*Peer, header *tar.Header, tarReader io.Reader, lmd *LMDInstance) ([]*Peer, error) {
	filename := header.Name
	log.Debugf("reading %s", filename)
	matches := reImportFileTable.FindStringSubmatch(filename)
	if len(matches) != 2 {
		return nil, fmt.Errorf("no idea what to do with file: %s", filename)
	}
	table, rows, columns, err := importReadFile(matches[1], tarReader, header.Size)
	if err != nil {
		return nil, fmt.Errorf("import error: %s", err)
	}
	return importData(peers, table, rows, columns, lmd)
}

// importData creates/extends peer from given table data
func importData(peers []*Peer, table *Table, rows ResultSet, columns ColumnList, lmd *LMDInstance) ([]*Peer, error) {
	colIndex := make(map[string]int)
	for i, col := range columns {
		colIndex[col.Name] = i
	}
	var p *Peer
	if len(peers) > 0 {
		p = peers[len(peers)-1]
	}
	if table.Name == TableSites {
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
		p = NewPeer(lmd, con)
		peers = append(peers, p)
		logWith(p).Infof("restoring peer id %s", p.ID)

		p.Status[PeerState] = PeerStatus(interface2int(rows[0][colIndex["status"]]))
		p.Status[LastUpdate] = interface2float64(rows[0][colIndex["last_update"]])
		p.Status[LastError] = interface2stringNoDedup(rows[0][colIndex["last_error"]])
		p.Status[LastOnline] = interface2float64(rows[0][colIndex["last_online"]])
		p.Status[Queries] = interface2int64(rows[0][colIndex["queries"]])
		p.Status[ResponseTime] = interface2float64(rows[0][colIndex["response_time"]])
		p.data = NewDataStoreSet(p)
	}
	if table.Virtual != nil {
		return peers, nil
	}

	if p != nil && p.isOnline() {
		store := NewDataStore(table, p)
		store.DataSet = p.data

		err := store.InsertData(rows, columns, false)
		if err != nil {
			return peers, fmt.Errorf("failed to insert data: %s", err)
		}
		p.data.Set(table.Name, store)
	}
	return peers, nil
}

// importReadFile returns table, data and columns from json file
func importReadFile(tableName string, tarReader io.Reader, size int64) (table *Table, rows ResultSet, columns ColumnList, err error) {
	for _, t := range Objects.Tables {
		if strings.EqualFold(t.Name.String(), tableName) {
			table = t
		}
	}
	if table == nil {
		err = fmt.Errorf("no table found by name: %s", tableName)
		return
	}
	data := make([]byte, 0, size)
	buf := bytes.NewBuffer(data)
	read, err := io.Copy(buf, tarReader)
	if err != nil && !errors.Is(err, io.EOF) {
		err = fmt.Errorf("read error: %s", err)
		return
	}
	if read != size {
		err = fmt.Errorf("expected size %d but got %d", size, read)
		return
	}
	rows, err = NewResultSet(buf.Bytes())
	if err != nil {
		err = fmt.Errorf("parse error: %s", err)
		return
	}
	if len(rows) == 0 {
		err = fmt.Errorf("missing column header")
		return
	}
	columnRow := rows[0]
	rows = rows[1:]
	for i := range columnRow {
		col := table.GetColumn(interface2stringNoDedup(columnRow[i]))
		if col == nil {
			err = fmt.Errorf("no column found by name: %s", columnRow[i])
			return
		}
		columns = append(columns, col)
	}
	return
}
