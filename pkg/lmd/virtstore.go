package lmd

var columnsStore *DataStore

type VirtualStoreResolveFunc func(table *Table, peer *Peer) *DataStore

// GetTableBackendsStore returns the virtual data used for the backends livestatus table.
func GetTableBackendsStore(_ *Table, peer *Peer) *DataStore {
	return (peer.statusStore)
}

// BuildTableBackendsStore returns the virtual data used for the backends livestatus table.
func BuildTableBackendsStore(table *Table, peer *Peer) *DataStore {
	// simply return a new DataStore with a single row, since all columns are virtual anyway
	store := NewDataStore(table, peer)
	rows := make(ResultSet, 1)
	_, columns := store.GetInitialColumns()
	err := store.InsertData(rows, columns, true)
	if err != nil {
		log.Errorf("store error: %s", err.Error())
	}

	return store
}

// GetTableColumnsStore returns the virtual data used for the columns/table livestatus table.
func GetTableColumnsStore(_ *Table, _ *Peer) *DataStore {
	return columnsStore
}

// BuildTableColumnsStore creates the virtual data used for the columns/table livestatus table.
// it only needs to be created once, since all the data is static.
func BuildTableColumnsStore(table *Table) *DataStore {
	store := NewDataStore(table, nil)
	data := make(ResultSet, 0)
	for _, table := range Objects.Tables {
		for i := range table.columns {
			col := table.columns[i]
			if col.StorageType == RefStore {
				continue
			}
			colTypeName := ""
			switch col.DataType {
			case IntCol, Int64Col:
				colTypeName = "int"
			case StringCol, StringLargeCol, JSONCol:
				colTypeName = "string"
			case FloatCol:
				colTypeName = "float"
			case StringListCol, Int64ListCol, ServiceMemberListCol, InterfaceListCol, CustomVarCol:
				colTypeName = "list"
			default:
				log.Panicf("type not handled in table %s: %#v", table.name.String(), col)
			}
			row := []any{
				col.Name,
				table.name.String(),
				colTypeName,
				col.Description,
				col.FetchType.String(),
				col.DataType.String(),
				col.StorageType.String(),
				col.Optional.List(),
			}
			data = append(data, row)
		}
	}
	columns := make(ColumnList, 0)
	for _, col := range table.columns {
		if col.StorageType == RefStore {
			continue
		}
		columns = append(columns, col)
	}
	err := store.InsertData(data, columns, true)
	if err != nil {
		log.Errorf("store error: %s", err.Error())
	}

	return store
}

// GetGroupByData returns fake query result for given groupby table.
func GetGroupByData(table *Table, peer *Peer) *DataStore {
	if !peer.isOnline() {
		return nil
	}
	store := NewDataStore(table, peer)
	store.dataSet = peer.data.Load()
	data := make(ResultSet, 0)
	dataSet := store.dataSet
	switch store.table.name {
	case TableHostsbygroup:
		table := dataSet.Get(TableHosts)
		table.lock.RLock()
		defer table.lock.RUnlock()
		nameCol := table.GetColumn("name")
		groupCol := table.GetColumn("groups")
		for _, row := range table.data {
			name := row.GetString(nameCol)
			groups := row.GetStringList(groupCol)
			for i := range groups {
				data = append(data, []any{name, groups[i]})
			}
		}
	case TableServicesbygroup:
		table := dataSet.Get(TableServices)
		table.lock.RLock()
		defer table.lock.RUnlock()
		hostNameCol := table.GetColumn("host_name")
		descCol := table.GetColumn("description")
		groupCol := table.GetColumn("groups")
		for _, row := range table.data {
			hostName := row.GetString(hostNameCol)
			description := row.GetString(descCol)
			groups := row.GetStringList(groupCol)
			for i := range groups {
				data = append(data, []any{hostName, description, groups[i]})
			}
		}
	case TableServicesbyhostgroup:
		table := dataSet.Get(TableServices)
		table.lock.RLock()
		defer table.lock.RUnlock()
		hostNameCol := table.GetColumn("host_name")
		descCol := table.GetColumn("description")
		hostGroupsCol := table.GetColumn("host_groups")
		for _, row := range table.data {
			hostName := row.GetString(hostNameCol)
			description := row.GetString(descCol)
			groups := row.GetStringList(hostGroupsCol)
			for i := range groups {
				data = append(data, []any{hostName, description, groups[i]})
			}
		}
	default:
		log.Panicf("GetGroupByData not implemented for table: %s", store.table.name.String())
	}
	_, columns := store.GetInitialColumns()
	err := store.InsertData(data, columns, true)
	if err != nil {
		log.Errorf("store error: %s", err.Error())
	}

	return store
}
