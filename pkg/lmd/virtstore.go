package lmd

type VirtualStoreResolveFunc func(table *Table, peer *Peer) *DataStore

// GetTableBackendsStore returns the virtual data used for the backends livestatus table.
func GetTableBackendsStore(table *Table, peer *Peer) *DataStore {
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
func GetTableColumnsStore(table *Table, _ *Peer) *DataStore {
	store := NewDataStore(table, nil)
	data := make(ResultSet, 0)
	for _, table := range Objects.Tables {
		for i := range table.Columns {
			col := table.Columns[i]
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
				log.Panicf("type not handled in table %s: %#v", table.Name.String(), col)
			}
			row := []interface{}{
				col.Name,
				table.Name.String(),
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
	for _, col := range table.Columns {
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
	store.DataSet = peer.data
	data := make(ResultSet, 0)
	dataSet := store.DataSet
	dataSet.Lock.RLock()
	defer dataSet.Lock.RUnlock()
	switch store.Table.Name {
	case TableHostsbygroup:
		nameCol := dataSet.tables[TableHosts].GetColumn("name")
		groupCol := dataSet.tables[TableHosts].GetColumn("groups")
		for _, row := range dataSet.tables[TableHosts].Data {
			name := row.GetString(nameCol)
			groups := row.GetStringList(groupCol)
			for i := range groups {
				data = append(data, []interface{}{name, groups[i]})
			}
		}
	case TableServicesbygroup:
		hostNameCol := dataSet.tables[TableServices].GetColumn("host_name")
		descCol := dataSet.tables[TableServices].GetColumn("description")
		groupCol := dataSet.tables[TableServices].GetColumn("groups")
		for _, row := range dataSet.tables[TableServices].Data {
			hostName := row.GetString(hostNameCol)
			description := row.GetString(descCol)
			groups := row.GetStringList(groupCol)
			for i := range groups {
				data = append(data, []interface{}{hostName, description, groups[i]})
			}
		}
	case TableServicesbyhostgroup:
		hostNameCol := dataSet.tables[TableServices].GetColumn("host_name")
		descCol := dataSet.tables[TableServices].GetColumn("description")
		hostGroupsCol := dataSet.tables[TableServices].GetColumn("host_groups")
		for _, row := range dataSet.tables[TableServices].Data {
			hostName := row.GetString(hostNameCol)
			description := row.GetString(descCol)
			groups := row.GetStringList(hostGroupsCol)
			for i := range groups {
				data = append(data, []interface{}{hostName, description, groups[i]})
			}
		}
	default:
		log.Panicf("GetGroupByData not implemented for table: %s", store.Table.Name.String())
	}
	_, columns := store.GetInitialColumns()
	err := store.InsertData(data, columns, true)
	if err != nil {
		log.Errorf("store error: %s", err.Error())
	}

	return store
}
