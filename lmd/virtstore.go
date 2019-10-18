package main

type VirtStoreResolveFunc func(table *Table, peer *Peer) *DataStore

// GetTableBackendsStore returns the virtual data used for the backends livestatus table.
func GetTableBackendsStore(table *Table, peer *Peer) *DataStore {
	// simply return a new DataStore with a single row, since all columns are virtual anyway
	store := NewDataStore(table, peer)
	rows := make(ResultSet, 1)
	_, columns := store.GetInitialColumns()
	store.InsertData(&rows, columns)
	return store
}

// GetTableColumnsStore returns the virtual data used for the columns/table livestatus table.
func GetTableColumnsStore(table *Table, _ *Peer) *DataStore {
	store := NewDataStore(table, nil)
	data := make(ResultSet, 0)
	for _, t := range Objects.Tables {
		for i := range t.Columns {
			c := t.Columns[i]
			if c.StorageType == RefStore {
				continue
			}
			colTypeName := ""
			switch c.DataType {
			case IntCol, Int64Col:
				colTypeName = "int"
			case StringCol, StringLargeCol:
				colTypeName = "string"
			case FloatCol:
				colTypeName = "float"
			case StringListCol, Int64ListCol, HashMapCol, ServiceMemberListCol, InterfaceListCol, CustomVarCol:
				colTypeName = "list"
			default:
				log.Panicf("type not handled in table %s: %#v", t.Name, c)
			}
			row := []interface{}{
				c.Name,
				t.Name.String(),
				colTypeName,
				c.Description,
				c.FetchType.String(),
				c.DataType.String(),
				c.StorageType.String(),
				c.Optional.List(),
			}
			data = append(data, row)
		}
	}
	store.InsertData(&data, &table.Columns)
	return store
}

// GetGroupByData returns fake query result for given groupby table
func GetGroupByData(table *Table, peer *Peer) *DataStore {
	store := NewDataStore(table, peer)
	data := make(ResultSet, 0)
	peer.DataLock.RLock()
	defer peer.DataLock.RUnlock()
	switch store.Table.Name {
	case TableHostsbygroup:
		nameCol := peer.Tables[TableHosts].GetColumn("name")
		groupCol := peer.Tables[TableHosts].GetColumn("groups")
		for _, row := range peer.Tables[TableHosts].Data {
			name := row.GetString(nameCol)
			groups := row.GetStringList(groupCol)
			for i := range *groups {
				data = append(data, []interface{}{*name, (*groups)[i]})
			}
		}
	case TableServicesbygroup:
		hostNameCol := peer.Tables[TableServices].GetColumn("host_name")
		descCol := peer.Tables[TableServices].GetColumn("description")
		groupCol := peer.Tables[TableServices].GetColumn("groups")
		for _, row := range peer.Tables[TableServices].Data {
			hostName := row.GetString(hostNameCol)
			description := row.GetString(descCol)
			groups := row.GetStringList(groupCol)
			for i := range *groups {
				data = append(data, []interface{}{*hostName, *description, (*groups)[i]})
			}
		}
	case TableServicesbyhostgroup:
		hostNameCol := peer.Tables[TableServices].GetColumn("host_name")
		descCol := peer.Tables[TableServices].GetColumn("description")
		hostGroupsCol := peer.Tables[TableServices].GetColumn("host_groups")
		for _, row := range peer.Tables[TableServices].Data {
			hostName := row.GetString(hostNameCol)
			description := row.GetString(descCol)
			groups := row.GetStringList(hostGroupsCol)
			for i := range *groups {
				data = append(data, []interface{}{*hostName, *description, (*groups)[i]})
			}
		}
	default:
		log.Panicf("GetGroupByData not implemented for table: %s", store.Table.Name)
	}
	_, columns := store.GetInitialColumns()
	store.InsertData(&data, columns)
	return store
}
