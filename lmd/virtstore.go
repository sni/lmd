package main

type VirtStoreResolveFunc func(table *Table, peer *Peer) *DataStore

// GetTableBackendsStore returns the virtual data used for the backends livestatus table.
func GetTableBackendsStore(table *Table, peer *Peer) *DataStore {
	// simply return a new DataStore with a single row, since all columns are virtual anyway
	store := NewDataStore(table, peer)
	rows := make([][]interface{}, 1)
	_, columns := store.GetInitialColumns()
	store.InsertData(&rows, &columns)
	return store
}

// GetTableColumnsStore returns the virtual data used for the columns/table livestatus table.
func GetTableColumnsStore(table *Table, _ *Peer) *DataStore {
	store := NewDataStore(table, nil)
	data := make([][]interface{}, 0)
	for _, t := range Objects.Tables {
		for i := range t.Columns {
			c := t.Columns[i]
			if c.StorageType == RefStore {
				continue
			}
			colTypeName := ""
			switch c.DataType {
			case IntCol:
				colTypeName = "int"
			case StringCol:
				colTypeName = "string"
			case StringListCol:
				colTypeName = "list"
			case IntListCol:
				colTypeName = "list"
			case HashMapCol:
				colTypeName = "list"
			case FloatCol:
				colTypeName = "float"
			case InterfaceListCol:
				colTypeName = "list"
			case CustomVarCol:
				colTypeName = "list"
			default:
				log.Panicf("type not handled in table %s: %#v", t.Name, c)
			}
			row := make([]interface{}, 4)
			row[0] = c.Name
			row[1] = t.Name
			row[2] = colTypeName
			row[3] = c.Description
			data = append(data, row)
		}
	}
	store.InsertData(&data, &table.Columns)
	return store
}
