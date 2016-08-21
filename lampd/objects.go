package main

type ObjectsType struct {
	Tables map[string]*Table
}

type Table struct {
	Name                   string
	MaxIndex               int
	ColumnsIndex           map[string]int
	Columns                []Column
	StaticColCacheNames    []string
	StaticColCacheIndexes  []int
	DynamicColCacheNames   []string
	DynamicColCacheIndexes []int
	RefColCacheNames       []string
	RefColCacheIndexes     []int
}

type UpdateType int

const (
	StaticUpdate UpdateType = iota
	DynamicUpdate
	RefUpdate
	RefNoUpdate
)

type ColumnType int

const (
	StringCol ColumnType = iota
	StringListCol
	IntCol
	FloatCol
	RefCol
)

type Column struct {
	Name        string
	Type        ColumnType
	Index       int
	RefIndex    int
	RefColIndex int
	Update      UpdateType
}

func (t *Table) AddColumnObject(col *Column) int {
	Index := t.MaxIndex
	t.MaxIndex++
	if t.ColumnsIndex == nil {
		t.ColumnsIndex = make(map[string]int)
	}
	t.ColumnsIndex[col.Name] = Index
	col.Index = Index
	switch col.Update {
	case StaticUpdate:
		t.StaticColCacheNames = append(t.StaticColCacheNames, col.Name)
		t.StaticColCacheIndexes = append(t.StaticColCacheIndexes, col.Index)
		break
	case DynamicUpdate:
		t.DynamicColCacheNames = append(t.DynamicColCacheNames, col.Name)
		t.DynamicColCacheIndexes = append(t.DynamicColCacheIndexes, col.Index)
		break
	case RefUpdate:
		t.RefColCacheNames = append(t.RefColCacheNames, col.Name)
		t.RefColCacheIndexes = append(t.RefColCacheIndexes, col.Index)
		break
	}
	t.Columns = append(t.Columns, *col)
	return col.Index
}

func (t *Table) AddColumn(Name string, Update UpdateType, Type ColumnType) int {
	Column := Column{
		Name:   Name,
		Type:   Type,
		Update: Update,
	}
	return t.AddColumnObject(&Column)
}

func (t *Table) AddRefColumn(Ref string, Prefix string, Name string, Type ColumnType) (err error) {
	LocalColumn := Column{
		Name:   Prefix + "_" + Name,
		Type:   Type,
		Update: StaticUpdate,
	}
	LocalIndex := t.AddColumnObject(&LocalColumn)

	RefColumn := Column{
		Name:        Ref, // type of reference, ex.: hosts
		Type:        RefCol,
		Update:      RefUpdate,
		RefIndex:    LocalIndex,                             // contains the index from the local column, ex: host_name in services
		RefColIndex: Objects.Tables[Ref].ColumnsIndex[Name], // contains the index from the remote column, ex: name in host
	}
	RefIndex := t.AddColumnObject(&RefColumn)

	// expand reference columns
	_, Ok := Objects.Tables[Ref]
	if !Ok {
		panic("no such reference " + Ref + " from column " + Prefix + "_" + Name)
	}
	for _, col := range Objects.Tables[Ref].Columns {
		if col.Name != Name {
			Column := Column{
				Name:        Prefix + "_" + col.Name,
				Type:        Type,
				Update:      RefNoUpdate,
				RefIndex:    RefIndex,
				RefColIndex: col.Index,
			}
			t.AddColumnObject(&Column)
		}
	}
	return
}

// create all table structures
func InitObjects() (err error) {
	Objects.Tables = make(map[string]*Table)
	Objects.Tables["status"] = NewStatusTable()
	Objects.Tables["contacts"] = NewContactsTable()
	Objects.Tables["hosts"] = NewHostsTable()
	Objects.Tables["services"] = NewServicesTable()
	// TODO: add more tables
	return
}

// add state table definitions
func NewStatusTable() (t *Table) {
	t = &Table{Name: "status"}
	t.AddColumn("program_start", DynamicUpdate, IntCol)
	t.AddColumn("program_version", StaticUpdate, StringCol)
	t.AddColumn("requests", DynamicUpdate, FloatCol)
	return
}

// add contacts table definitions
func NewContactsTable() (t *Table) {
	t = &Table{Name: "contacts"}
	t.AddColumn("name", StaticUpdate, StringCol)
	t.AddColumn("alias", StaticUpdate, StringCol)
	t.AddColumn("email", StaticUpdate, StringCol)
	return
}

// add hosts table definitions
func NewHostsTable() (t *Table) {
	t = &Table{Name: "hosts"}
	t.AddColumn("name", StaticUpdate, StringCol)
	t.AddColumn("alias", StaticUpdate, StringCol)
	t.AddColumn("state", DynamicUpdate, IntCol)
	t.AddColumn("acknowledged", DynamicUpdate, IntCol)
	t.AddColumn("latency", DynamicUpdate, FloatCol)
	t.AddColumn("current_attempt", DynamicUpdate, IntCol)
	t.AddColumn("max_check_attempts", DynamicUpdate, IntCol)
	t.AddColumn("contacts", StaticUpdate, StringListCol)
	return
}

// add services table definitions
func NewServicesTable() (t *Table) {
	t = &Table{Name: "services"}
	t.AddColumn("description", StaticUpdate, StringCol)
	t.AddColumn("state", DynamicUpdate, IntCol)
	t.AddColumn("contacts", StaticUpdate, StringListCol)

	t.AddRefColumn("hosts", "host", "name", StringCol)
	return
}
