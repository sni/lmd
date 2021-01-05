package main

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// StatsType is the stats operator.
type StatsType uint8

// Besides the Counter, which counts the data rows by using a filter, there are 4 aggregations
// operators: Sum, Average, Min and Max.
const (
	NoStats StatsType = iota
	Counter
	Sum     // sum
	Average // avg
	Min     // min
	Max     // max
)

const RegexDotMinSize = 4

var reRegexDotReplace = regexp.MustCompile(`[a-zA-Z0-9]\.[a-zA-Z]`)

// String converts a StatsType back to the original string.
func (op *StatsType) String() string {
	switch *op {
	case Average:
		return "avg"
	case Sum:
		return "sum"
	case Min:
		return "min"
	case Max:
		return "Max"
	default:
		log.Panicf("not implemented")
	}
	return ""
}

// Filter defines a single filter object.
type Filter struct {
	noCopy noCopy
	// filter can either be a single filter
	Column     *Column
	Operator   Operator
	StrValue   string
	FloatValue float64
	Regexp     *regexp.Regexp
	CustomTag  string
	IsEmpty    bool
	Negate     bool

	// or a group of filters
	Filter        []*Filter
	GroupOperator GroupOperator

	// stats query
	Stats      float64
	StatsCount int
	StatsType  StatsType

	// copy of Column.Optional
	ColumnOptional OptionalFlags
}

// Operator defines a filter operator.
type Operator uint8

// Operator defines the kind of operator used to compare values with
// data columns.
const (
	_ Operator = iota
	// Generic
	Equal         // =
	Unequal       // !=
	EqualNocase   // =~
	UnequalNocase // !=~

	// Text
	RegexMatch          // ~
	RegexMatchNot       // !~
	RegexNoCaseMatch    // ~~
	RegexNoCaseMatchNot // !~~

	// String Matching
	Contains          // internal only
	ContainsNot       // internal only
	ContainsNoCase    // internal only
	ContainsNoCaseNot // internal only

	// Numeric
	Less        // <
	LessThan    // <=
	Greater     // >
	GreaterThan // >=

	// Groups
	GroupContainsNot // !>=
)

// String converts a Operator back to the original string.
func (op *Operator) String() string {
	switch *op {
	case Equal:
		return ("=")
	case Unequal:
		return ("!=")
	case EqualNocase:
		return ("=~")
	case UnequalNocase:
		return ("!=~")
	case RegexMatch:
		return ("~")
	case RegexMatchNot:
		return ("!~")
	case RegexNoCaseMatch:
		return ("~~")
	case RegexNoCaseMatchNot:
		return ("!~~")
	case Contains:
		return ("~")
	case ContainsNot:
		return ("!~")
	case ContainsNoCase:
		return ("~~")
	case ContainsNoCaseNot:
		return ("!~~")
	case Less:
		return ("<")
	case LessThan:
		return ("<=")
	case Greater:
		return (">")
	case GreaterThan:
		return (">=")
	case GroupContainsNot:
		return ("!>=")
	}
	log.Panicf("not implemented")
	return ""
}

// String converts a filter back to its string representation.
func (f *Filter) String(prefix string) (str string) {
	if len(f.Filter) > 0 {
		for i := range f.Filter {
			str += f.Filter[i].String(prefix)
		}
		str += fmt.Sprintf("%s%s: %d\n", prefix, f.GroupOperator.String(), len(f.Filter))
		return
	}

	strVal := f.strValue()
	if strVal != "" {
		strVal = " " + strVal
	}

	// trim lower case columns prefix, they are used internally only
	colName := strings.TrimSuffix(f.Column.Name, "_lc")

	switch f.StatsType {
	case NoStats:
		if prefix == "" {
			prefix = "Filter"
		}
		str = fmt.Sprintf("%s: %s %s%s\n", prefix, colName, f.Operator.String(), strVal)
	case Counter:
		str = fmt.Sprintf("Stats: %s %s%s\n", colName, f.Operator.String(), strVal)
	default:
		str = fmt.Sprintf("Stats: %s %s\n", f.StatsType.String(), colName)
	}
	if f.Negate {
		str += fmt.Sprintf("%s\n", "Negate:")
	}
	return
}

func (f *Filter) strValue() string {
	colType := f.Column.DataType
	if f.IsEmpty {
		return f.CustomTag
	}
	var value string
	switch colType {
	case HashMapCol:
		fallthrough
	case CustomVarCol:
		value = f.CustomTag + " " + f.StrValue
	case Int64ListCol:
		fallthrough
	case IntCol, Int64Col:
		fallthrough
	case FloatCol:
		fallthrough
	case StringListCol:
		fallthrough
	case ServiceMemberListCol:
		fallthrough
	case InterfaceListCol:
		fallthrough
	case StringCol, StringLargeCol:
		value = f.StrValue
	default:
		log.Panicf("not implemented column type: %v", f.Column.DataType)
	}

	return value
}

// ApplyValue add the given value to this stats filter
func (f *Filter) ApplyValue(val float64, count int) {
	switch f.StatsType {
	case Counter:
		f.Stats += float64(count)
	case Average:
		fallthrough
	case Sum:
		f.Stats += val
	case Min:
		value := val
		if f.Stats > value || f.Stats == -1 {
			f.Stats = value
		}
	case Max:
		value := val
		if f.Stats < value {
			f.Stats = value
		}
	default:
		panic("not implemented stats type")
	}
	f.StatsCount += count
}

// ParseFilter parses a single line into a filter object.
// It returns any error encountered.
func ParseFilter(value []byte, table TableName, stack *[]*Filter, options ParseOptions) (err error) {
	tmp := bytes.SplitN(value, []byte(" "), 3)
	if len(tmp) < 2 {
		err = errors.New("filter header must be Filter: <field> <operator> <value>")
		return
	}
	// filter are allowed to be empty
	if len(tmp) == 2 {
		tmp = append(tmp, []byte(""))
	}

	op, isRegex, err := parseFilterOp(tmp[1])
	if err != nil {
		return
	}

	columnName := string(tmp[0])

	// convert value to type of column
	col := Objects.Tables[table].GetColumnWithFallback(columnName)
	filter := &Filter{
		Operator: op,
		Column:   col,
		Negate:   false,
	}

	err = filter.setFilterValue(string(tmp[2]))
	if err != nil {
		return
	}

	if options&ParseOptimize != 0 {
		filter.setLowerCaseColumn()
	}

	if isRegex {
		err = filter.setRegexFilter(options)
		if err != nil {
			return
		}
	}

	filter.ColumnOptional = col.Optional
	*stack = append(*stack, filter)
	return
}

// setFilterValue converts the text value into the given filters type value
func (f *Filter) setRegexFilter(options ParseOptions) error {
	val := strings.TrimPrefix(f.StrValue, ".*")
	val = strings.TrimSuffix(val, ".*")

	if options&ParseOptimize != 0 && !hasRegexpCharacters(val) {
		switch f.Operator {
		case RegexMatch:
			f.Operator = Contains
			f.StrValue = val
		case RegexMatchNot:
			f.Operator = ContainsNot
			f.StrValue = val
		case RegexNoCaseMatch:
			f.Operator = ContainsNoCase
			f.StrValue = strings.ToLower(val)
		case RegexNoCaseMatchNot:
			f.Operator = ContainsNoCaseNot
			f.StrValue = strings.ToLower(val)
		}
	} else {
		if f.Operator == RegexNoCaseMatchNot || f.Operator == RegexNoCaseMatch {
			val = "(?i)" + val
		}
		regex, err := regexp.Compile(val)
		if err != nil {
			return errors.New("invalid regular expression: " + err.Error())
		}
		f.Regexp = regex
	}
	return nil
}

// setFilterValue converts the text value into the given filters type value
func (f *Filter) setFilterValue(strVal string) (err error) {
	colType := f.Column.DataType
	if strVal == "" {
		f.IsEmpty = true
	}
	f.StrValue = strVal
	switch colType {
	case Int64ListCol:
		fallthrough
	case IntCol, Int64Col, FloatCol:
		switch f.Operator {
		case Equal, Unequal, Greater, GreaterThan, Less, LessThan:
			if !f.IsEmpty {
				filtervalue, cerr := strconv.ParseFloat(strVal, 64)
				if cerr != nil {
					err = fmt.Errorf("could not convert %s to number in filter: %s", strVal, f.String(""))
					return
				}
				f.FloatValue = filtervalue
			}
		default:
		}
		return
	case HashMapCol:
		fallthrough
	case CustomVarCol:
		vars := strings.SplitN(strVal, " ", 2)
		if vars[0] == "" {
			err = errors.New("custom variable filter must have form \"Filter: custom_variables <op> <variable> [<value>]\"")
			return
		}
		if len(vars) == 1 {
			f.IsEmpty = true
		} else {
			f.StrValue = vars[1]
		}
		f.CustomTag = vars[0]
		return
	case InterfaceListCol:
		return
	case StringListCol:
		return
	case ServiceMemberListCol:
		return
	case StringCol, StringLargeCol:
		return
	}
	log.Panicf("not implemented column type: %v", colType)
	return
}

// setLowerCaseColumn tries to use the lowercase column if possible
func (f *Filter) setLowerCaseColumn() {
	col := f.Column
	table := col.Table
	// only hosts and services tables have lower case cache fields
	if table.Name != TableHosts && table.Name != TableServices {
		return
	}
	// lower case fields will only be used for case-insensitive operators
	var op Operator
	switch f.Operator {
	default:
		return
	case ContainsNoCase:
		op = Contains
	case ContainsNoCaseNot:
		op = ContainsNot
	case RegexNoCaseMatch:
		op = RegexMatch
	case RegexNoCaseMatchNot:
		op = RegexMatchNot
	}
	col, ok := table.ColumnsIndex[col.Name+"_lc"]
	if !ok {
		return
	}
	f.Column = col
	f.Operator = op
	f.StrValue = strings.ToLower(f.StrValue)
}

func parseFilterOp(in []byte) (op Operator, isRegex bool, err error) {
	isRegex = false
	switch string(in) {
	case "=":
		op = Equal
		return
	case "=~":
		op = EqualNocase
		return
	case "~":
		op = RegexMatch
		isRegex = true
		return
	case "!~":
		op = RegexMatchNot
		isRegex = true
		return
	case "~~":
		op = RegexNoCaseMatch
		isRegex = true
		return
	case "!~~":
		op = RegexNoCaseMatchNot
		isRegex = true
		return
	case "!=":
		op = Unequal
		return
	case "!=~":
		op = UnequalNocase
		return
	case "<":
		op = Less
		return
	case "<=":
		op = LessThan
		return
	case ">":
		op = Greater
		return
	case ">=":
		op = GreaterThan
		return
	case "!>=":
		op = GroupContainsNot
		return
	case "like":
		op = Contains
		return
	case "unlike":
		op = ContainsNot
		return
	case "ilike":
		op = ContainsNoCase
		return
	case "iunlike":
		op = ContainsNoCaseNot
		return
	}
	err = fmt.Errorf("unrecognized filter operator: %s", in)
	return
}

// ParseStats parses a text line into a stats object.
// It returns any error encountered.
func ParseStats(value []byte, table TableName, stack *[]*Filter, options ParseOptions) (err error) {
	tmp := bytes.SplitN(value, []byte(" "), 2)
	if len(tmp) < 2 {
		err = fmt.Errorf("stats header, must be Stats: <field> <operator> <value> OR Stats: <sum|avg|min|max> <field>")
		return
	}
	startWith := float64(0)
	var op StatsType
	switch string(bytes.ToLower(tmp[0])) {
	case "avg":
		op = Average
	case "min":
		op = Min
		startWith = -1
	case "max":
		op = Max
	case "sum":
		op = Sum
	default:
		err = ParseFilter(value, table, stack, options)
		if err != nil {
			return
		}
		// set last one to counter
		(*stack)[len(*stack)-1].StatsType = Counter
		return
	}

	columnName := string(tmp[1])
	col := Objects.Tables[table].ColumnsIndex[columnName]
	if col == nil {
		err = fmt.Errorf("unrecognized column from stats: %s", columnName)
		return
	}
	stats := &Filter{
		Column:     col,
		StatsType:  op,
		Stats:      startWith,
		StatsCount: 0,
	}
	*stack = append(*stack, stats)
	return
}

// ParseFilterOp parses a text line into a filter group operator like And: <nr>.
// It returns any error encountered.
func ParseFilterOp(op GroupOperator, value []byte, stack *[]*Filter) (err error) {
	num, cerr := strconv.Atoi(string(value))
	if cerr != nil || num < 0 {
		err = fmt.Errorf("%s must be a positive number", op.String())
		return
	}
	if num == 0 {
		if log.IsV(LogVerbosityDebug) {
			log.Debugf("ignoring %s as value is not positive", value)
		}
		return
	}
	stackLen := len(*stack)
	if stackLen < num {
		err = errors.New("not enough filter on stack")
		return
	}
	// remove x entrys from stack and combine them to a new group
	groupedStack, remainingStack := (*stack)[stackLen-num:], (*stack)[:stackLen-num]
	stackedFilter := &Filter{Filter: groupedStack, GroupOperator: op}
	*stack = make([]*Filter, 0, len(remainingStack)+1)
	*stack = append(*stack, remainingStack...)
	*stack = append(*stack, stackedFilter)
	return
}

// ParseFilterNegate sets the last filter group to be negated
func ParseFilterNegate(stack *[]*Filter) (err error) {
	stackLen := len(*stack)
	if stackLen == 0 {
		err = fmt.Errorf("no filter on stack to negate")
		return
	}
	filter := (*stack)[stackLen-1]
	filter.Negate = true
	return
}

// Match returns true if the given filter matches the given value.
func (f *Filter) Match(row *DataRow) bool {
	switch f.Column.DataType {
	case StringCol, StringLargeCol:
		return f.MatchString(row.GetString(f.Column))
	case StringListCol:
		return f.MatchStringList(row.GetStringList(f.Column))
	case IntCol:
		if f.IsEmpty {
			return matchEmptyFilter(f.Operator)
		}
		return f.MatchInt(row.GetInt(f.Column))
	case Int64Col:
		if f.IsEmpty {
			return matchEmptyFilter(f.Operator)
		}
		return f.MatchInt64(row.GetInt64(f.Column))
	case FloatCol:
		if f.IsEmpty {
			return matchEmptyFilter(f.Operator)
		}
		return f.MatchFloat(row.GetFloat(f.Column))
	case Int64ListCol:
		return f.MatchInt64List(row.GetInt64List(f.Column))
	case HashMapCol:
		fallthrough
	case CustomVarCol:
		return f.MatchCustomVar(row.GetHashMap(f.Column))
	case InterfaceListCol, ServiceMemberListCol:
		// not implemented
		return false
	}
	log.Panicf("not implemented filter match type: %s", f.Column.DataType.String())
	return false
}

func (f *Filter) MatchInt(value int) bool {
	intVal := int(f.FloatValue)
	switch f.Operator {
	case Equal:
		return value == intVal
	case Unequal:
		return value != intVal
	case Less:
		return value < intVal
	case LessThan:
		return value <= intVal
	case Greater:
		return value > intVal
	case GreaterThan:
		return value >= intVal
	}
	strVal := fmt.Sprintf("%v", value)
	return f.MatchString(&strVal)
}

func (f *Filter) MatchInt64(value int64) bool {
	intVal := int64(f.FloatValue)
	switch f.Operator {
	case Equal:
		return value == intVal
	case Unequal:
		return value != intVal
	case Less:
		return value < intVal
	case LessThan:
		return value <= intVal
	case Greater:
		return value > intVal
	case GreaterThan:
		return value >= intVal
	}
	strVal := fmt.Sprintf("%v", value)
	return f.MatchString(&strVal)
}

func (f *Filter) MatchFloat(value float64) bool {
	switch f.Operator {
	case Equal:
		return value == f.FloatValue
	case Unequal:
		return value != f.FloatValue
	case Less:
		return value < f.FloatValue
	case LessThan:
		return value <= f.FloatValue
	case Greater:
		return value > f.FloatValue
	case GreaterThan:
		return value >= f.FloatValue
	}
	strVal := fmt.Sprintf("%v", value)
	return f.MatchString(&strVal)
}

func matchEmptyFilter(op Operator) bool {
	switch op {
	case Equal:
		return false
	case Unequal:
		return true
	case Less:
		return false
	case LessThan:
		return false
	case Greater:
		return true
	case GreaterThan:
		return true
	}
	log.Warnf("not implemented empty op: %s", op.String())
	return false
}

func (f *Filter) MatchString(value *string) bool {
	switch f.Operator {
	case Equal:
		return *value == f.StrValue
	case Unequal:
		return *value != f.StrValue
	case EqualNocase:
		return strings.EqualFold(*value, f.StrValue)
	case UnequalNocase:
		return !strings.EqualFold(*value, f.StrValue)
	case RegexMatch, RegexNoCaseMatch:
		return f.Regexp.MatchString(*value)
	case RegexMatchNot, RegexNoCaseMatchNot:
		return !f.Regexp.MatchString(*value)
	case Less:
		return *value < f.StrValue
	case LessThan:
		return *value <= f.StrValue
	case Greater:
		return *value > f.StrValue
	case GreaterThan:
		return *value >= f.StrValue
	case Contains:
		return strings.Contains(*value, f.StrValue)
	case ContainsNot:
		return !strings.Contains(*value, f.StrValue)
	case ContainsNoCase:
		return strings.Contains(strings.ToLower(*value), f.StrValue)
	case ContainsNoCaseNot:
		return !strings.Contains(strings.ToLower(*value), f.StrValue)
	}
	log.Warnf("not implemented string op: %s", f.Operator.String())
	return false
}

func (f *Filter) MatchStringList(list *[]string) bool {
	switch f.Operator {
	case Equal:
		// used to match for empty lists, like: contacts = ""
		// return true if the list is empty
		return f.StrValue == "" && len(*list) == 0
	case Unequal:
		// used to match for any entry in lists, like: contacts != ""
		// return true if the list is not empty
		return f.StrValue == "" && len(*list) != 0
	case GreaterThan:
		for i := range *list {
			if f.StrValue == (*list)[i] {
				return true
			}
		}
		return false
	case GroupContainsNot:
		for i := range *list {
			if f.StrValue == (*list)[i] {
				return false
			}
		}
		return true
	case RegexMatch, RegexNoCaseMatch, Contains, ContainsNoCase:
		for i := range *list {
			if f.MatchString(&(*list)[i]) {
				return true
			}
		}
		return false
	case RegexMatchNot, RegexNoCaseMatchNot, ContainsNot, ContainsNoCaseNot:
		for i := range *list {
			if f.MatchString(&(*list)[i]) {
				return false
			}
		}
		return true
	}
	log.Warnf("not implemented stringlist op: %s", f.Operator.String())
	return false
}

func (f *Filter) MatchInt64List(list []int64) bool {
	switch f.Operator {
	case Equal:
		return f.IsEmpty && len(list) == 0
	case Unequal:
		return f.IsEmpty && len(list) != 0
	case GreaterThan:
		fVal := int64(f.FloatValue)
		for i := range list {
			if fVal == list[i] {
				return true
			}
		}
		return false
	case GroupContainsNot:
		fVal := int64(f.FloatValue)
		for i := range list {
			if fVal == list[i] {
				return false
			}
		}
		return true
	}
	log.Warnf("not implemented Int64list op: %s", f.Operator.String())
	return false
}

func (f *Filter) MatchCustomVar(value map[string]string) bool {
	val, ok := value[f.CustomTag]
	if !ok {
		val = ""
	}
	return f.MatchString(&val)
}

// some broken clients request <table>_column instead of just column
// be nice to them as well...
func fixBrokenClientsRequestColumn(columnName *string, table TableName) bool {
	fixedColumnName := *columnName

	switch table {
	case TableHostsbygroup:
		fixedColumnName = strings.TrimPrefix(fixedColumnName, "host_")
	case TableServicesbygroup, TableServicesbyhostgroup:
		fixedColumnName = strings.TrimPrefix(fixedColumnName, "service_")
	case TableStatus:
		fixedColumnName = strings.TrimPrefix(fixedColumnName, "status_")
	default:
		var tablePrefix strings.Builder
		tablePrefix.WriteString(strings.TrimSuffix(table.String(), "s"))
		tablePrefix.WriteString("_")
		fixedColumnName = strings.TrimPrefix(fixedColumnName, tablePrefix.String())
	}

	if _, ok := Objects.Tables[table].ColumnsIndex[fixedColumnName]; ok {
		*columnName = fixedColumnName
		return true
	}

	return false
}

// hasRegexpCharacters returns true if string is a probably a regular expression
func hasRegexpCharacters(val string) bool {
	if strings.ContainsAny(val, `|([{*+?^\$`) {
		return true
	}
	// dots are part of regular expressions but also common in host names
	// try to distinguish between them
	if strings.Contains(val, ".") {
		if len(val) < RegexDotMinSize {
			return true
		}
		test := reRegexDotReplace.ReplaceAllString(val, "")
		// still contans dots?
		if strings.Contains(test, ".") {
			return true
		}
	}
	return false
}
