package lmd

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"slices"
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
	StatsGroup
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
		log.Panicf("not implemented: %#v", op)
	}

	return ""
}

// Filter defines a single filter object.
// filter can either be a single filter
// or a group of filters (GroupOperator).
type Filter struct {
	noCopy         noCopy
	column         *Column // filter can either be a single filter
	regexp         *regexp.Regexp
	hstRegexp      *regexp.Regexp // used for service member filters
	svcRegexp      *regexp.Regexp // same
	stringVal      string
	customTag      string
	hstVal         string    // used for service member filters
	svcVal         string    // same
	filter         []*Filter // filter can be a group of filters
	int64Value     int64
	floatValue     float64
	stats          float64 // stats query
	statsCount     int
	statsPos       int           // position in stats result array
	columnIndex    int           // copy of Column.Index if Column is of type LocalStore
	columnOptional OptionalFlags // copy of Column.Optional
	intValue       int8
	isEmpty        bool
	negate         bool
	groupOperator  GroupOperator
	operator       Operator
	statsType      StatsType
}

// Operator defines a filter operator.
type Operator uint8

// Operator defines the kind of operator used to compare values with
// data columns.
const (
	_ Operator = iota
	// Generic Operators.
	Equal         // =
	Unequal       // !=
	EqualNoCase   // =~
	UnequalNoCase // !=~

	// Text Regexp Operators.
	RegexMatch          // ~
	RegexMatchNot       // !~
	RegexNoCaseMatch    // ~~
	RegexNoCaseMatchNot // !~~

	// String Matching.
	Contains          // internal only
	ContainsNot       // internal only
	ContainsNoCase    // internal only
	ContainsNoCaseNot // internal only

	// Numeric.
	Less        // <
	LessThan    // <=
	Greater     // >
	GreaterThan // >=

	// Groups.
	GroupContainsNot // !>=
)

// String converts a Operator back to the original string.
func (op *Operator) String() string {
	switch *op {
	case Equal:
		return ("=")
	case Unequal:
		return ("!=")
	case EqualNoCase:
		return ("=~")
	case UnequalNoCase:
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
	strNegate := ""

	if f.negate {
		if f.statsType == NoStats {
			strNegate = fmt.Sprintf("%s\n", "Negate:")
		} else {
			strNegate = fmt.Sprintf("%s\n", "StatsNegate:")
		}
	}

	if f.groupOperator == And || f.groupOperator == Or {
		if len(f.filter) > 0 {
			for i := range f.filter {
				str += f.filter[i].String(prefix)
			}
			str += fmt.Sprintf("%s%s: %d\n", prefix, f.groupOperator.String(), len(f.filter))
			str += strNegate

			return str
		}
	}

	strVal := f.strValue()
	if strVal != "" {
		strVal = " " + strVal
	}

	// trim lower case columns prefix, they are used internally only
	colName := strings.TrimSuffix(f.column.Name, "_lc")

	switch f.statsType {
	case NoStats:
		if prefix == "" {
			prefix = "Filter"
		}
		str = fmt.Sprintf("%s: %s %s%s\n", prefix, colName, f.operator.String(), strVal)
	case StatsGroup:
		if prefix == "" {
			prefix = "Filter"
		}
		str = fmt.Sprintf("%sGroup: %s %s%s\n", prefix, colName, f.operator.String(), strVal)
	case Counter:
		str = fmt.Sprintf("Stats: %s %s%s\n", colName, f.operator.String(), strVal)
	default:
		str = fmt.Sprintf("Stats: %s %s\n", f.statsType.String(), colName)
	}
	str += strNegate

	return str
}

// Equals returns true if both filter are exactly identical.
func (f *Filter) Equals(other *Filter) bool {
	if f.column != other.column {
		return false
	}
	if f.operator != other.operator {
		return false
	}
	if f.stringVal != other.stringVal {
		return false
	}
	if f.negate != other.negate {
		return false
	}
	if f.floatValue != other.floatValue {
		return false
	}
	if f.statsType != other.statsType {
		return false
	}
	if f.groupOperator != other.groupOperator {
		return false
	}
	if len(f.filter) != len(other.filter) {
		return false
	}
	for i := range f.filter {
		if !f.filter[i].Equals(other.filter[i]) {
			return false
		}
	}

	return true
}

func (f *Filter) strValue() string {
	colType := f.column.DataType
	if f.isEmpty {
		return f.customTag
	}
	var value string
	switch colType {
	case CustomVarCol:
		value = f.customTag + " " + f.stringVal
	case Int64ListCol,
		IntCol, Int64Col,
		FloatCol,
		StringListCol,
		ServiceMemberListCol,
		InterfaceListCol,
		JSONCol,
		StringLargeCol,
		StringCol:
		value = f.stringVal
	default:
		log.Panicf("not implemented column type: %v", f.column.DataType)
	}

	return value
}

// ApplyValue add the given value to this stats filter.
func (f *Filter) ApplyValue(val float64, count int) {
	switch f.statsType {
	case Counter:
		f.stats += float64(count)
	case Average, Sum:
		f.stats += val
	case Min:
		value := val
		if f.stats > value || f.stats == -1 {
			f.stats = value
		}
	case Max:
		value := val
		if f.stats < value {
			f.stats = value
		}
	default:
		panic("not implemented stats type")
	}
	f.statsCount += count
}

// ParseFilter parses a single line into a filter object.
// It returns any error encountered.
func ParseFilter(value []byte, table TableName, stack *[]*Filter, options ParseOptions) (err error) {
	tmp := bytes.SplitN(value, []byte(" "), 3)
	if len(tmp) < 2 {
		return errors.New("filter header must be Filter: <field> <operator> <value>")
	}
	// filter are allowed to be empty
	if len(tmp) == 2 {
		tmp = append(tmp, []byte(""))
	}

	operator, isRegex, err := parseFilterOp(tmp[1])
	if err != nil {
		return err
	}

	// convert value to type of column
	col := Objects.Tables[table].GetColumnWithFallback(string(tmp[0]))
	filter := &Filter{
		operator:       operator,
		column:         col,
		negate:         false,
		columnIndex:    -1,
		columnOptional: col.Optional,
	}

	err = filter.setFilterValue(string(tmp[2]))
	if err != nil {
		return err
	}

	if options&ParseOptimize != 0 {
		filter.setLowerCaseColumn()
		col = filter.column // might have changed
	}

	if isRegex {
		err = filter.setRegexFilter(options)
		if err != nil {
			return err
		}
	}

	if !filter.isEmpty && col.Optional == NoFlags && col.StorageType == LocalStore {
		filter.columnIndex = col.Index
	}

	err = filter.setFilterValueServiceMembers()
	if err != nil {
		return err
	}

	*stack = append(*stack, filter)

	return nil
}

// setFilterValue converts the text value into the given filters type value.
func (f *Filter) setRegexFilter(options ParseOptions) error {
	val := strings.TrimPrefix(f.stringVal, ".*")
	val = strings.TrimSuffix(val, ".*")

	// special case Filter: host_name ~ ^name$
	if options&ParseOptimize != 0 && strings.HasPrefix(val, "^") && strings.HasSuffix(val, "$") {
		val2 := strings.TrimPrefix(val, "^")
		val2 = strings.TrimSuffix(val2, "$")
		if !hasRegexpCharacters(val2) {
			switch f.operator {
			case RegexMatch:
				f.operator = Equal
				f.stringVal = val2
			case RegexNoCaseMatch:
				f.operator = EqualNoCase
				f.stringVal = val2
			default:
				// other columns not supported
			}
		}
	}

	if options&ParseOptimize != 0 && !hasRegexpCharacters(val) {
		switch f.operator {
		case RegexMatch:
			f.operator = Contains
			f.stringVal = val
		case RegexMatchNot:
			f.operator = ContainsNot
			f.stringVal = val
		case RegexNoCaseMatch:
			f.operator = ContainsNoCase
			f.stringVal = strings.ToLower(val)
		case RegexNoCaseMatchNot:
			f.operator = ContainsNoCaseNot
			f.stringVal = strings.ToLower(val)
		default:
			// only relevant for regex operators
		}
	} else {
		if f.operator == RegexNoCaseMatchNot || f.operator == RegexNoCaseMatch {
			val = "(?i)" + val
		}
		regex, err := regexp.Compile(val)
		if err != nil {
			return errors.New("invalid regular expression: " + err.Error())
		}
		f.regexp = regex
	}

	return nil
}

// setFilterValue converts the text value into the given filters type value.
func (f *Filter) setFilterValue(strVal string) (err error) {
	strVal = strings.TrimSpace(strVal)
	colType := f.column.DataType
	if strVal == "" {
		f.isEmpty = true
	}
	f.stringVal = strVal
	switch colType {
	case IntCol, Int64Col, Int64ListCol, FloatCol:
		switch f.operator {
		case Equal, Unequal, Greater, GreaterThan, Less, LessThan, GroupContainsNot:
			if !f.isEmpty {
				filterValue, cerr := strconv.ParseFloat(strVal, 64)
				if cerr != nil {
					return fmt.Errorf("could not convert %s to number in filter: %s", strVal, f.String(""))
				}
				f.floatValue = filterValue
				f.intValue = int8(filterValue)
				f.int64Value = int64(filterValue)
			}
		default:
		}

		return nil
	case CustomVarCol:
		vars := strings.SplitN(strVal, " ", 2)
		if vars[0] == "" {
			return errors.New("custom variable filter must have form \"Filter: custom_variables <op> <variable> [<value>]\"")
		}
		if len(vars) == 1 {
			f.isEmpty = true
		} else {
			f.stringVal = vars[1]
		}
		f.customTag = vars[0]

		return nil
	case InterfaceListCol:
		return nil
	case StringListCol:
		return nil
	case ServiceMemberListCol:
		// handled later in setFilterValueServiceMembers()
		return nil
	case JSONCol:
		return nil
	case StringLargeCol:
		return nil
	case StringCol:
		return nil
	case StringListSortedCol:
		log.Panicf("sorted string list is a virtual column type and not directly used")
	}

	log.Panicf("not implemented column type: %v", colType)

	return nil
}

// setLowerCaseColumn tries to use the lowercase column if possible.
func (f *Filter) setLowerCaseColumn() {
	col := f.column
	table := col.Table
	// only hosts and services tables have lower case cache fields
	if table.name != TableHosts && table.name != TableServices {
		return
	}
	// lower case fields will only be used for case-insensitive operators
	var operator Operator
	switch f.operator {
	case ContainsNoCase:
		operator = Contains
	case ContainsNoCaseNot:
		operator = ContainsNot
	case RegexNoCaseMatch:
		operator = RegexMatch
	case RegexNoCaseMatchNot:
		operator = RegexMatchNot
	default:
		return
	}
	col, ok := table.columnsIndex[col.Name+"_lc"]
	if !ok {
		return
	}
	f.column = col
	f.operator = operator
	f.stringVal = strings.ToLower(f.stringVal)
}

func (f *Filter) setFilterValueServiceMembers() error {
	if f.column.DataType != ServiceMemberListCol {
		return nil
	}

	vars := strings.SplitN(f.stringVal, ";", 2)

	f.hstVal = vars[0]
	if len(vars) == 2 {
		f.svcVal = vars[1]
	}

	if f.regexp != nil {
		hstVal := strings.TrimPrefix(f.hstVal, ".*")
		hstVal = strings.TrimSuffix(hstVal, ".*")

		svcVal := strings.TrimPrefix(f.svcVal, ".*")
		svcVal = strings.TrimSuffix(svcVal, ".*")

		if f.operator == RegexNoCaseMatchNot || f.operator == RegexNoCaseMatch {
			hstVal = "(?i)" + hstVal
			svcVal = "(?i)" + svcVal
		}

		hstRegex, err := regexp.Compile(hstVal)
		if err != nil {
			return errors.New("invalid regular expression: " + err.Error())
		}
		f.hstRegexp = hstRegex

		svcRegex, err := regexp.Compile(svcVal)
		if err != nil {
			return errors.New("invalid regular expression: " + err.Error())
		}
		f.svcRegexp = svcRegex
	}

	return nil
}

func parseFilterOp(raw []byte) (op Operator, isRegex bool, err error) {
	switch string(raw) {
	case "=":
		return Equal, false, nil
	case "=~":
		return EqualNoCase, false, nil
	case "~":
		return RegexMatch, true, nil
	case "!~":
		return RegexMatchNot, true, nil
	case "~~":
		return RegexNoCaseMatch, true, nil
	case "!~~":
		return RegexNoCaseMatchNot, true, nil
	case "!=":
		return Unequal, false, nil
	case "!=~":
		return UnequalNoCase, false, nil
	case "<":
		return Less, false, nil
	case "<=":
		return LessThan, false, nil
	case ">":
		return Greater, false, nil
	case ">=":
		return GreaterThan, false, nil
	case "!>=":
		return GroupContainsNot, false, nil
	case "like":
		return Contains, false, nil
	case "unlike":
		return ContainsNot, false, nil
	case "ilike":
		return ContainsNoCase, false, nil
	case "iunlike":
		return ContainsNoCaseNot, false, nil
	}

	return 0, false, fmt.Errorf("unrecognized filter operator: %s", raw)
}

// ParseStats parses a text line into a stats object.
// It returns any error encountered.
func ParseStats(value []byte, table TableName, stack *[]*Filter, options ParseOptions) (err error) {
	tmp := bytes.SplitN(value, []byte(" "), 2)
	if len(tmp) < 2 {
		return fmt.Errorf("stats header, must be Stats: <field> <operator> <value> OR Stats: <sum|avg|min|max> <field>")
	}
	startWith := float64(0)
	var statsOp StatsType
	switch string(bytes.ToLower(tmp[0])) {
	case "avg":
		statsOp = Average
	case "min":
		statsOp = Min
		startWith = -1
	case "max":
		statsOp = Max
	case "sum":
		statsOp = Sum
	default:
		err = ParseFilter(value, table, stack, options)
		if err != nil {
			return err
		}
		// set last one to counter
		(*stack)[len(*stack)-1].statsType = Counter

		return nil
	}

	col := Objects.Tables[table].GetColumnWithFallback(string(tmp[1]))
	stats := &Filter{
		column:         col,
		statsType:      statsOp,
		stats:          startWith,
		statsCount:     0,
		columnIndex:    -1,
		columnOptional: col.Optional,
	}
	if !stats.isEmpty && col.Optional == NoFlags && col.StorageType == LocalStore {
		stats.columnIndex = col.Index
	}

	*stack = append(*stack, stats)

	return nil
}

// parseFilterGroupOp parses a text line into a filter group operator like And: <nr>.
// It returns any error encountered.
func parseFilterGroupOp(groupOp GroupOperator, value []byte, stack *[]*Filter) (err error) {
	num, cerr := strconv.Atoi(string(value))
	if cerr != nil || num < 0 {
		return fmt.Errorf("%s must be a positive number", groupOp.String())
	}
	if num == 0 {
		if log.IsV(LogVerbosityDebug) {
			log.Debugf("ignoring %s as value is not positive", value)
		}

		return nil
	}
	stackLen := len(*stack)
	if stackLen < num {
		return errors.New("not enough filter on stack")
	}
	// remove x entries from stack and combine them to a new group
	groupedStack, remainingStack := (*stack)[stackLen-num:], (*stack)[:stackLen-num]
	stackedFilter := &Filter{filter: groupedStack, groupOperator: groupOp}
	*stack = make([]*Filter, 0, len(remainingStack)+1)
	*stack = append(*stack, remainingStack...)
	*stack = append(*stack, stackedFilter)

	return nil
}

// ParseFilterNegate sets the last filter or stats group to be negated.
func ParseFilterNegate(stack []*Filter) (err error) {
	stackLen := len(stack)
	if stackLen == 0 {
		return fmt.Errorf("no filter/stats on stack to negate")
	}

	stack[stackLen-1].negate = true

	return err
}

// Match returns true if the given filter matches the given value.
func (f *Filter) Match(row *DataRow) bool {
	switch f.column.DataType {
	case StringCol:
		if f.columnIndex != -1 {
			return f.MatchString(row.dataString[f.columnIndex])
		}

		return f.MatchString(row.GetString(f.column))
	case StringLargeCol, JSONCol:
		return f.MatchString(row.GetString(f.column))
	case StringListCol:
		return f.MatchStringList(row.GetStringList(f.column))
	case IntCol:
		if f.columnIndex != -1 {
			return f.MatchInt8(row.dataInt[f.columnIndex])
		}
		if f.isEmpty {
			return matchEmptyFilter(f.operator)
		}

		return f.MatchInt8(row.GetInt8(f.column))
	case Int64Col:
		if f.columnIndex != -1 {
			return f.MatchInt64(row.dataInt64[f.columnIndex])
		}
		if f.isEmpty {
			return matchEmptyFilter(f.operator)
		}

		return f.MatchInt64(row.GetInt64(f.column))
	case FloatCol:
		if f.columnIndex != -1 {
			return f.MatchFloat(row.dataFloat[f.columnIndex])
		}
		if f.isEmpty {
			return matchEmptyFilter(f.operator)
		}

		return f.MatchFloat(row.GetFloat(f.column))
	case Int64ListCol:
		return f.MatchInt64List(row.GetInt64List(f.column))
	case CustomVarCol:
		return f.MatchString(row.GetCustomVarValue(f.column, f.customTag))
	case InterfaceListCol:
		return f.MatchInterfaceList(row.GetInterfaceList(f.column))
	case ServiceMemberListCol:
		return f.MatchServiceMemberList(row.GetServiceMemberList(f.column))
	case StringListSortedCol:
		// columns are StringListCol internally after the first sort
		log.Panicf("sorted string list is a virtual column type and not directly used")
	}

	log.Panicf("not implemented filter match type: %s", f.column.DataType.String())

	return false
}

func (f *Filter) MatchInt8(value int8) bool {
	switch f.operator {
	case Equal:
		return value == f.intValue
	case Unequal:
		return value != f.intValue
	case Less:
		return value < f.intValue
	case LessThan:
		return value <= f.intValue
	case Greater:
		return value > f.intValue
	case GreaterThan:
		return value >= f.intValue
	default:
		strVal := fmt.Sprintf("%v", value)

		return f.MatchString(strVal)
	}
}

func (f *Filter) MatchInt64(value int64) bool {
	switch f.operator {
	case Equal:
		return value == f.int64Value
	case Unequal:
		return value != f.int64Value
	case Less:
		return value < f.int64Value
	case LessThan:
		return value <= f.int64Value
	case Greater:
		return value > f.int64Value
	case GreaterThan:
		return value >= f.int64Value
	default:
		strVal := fmt.Sprintf("%v", value)

		return f.MatchString(strVal)
	}
}

func (f *Filter) MatchFloat(value float64) bool {
	switch f.operator {
	case Equal:
		return value == f.floatValue
	case Unequal:
		return value != f.floatValue
	case Less:
		return value < f.floatValue
	case LessThan:
		return value <= f.floatValue
	case Greater:
		return value > f.floatValue
	case GreaterThan:
		return value >= f.floatValue
	default:
		strVal := fmt.Sprintf("%v", value)

		return f.MatchString(strVal)
	}
}

func matchEmptyFilter(operator Operator) bool {
	switch operator {
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
	default:
		log.Warnf("not implemented empty op: %s", operator.String())

		return false
	}
}

func (f *Filter) MatchString(value string) bool {
	return (matchStringVal(value, f.operator, f.stringVal, f.regexp))
}

// regex should be from substr.
func matchStringVal(value string, operator Operator, substr string, regex *regexp.Regexp) bool {
	switch operator {
	case Equal:
		return value == substr
	case Unequal:
		return value != substr
	case EqualNoCase:
		return strings.EqualFold(value, substr)
	case UnequalNoCase:
		return !strings.EqualFold(value, substr)
	case RegexMatch, RegexNoCaseMatch:
		return regex.MatchString(value)
	case RegexMatchNot, RegexNoCaseMatchNot:
		return !regex.MatchString(value)
	case Less:
		return value < substr
	case LessThan:
		return value <= substr
	case Greater:
		return value > substr
	case GreaterThan:
		return value >= substr
	case Contains:
		return strings.Contains(value, substr)
	case ContainsNot:
		return !strings.Contains(value, substr)
	case ContainsNoCase:
		return strings.Contains(strings.ToLower(value), substr)
	case ContainsNoCaseNot:
		return !strings.Contains(strings.ToLower(value), substr)
	default:
		log.Warnf("not implemented string op: %s", operator.String())

		return false
	}
}

func (f *Filter) MatchStringList(list []string) bool {
	switch f.operator {
	case Equal:
		// used to match for empty lists, like: contacts = ""
		// return true if the list is empty
		return f.stringVal == "" && len(list) == 0
	case Unequal:
		// used to match for any entry in lists, like: contacts != ""
		// return true if the list is not empty
		return f.stringVal == "" && len(list) != 0
	case GreaterThan:
		return slices.Contains(list, f.stringVal)
	case GroupContainsNot, LessThan:
		return !slices.Contains(list, f.stringVal)
	case RegexMatch, RegexNoCaseMatch, Contains, ContainsNoCase:
		return slices.ContainsFunc(list, f.MatchString)
	case RegexMatchNot, RegexNoCaseMatchNot, ContainsNot, ContainsNoCaseNot:
		for _, v := range list {
			// MatchString takes operator into account, so negate the result
			// so if it returns false it means the value has been found
			if !f.MatchString(v) {
				return false
			}
		}

		return true
	default:
		log.Warnf("not implemented stringlist op: %s", f.operator.String())

		return false
	}
}

func (f *Filter) MatchInt64List(list []int64) bool {
	switch f.operator {
	case Equal:
		return f.isEmpty && len(list) == 0
	case Unequal:
		return f.isEmpty && len(list) != 0
	case GreaterThan:
		return slices.Contains(list, int64(f.intValue))
	case GroupContainsNot:
		return !slices.Contains(list, int64(f.intValue))
	default:
		log.Warnf("not implemented Int64list op: %s", f.operator.String())

		return false
	}
}

func (f *Filter) MatchCustomVar(value map[string]string) bool {
	val, ok := value[f.customTag]
	if !ok {
		val = ""
	}

	return f.MatchString(val)
}

func (f *Filter) MatchInterfaceList(list []any) bool {
	if f.isEmpty {
		if len(list) == 0 {
			return f.MatchString("")
		}

		return !f.MatchString("")
	}

	switch f.operator {
	case Equal, EqualNoCase, Contains, ContainsNoCase, RegexMatch, RegexNoCaseMatch:
		for _, irow := range list {
			subrow := interface2interfaceList(irow)
			for _, entry := range subrow {
				val := interface2stringNoDedup(entry)
				if f.MatchString(val) {
					return true
				}
			}
		}

		return false
	case Unequal, UnequalNoCase, ContainsNot, ContainsNoCaseNot, RegexMatchNot, RegexNoCaseMatchNot:
		for _, irow := range list {
			subrow := interface2interfaceList(irow)
			for _, entry := range subrow {
				val := interface2stringNoDedup(entry)
				if !f.MatchString(val) {
					return false
				}
			}
		}

		return true
	default:
		log.Warnf("not implemented interfaceList op: %s (%v)", f.operator.String(), f.operator)

		return false
	}
}

func (f *Filter) MatchServiceMemberList(list []ServiceMember) bool {
	switch f.operator {
	case Equal:
		// used to match for empty lists, like: depends_exec = ""
		// return true if the list is empty
		return f.isEmpty && len(list) == 0
	case Unequal:
		// used to match for any entry in lists, like: depends_exec != ""
		// return true if the list is not empty
		return f.isEmpty && len(list) != 0
	case GreaterThan:
		for _, v := range list {
			if v[0] == f.hstVal && v[1] == f.svcVal {
				return true
			}
		}

		return false
	case GroupContainsNot, LessThan:
		for _, v := range list {
			if v[0] == f.hstVal && v[1] == f.svcVal {
				return false
			}
		}

		return true
	case RegexMatch, RegexNoCaseMatch, Contains, ContainsNoCase:
		for _, v := range list {
			if matchStringVal(v[0], f.operator, f.hstVal, f.hstRegexp) && matchStringVal(v[1], f.operator, f.svcVal, f.svcRegexp) {
				return true
			}
		}

		return false
	case RegexMatchNot, RegexNoCaseMatchNot, ContainsNot, ContainsNoCaseNot:
		for _, v := range list {
			// matchStringVal takes operator into account, so negate the result
			// so if it returns false it means the value has been found
			if !matchStringVal(v[0], f.operator, f.hstVal, f.hstRegexp) && !matchStringVal(v[1], f.operator, f.svcVal, f.svcRegexp) {
				return false
			}
		}

		return true
	default:
		log.Warnf("not implemented serviceMemberList op: %s", f.operator.String())

		return false
	}
}

// optimize removes unnecessary filter indentation unless it is negated.
// it also sorts filter for better performance.
func (f *Filter) optimize() *Filter {
	if f.negate {
		return f
	}

	// recursively optimize all filters
	for i := range f.filter {
		f.filter[i] = f.filter[i].optimize()
	}

	if len(f.filter) == 1 {
		return f.filter[0]
	}

	// reorder by complexity
	slices.SortStableFunc(f.filter, func(a, b *Filter) int {
		return a.complexity() - b.complexity()
	})

	return f
}

// complexity calculates complexity of the filter.
func (f *Filter) complexity() int {
	complexity := 0

	for i := range f.filter {
		complexity += f.filter[i].complexity()
	}

	if f.column != nil {
		// add column complexity
		switch f.column.DataType {
		case StringCol:
			complexity += 3
		case StringListCol, StringListSortedCol:
			complexity += 6
		case IntCol, Int64Col, FloatCol:
			complexity++
		case Int64ListCol:
			complexity += 4
		case JSONCol:
			complexity += 5
		case CustomVarCol:
			complexity += 8
		case ServiceMemberListCol:
			complexity += 7
		case InterfaceListCol:
			complexity += 7
		case StringLargeCol:
			complexity += 30
		default:
			log.Panicf("not implemented: %#v", f.column.DataType)
		}

		if f.column.RefCol != nil {
			complexity++
		}
	}

	return complexity
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

	if _, ok := Objects.Tables[table].columnsIndex[fixedColumnName]; ok {
		*columnName = fixedColumnName

		return true
	}

	return false
}

// hasRegexpCharacters returns true if string is a probably a regular expression.
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
		// still contains dots?
		if strings.Contains(test, ".") {
			return true
		}
	}

	return false
}

func createLocalStatsCopy(stats []*Filter) []*Filter {
	localStats := make([]*Filter, len(stats))
	for i, s := range stats {
		localStats[i] = &Filter{}
		localStats[i].statsType = s.statsType
		if s.statsType == Min {
			localStats[i].stats = -1
		}
	}

	return localStats
}
