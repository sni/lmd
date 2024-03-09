package lmd

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
type Filter struct {
	noCopy noCopy
	// filter can either be a single filter
	Column     *Column
	Operator   Operator
	StrValue   string
	FloatValue float64
	IntValue   int
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
	StatsPos   int // position in stats result array

	// copy of Column.Optional
	ColumnOptional OptionalFlags
	// copy of Column.Index if Column is of type LocalStore
	ColumnIndex int
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
	EqualNocase   // =~
	UnequalNocase // !=~

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
	strNegate := ""

	if f.Negate {
		if f.StatsType == NoStats {
			strNegate = fmt.Sprintf("%s\n", "Negate:")
		} else {
			strNegate = fmt.Sprintf("%s\n", "StatsNegate:")
		}
	}

	if f.GroupOperator == And || f.GroupOperator == Or {
		if len(f.Filter) > 0 {
			for i := range f.Filter {
				str += f.Filter[i].String(prefix)
			}
			str += fmt.Sprintf("%s%s: %d\n", prefix, f.GroupOperator.String(), len(f.Filter))
			str += strNegate

			return str
		}
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
	case StatsGroup:
		if prefix == "" {
			prefix = "Filter"
		}
		str = fmt.Sprintf("%sGroup: %s %s%s\n", prefix, colName, f.Operator.String(), strVal)
	case Counter:
		str = fmt.Sprintf("Stats: %s %s%s\n", colName, f.Operator.String(), strVal)
	default:
		str = fmt.Sprintf("Stats: %s %s\n", f.StatsType.String(), colName)
	}
	str += strNegate

	return str
}

// Equals returns true if both filter are exactly identical.
func (f *Filter) Equals(other *Filter) bool {
	if f.Column != other.Column {
		return false
	}
	if f.Operator != other.Operator {
		return false
	}
	if f.StrValue != other.StrValue {
		return false
	}
	if f.Negate != other.Negate {
		return false
	}
	if f.FloatValue != other.FloatValue {
		return false
	}
	if f.StatsType != other.StatsType {
		return false
	}
	if f.GroupOperator != other.GroupOperator {
		return false
	}
	if len(f.Filter) != len(other.Filter) {
		return false
	}
	for i := range f.Filter {
		if !f.Filter[i].Equals(other.Filter[i]) {
			return false
		}
	}

	return true
}

func (f *Filter) strValue() string {
	colType := f.Column.DataType
	if f.IsEmpty {
		return f.CustomTag
	}
	var value string
	switch colType {
	case CustomVarCol:
		value = f.CustomTag + " " + f.StrValue
	case Int64ListCol,
		IntCol, Int64Col,
		FloatCol,
		StringListCol,
		ServiceMemberListCol,
		InterfaceListCol,
		JSONCol,
		StringLargeCol,
		StringCol:
		value = f.StrValue
	default:
		log.Panicf("not implemented column type: %v", f.Column.DataType)
	}

	return value
}

// ApplyValue add the given value to this stats filter.
func (f *Filter) ApplyValue(val float64, count int) {
	switch f.StatsType {
	case Counter:
		f.Stats += float64(count)
	case Average, Sum:
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
		Operator:       operator,
		Column:         col,
		Negate:         false,
		ColumnIndex:    -1,
		ColumnOptional: col.Optional,
	}

	err = filter.setFilterValue(string(tmp[2]))
	if err != nil {
		return err
	}

	if options&ParseOptimize != 0 {
		filter.setLowerCaseColumn()
		col = filter.Column // might have changed
	}

	if isRegex {
		err = filter.setRegexFilter(options)
		if err != nil {
			return err
		}
	}

	if !filter.IsEmpty && col.Optional == NoFlags && col.StorageType == LocalStore {
		filter.ColumnIndex = col.Index
	}

	*stack = append(*stack, filter)

	return nil
}

// setFilterValue converts the text value into the given filters type value.
func (f *Filter) setRegexFilter(options ParseOptions) error {
	val := strings.TrimPrefix(f.StrValue, ".*")
	val = strings.TrimSuffix(val, ".*")

	// special case Filter: host_name ~ ^name$
	if options&ParseOptimize != 0 && f.Operator == RegexMatch && strings.HasPrefix(val, "^") && strings.HasSuffix(val, "$") {
		val2 := strings.TrimPrefix(val, "^")
		val2 = strings.TrimSuffix(val2, "$")
		if !hasRegexpCharacters(val2) {
			f.Operator = Equal
			f.StrValue = val2
		}
	}

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
		default:
			// only relevant for regex operators
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

// setFilterValue converts the text value into the given filters type value.
func (f *Filter) setFilterValue(strVal string) (err error) {
	colType := f.Column.DataType
	if strVal == "" {
		f.IsEmpty = true
	}
	f.StrValue = strVal
	switch colType {
	case IntCol, Int64Col, Int64ListCol, FloatCol:
		switch f.Operator {
		case Equal, Unequal, Greater, GreaterThan, Less, LessThan:
			if !f.IsEmpty {
				filterValue, cerr := strconv.ParseFloat(strVal, 64)
				if cerr != nil {
					return fmt.Errorf("could not convert %s to number in filter: %s", strVal, f.String(""))
				}
				f.FloatValue = filterValue
				f.IntValue = int(filterValue)
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
			f.IsEmpty = true
		} else {
			f.StrValue = vars[1]
		}
		f.CustomTag = vars[0]

		return nil
	case InterfaceListCol:
		return nil
	case StringListCol:
		return nil
	case ServiceMemberListCol:
		return nil
	case JSONCol:
		return nil
	case StringLargeCol:
		return nil
	case StringCol:
		return nil
	}

	log.Panicf("not implemented column type: %v", colType)

	return nil
}

// setLowerCaseColumn tries to use the lowercase column if possible.
func (f *Filter) setLowerCaseColumn() {
	col := f.Column
	table := col.Table
	// only hosts and services tables have lower case cache fields
	if table.Name != TableHosts && table.Name != TableServices {
		return
	}
	// lower case fields will only be used for case-insensitive operators
	var operator Operator
	switch f.Operator {
	default:
		return
	case ContainsNoCase:
		operator = Contains
	case ContainsNoCaseNot:
		operator = ContainsNot
	case RegexNoCaseMatch:
		operator = RegexMatch
	case RegexNoCaseMatchNot:
		operator = RegexMatchNot
	}
	col, ok := table.ColumnsIndex[col.Name+"_lc"]
	if !ok {
		return
	}
	f.Column = col
	f.Operator = operator
	f.StrValue = strings.ToLower(f.StrValue)
}

func parseFilterOp(raw []byte) (op Operator, isRegex bool, err error) {
	switch string(raw) {
	case "=":
		return Equal, false, nil
	case "=~":
		return EqualNocase, false, nil
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
		return UnequalNocase, false, nil
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
		(*stack)[len(*stack)-1].StatsType = Counter

		return nil
	}

	col := Objects.Tables[table].GetColumnWithFallback(string(tmp[1]))
	stats := &Filter{
		Column:         col,
		StatsType:      statsOp,
		Stats:          startWith,
		StatsCount:     0,
		ColumnIndex:    -1,
		ColumnOptional: col.Optional,
	}
	if !stats.IsEmpty && col.Optional == NoFlags && col.StorageType == LocalStore {
		stats.ColumnIndex = col.Index
	}

	*stack = append(*stack, stats)

	return nil
}

// ParseFilterOp parses a text line into a filter group operator like And: <nr>.
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
	// remove x entrys from stack and combine them to a new group
	groupedStack, remainingStack := (*stack)[stackLen-num:], (*stack)[:stackLen-num]
	stackedFilter := &Filter{Filter: groupedStack, GroupOperator: groupOp}
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

	stack[stackLen-1].Negate = true

	return
}

// Match returns true if the given filter matches the given value.
func (f *Filter) Match(row *DataRow) bool {
	switch f.Column.DataType {
	case StringCol:
		if f.ColumnIndex != -1 {
			return f.MatchString(row.dataString[f.ColumnIndex])
		}

		return f.MatchString(row.GetString(f.Column))
	case StringLargeCol, JSONCol:
		return f.MatchString(row.GetString(f.Column))
	case StringListCol:
		return f.MatchStringList(row.GetStringList(f.Column))
	case IntCol:
		if f.ColumnIndex != -1 {
			return f.MatchInt(row.dataInt[f.ColumnIndex])
		}
		if f.IsEmpty {
			return matchEmptyFilter(f.Operator)
		}

		return f.MatchInt(row.GetInt(f.Column))
	case Int64Col:
		if f.ColumnIndex != -1 {
			return f.MatchInt64(row.dataInt64[f.ColumnIndex])
		}
		if f.IsEmpty {
			return matchEmptyFilter(f.Operator)
		}

		return f.MatchInt64(row.GetInt64(f.Column))
	case FloatCol:
		if f.ColumnIndex != -1 {
			return f.MatchFloat(row.dataFloat[f.ColumnIndex])
		}
		if f.IsEmpty {
			return matchEmptyFilter(f.Operator)
		}

		return f.MatchFloat(row.GetFloat(f.Column))
	case Int64ListCol:
		return f.MatchInt64List(row.GetInt64List(f.Column))
	case CustomVarCol:
		return f.MatchString(row.GetCustomVarValue(f.Column, f.CustomTag))
	case InterfaceListCol, ServiceMemberListCol:
		// not implemented
		return false
	}

	log.Panicf("not implemented filter match type: %s", f.Column.DataType.String())

	return false
}

func (f *Filter) MatchInt(value int) bool {
	switch f.Operator {
	case Equal:
		return value == f.IntValue
	case Unequal:
		return value != f.IntValue
	case Less:
		return value < f.IntValue
	case LessThan:
		return value <= f.IntValue
	case Greater:
		return value > f.IntValue
	case GreaterThan:
		return value >= f.IntValue
	default:
		strVal := fmt.Sprintf("%v", value)

		return f.MatchString(strVal)
	}
}

func (f *Filter) MatchInt64(value int64) bool {
	switch f.Operator {
	case Equal:
		return value == int64(f.IntValue)
	case Unequal:
		return value != int64(f.IntValue)
	case Less:
		return value < int64(f.IntValue)
	case LessThan:
		return value <= int64(f.IntValue)
	case Greater:
		return value > int64(f.IntValue)
	case GreaterThan:
		return value >= int64(f.IntValue)
	default:
		strVal := fmt.Sprintf("%v", value)

		return f.MatchString(strVal)
	}
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
	switch f.Operator {
	case Equal:
		return value == f.StrValue
	case Unequal:
		return value != f.StrValue
	case EqualNocase:
		return strings.EqualFold(value, f.StrValue)
	case UnequalNocase:
		return !strings.EqualFold(value, f.StrValue)
	case RegexMatch, RegexNoCaseMatch:
		return f.Regexp.MatchString(value)
	case RegexMatchNot, RegexNoCaseMatchNot:
		return !f.Regexp.MatchString(value)
	case Less:
		return value < f.StrValue
	case LessThan:
		return value <= f.StrValue
	case Greater:
		return value > f.StrValue
	case GreaterThan:
		return value >= f.StrValue
	case Contains:
		return strings.Contains(value, f.StrValue)
	case ContainsNot:
		return !strings.Contains(value, f.StrValue)
	case ContainsNoCase:
		return strings.Contains(strings.ToLower(value), f.StrValue)
	case ContainsNoCaseNot:
		return !strings.Contains(strings.ToLower(value), f.StrValue)
	default:
		log.Warnf("not implemented string op: %s", f.Operator.String())

		return false
	}
}

func (f *Filter) MatchStringList(list []string) bool {
	switch f.Operator {
	case Equal:
		// used to match for empty lists, like: contacts = ""
		// return true if the list is empty
		return f.StrValue == "" && len(list) == 0
	case Unequal:
		// used to match for any entry in lists, like: contacts != ""
		// return true if the list is not empty
		return f.StrValue == "" && len(list) != 0
	case GreaterThan:
		for _, v := range list {
			if f.StrValue == v {
				return true
			}
		}

		return false
	case GroupContainsNot, LessThan:
		for _, v := range list {
			if f.StrValue == v {
				return false
			}
		}

		return true
	case RegexMatch, RegexNoCaseMatch, Contains, ContainsNoCase:
		for _, v := range list {
			if f.MatchString(v) {
				return true
			}
		}

		return false
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
		log.Warnf("not implemented stringlist op: %s", f.Operator.String())

		return false
	}
}

func (f *Filter) MatchInt64List(list []int64) bool {
	switch f.Operator {
	case Equal:
		return f.IsEmpty && len(list) == 0
	case Unequal:
		return f.IsEmpty && len(list) != 0
	case GreaterThan:
		fVal := int64(f.IntValue)
		for i := range list {
			if fVal == list[i] {
				return true
			}
		}

		return false
	case GroupContainsNot:
		fVal := int64(f.IntValue)
		for i := range list {
			if fVal == list[i] {
				return false
			}
		}

		return true
	default:
		log.Warnf("not implemented Int64list op: %s", f.Operator.String())

		return false
	}
}

func (f *Filter) MatchCustomVar(value map[string]string) bool {
	val, ok := value[f.CustomTag]
	if !ok {
		val = ""
	}

	return f.MatchString(val)
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
