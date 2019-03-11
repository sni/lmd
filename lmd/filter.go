package main

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// StatsType is the stats operator.
type StatsType int

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

// String converts a StatsType back to the original string.
func (op *StatsType) String() string {
	switch *op {
	case Average:
		return ("avg")
	case Sum:
		return ("sum")
	case Min:
		return ("min")
	case Max:
		return ("Max")
	}
	log.Panicf("not implemented")
	return ""
}

// Filter defines a single filter object.
type Filter struct {
	noCopy noCopy
	// filter can either be a single filter
	Column     *ResultColumn
	Operator   Operator
	StrValue   string
	FloatValue float64
	Regexp     *regexp.Regexp
	CustomTag  string
	IsEmpty    bool

	// or a group of filters
	Filter        []*Filter
	GroupOperator GroupOperator

	// stats query
	Stats      float64
	StatsCount int
	StatsType  StatsType
}

// Operator defines a filter operator.
type Operator int

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

	switch f.StatsType {
	case NoStats:
		if prefix == "" {
			prefix = "Filter"
		}
		str = fmt.Sprintf("%s: %s %s%s\n", prefix, f.Column.Name, f.Operator.String(), strVal)
	case Counter:
		str = fmt.Sprintf("Stats: %s %s%s\n", f.Column.Name, f.Operator.String(), strVal)
	default:
		str = fmt.Sprintf("Stats: %s %s\n", f.StatsType.String(), f.Column.Name)
	}
	return
}

func (f *Filter) strValue() (str string) {
	colType := f.Column.Type
	if f.IsEmpty {
		str = ""
		if colType == CustomVarCol {
			str = f.CustomTag
		}
		return
	}
	var value string
	if colType == VirtCol {
		colType = f.Column.Column.VirtType
	}
	switch colType {
	case CustomVarCol:
		value = f.CustomTag + " " + f.StrValue
	case TimeCol:
		fallthrough
	case IntListCol:
		fallthrough
	case IntCol:
		value = fmt.Sprintf("%d", int(f.FloatValue))
	case FloatCol:
		value = fmt.Sprintf("%v", f.FloatValue)
	case HashMapCol:
		// not impemented right now
		fallthrough
	case StringListCol:
		fallthrough
	case StringCol:
		value = f.StrValue
	default:
		log.Panicf("not implemented column type: %v", f.Column.Type)
	}

	str = fmt.Sprintf("%v", value)
	return
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
func ParseFilter(value string, line *string, table string, stack *[]*Filter) (err error) {
	tmp := strings.SplitN(value, " ", 3)
	if len(tmp) < 2 {
		err = errors.New("bad request: filter header, must be Filter: <field> <operator> <value>")
		return
	}
	// filter are allowed to be empty
	if len(tmp) == 2 {
		tmp = append(tmp, "")
	}

	op, isRegex, err := parseFilterOp(tmp[1], line)
	if err != nil {
		return
	}

	columnName := tmp[0]

	// convert value to type of column
	i, Ok := Objects.Tables[table].ColumnsIndex[columnName]
	if !Ok {
		if !fixBrokenClientsRequestColumn(&columnName, table) {
			columnName = EMPTY
		}
		i = Objects.Tables[table].ColumnsIndex[columnName]
	}
	col := Objects.Tables[table].Columns[i]
	resCol := &ResultColumn{Name: columnName, Type: col.Type, Index: 0, Column: col}
	filter := Filter{Operator: op, Column: resCol}

	err = filter.setFilterValue(col, tmp[2], line)
	if err != nil {
		return
	}

	if isRegex {
		val := filter.StrValue
		if op == RegexNoCaseMatchNot || op == RegexNoCaseMatch {
			val = strings.ToLower(val)
		}
		regex, rerr := regexp.Compile(val)
		if rerr != nil {
			err = errors.New("bad request: invalid regular expression: " + rerr.Error() + " in filter " + *line)
			return
		}
		filter.Regexp = regex
	}
	*stack = append(*stack, &filter)
	return
}

// setFilterValue converts the text value into the given filters type value
func (f *Filter) setFilterValue(col *Column, strVal string, line *string) (err error) {
	colType := col.Type
	if colType == VirtCol {
		colType = col.VirtType
	}
	if strVal == "" {
		f.IsEmpty = true
	}
	switch colType {
	case IntListCol:
		fallthrough
	case TimeCol:
		fallthrough
	case IntCol:
		filtervalue, cerr := strconv.Atoi(strVal)
		if cerr != nil && !f.IsEmpty {
			err = fmt.Errorf("bad request: could not convert %s to integer from filter: %s", strVal, *line)
			return
		}
		f.FloatValue = float64(filtervalue)
		return
	case FloatCol:
		filtervalue, cerr := strconv.ParseFloat(strVal, 64)
		if cerr != nil && !f.IsEmpty {
			err = fmt.Errorf("bad request: could not convert %s to float from filter: %s", strVal, *line)
			return
		}
		f.FloatValue = filtervalue
		return
	case CustomVarCol:
		vars := strings.SplitN(strVal, " ", 2)
		if vars[0] == "" {
			err = errors.New("bad request: custom variable filter must have form \"Filter: custom_variables <op> <variable> [<value>]\" in " + *line)
			return
		}
		if len(vars) == 1 {
			f.IsEmpty = true
		} else {
			f.StrValue = vars[1]
		}
		f.CustomTag = vars[0]
		return
	case HashMapCol:
		// not impemented right now
		fallthrough
	case StringListCol:
		fallthrough
	case StringCol:
		f.StrValue = strVal
		return
	}
	log.Panicf("not implemented column type: %v", colType)
	return
}

func parseFilterOp(opStr string, line *string) (op Operator, isRegex bool, err error) {
	isRegex = false
	switch opStr {
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
	}
	err = errors.New("bad request: unrecognized filter operator: " + opStr + " in " + *line)
	return
}

// ParseStats parses a text line into a stats object.
// It returns any error encountered.
func ParseStats(value string, line *string, table string, stack *[]*Filter) (err error) {
	tmp := strings.SplitN(value, " ", 3)
	if len(tmp) < 2 {
		err = errors.New("bad request: stats header, must be Stats: <field> <operator> <value> OR Stats: <sum|avg|min|max> <field>")
		return
	}
	startWith := float64(0)
	var op StatsType
	switch strings.ToLower(tmp[0]) {
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
		err = ParseFilter(value, line, table, stack)
		if err != nil {
			return
		}
		// set last one to counter
		(*stack)[len(*stack)-1].StatsType = Counter
		return
	}

	i, Ok := Objects.Tables[table].ColumnsIndex[tmp[1]]
	if !Ok {
		err = errors.New("bad request: unrecognized column from stats: " + tmp[1] + " in " + *line)
		return
	}
	col := Objects.Tables[table].Columns[i]
	resCol := &ResultColumn{Name: col.Name, Type: col.Type, Index: 0, Column: col}
	stats := &Filter{Column: resCol, StatsType: op, Stats: startWith, StatsCount: 0}
	*stack = append(*stack, stats)
	return
}

// ParseFilterOp parses a text line into a filter group operator like And: <nr>.
// It returns any error encountered.
func ParseFilterOp(header string, value string, line *string, stack *[]*Filter) (err error) {
	num, cerr := strconv.Atoi(value)
	if cerr != nil || num < 0 {
		err = fmt.Errorf("bad request: %s must be a positive number in: %s", header, *line)
		return
	}
	if num == 0 {
		if log.IsV(2) {
			log.Debugf("Ignoring %s as value is not positive", *line)
		}
		return
	}
	stackLen := len(*stack)
	if stackLen < num {
		err = errors.New("bad request: not enough filter on stack in " + *line)
		return
	}
	// remove x entrys from stack and combine them to a new group
	groupedStack, remainingStack := (*stack)[stackLen-num:], (*stack)[:stackLen-num]
	op := Or
	if header == "and" {
		op = And
	}
	stackedFilter := &Filter{Filter: groupedStack, GroupOperator: op}
	*stack = []*Filter{}
	*stack = append(*stack, remainingStack...)
	*stack = append(*stack, stackedFilter)
	return
}

// MatchFilter returns true if the given filter matches the given value.
func (f *Filter) MatchFilter(value *interface{}) bool {
	colType := f.Column.Type
	if colType == VirtCol {
		colType = f.Column.Column.VirtType
	}
	switch colType {
	case StringCol:
		return matchStringFilter(f, value)
	case CustomVarCol:
		return matchCustomVarFilter(f, value)
	case TimeCol:
		fallthrough
	case IntCol:
		fallthrough
	case FloatCol:
		if f.IsEmpty {
			return matchEmptyFilter(f.Operator)
		}
		if v, ok := (*value).(float64); ok {
			// inline matchNumberFilter
			switch f.Operator {
			case Equal:
				return v == f.FloatValue
			case Unequal:
				return v != f.FloatValue
			case Less:
				return v < f.FloatValue
			case LessThan:
				return v <= f.FloatValue
			case Greater:
				return v > f.FloatValue
			case GreaterThan:
				return v >= f.FloatValue
			}
		}
		return matchNumberFilter(f.Operator, numberToFloat(value), f.FloatValue)
	case HashMapCol:
		// not implemented
		fallthrough
	case StringListCol:
		return matchStringListFilter(f, value)
	case IntListCol:
		return matchIntListFilter(f, value)
	}
	log.Panicf("not implemented filter type: %v", colType)
	return false
}

func matchNumberFilter(op Operator, valueA float64, valueB float64) bool {
	switch op {
	case Equal:
		return valueA == valueB
	case Unequal:
		return valueA != valueB
	case Less:
		return valueA < valueB
	case LessThan:
		return valueA <= valueB
	case Greater:
		return valueA > valueB
	case GreaterThan:
		return valueA >= valueB
	}
	log.Warnf("not implemented op: %v", op)
	return false
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
	log.Warnf("not implemented op: %v", op)
	return false
}

func matchStringFilter(filter *Filter, value *interface{}) bool {
	return matchStringValueOperator(filter.Operator, value, &filter.StrValue, filter.Regexp)
}

func matchStringValueOperator(op Operator, valueA *interface{}, valueB *string, regex *regexp.Regexp) bool {
	var strA string
	switch s := (*valueA).(type) {
	case string:
		strA = s
	case nil:
		strA = ""
	default:
		strA = fmt.Sprintf("%v", *valueA)
	}
	strB := *valueB
	switch op {
	case Equal:
		return strA == strB
	case Unequal:
		return strA != strB
	case EqualNocase:
		return strings.EqualFold(strA, strB)
	case UnequalNocase:
		return !strings.EqualFold(strA, strB)
	case RegexMatch:
		return (*regex).MatchString(strA)
	case RegexMatchNot:
		return !(*regex).MatchString(strA)
	case RegexNoCaseMatch:
		return (*regex).MatchString(strings.ToLower(strA))
	case RegexNoCaseMatchNot:
		return !(*regex).MatchString(strings.ToLower(strA))
	case Less:
		return strA < strB
	case LessThan:
		return strA <= strB
	case Greater:
		return strA > strB
	case GreaterThan:
		return strA >= strB
	}
	log.Warnf("not implemented op: %v", op)
	return false
}

func matchStringListFilter(filter *Filter, value *interface{}) bool {
	if *value == nil || reflect.TypeOf(*value).Kind() != reflect.Slice {
		*value = make([]string, 0)
	}
	list := reflect.ValueOf(*value)
	listLen := list.Len()
	switch filter.Operator {
	case Equal:
		// used to match for empty lists, like: contacts = ""
		// return true if the list is empty
		return filter.StrValue == "" && listLen == 0
	case Unequal:
		// used to match for any entry in lists, like: contacts != ""
		// return true if the list is not empty
		return filter.StrValue == "" && listLen != 0
	case GreaterThan:
		for i := 0; i < listLen; i++ {
			if s, ok := list.Index(i).Interface().(string); ok {
				if filter.StrValue == s {
					return true
				}
			}
		}
		return false
	case GroupContainsNot:
		for i := 0; i < listLen; i++ {
			if s, ok := list.Index(i).Interface().(string); ok {
				if filter.StrValue == s {
					return false
				}
			}
		}
		return true
	case RegexMatch:
		fallthrough
	case RegexNoCaseMatch:
		for i := 0; i < listLen; i++ {
			listVal := list.Index(i).Interface()
			if matchStringValueOperator(filter.Operator, &listVal, &filter.StrValue, filter.Regexp) {
				return true
			}
		}
		return false
	case RegexMatchNot:
		fallthrough
	case RegexNoCaseMatchNot:
		for i := 0; i < listLen; i++ {
			listVal := list.Index(i).Interface()
			if matchStringValueOperator(filter.Operator, &listVal, &filter.StrValue, filter.Regexp) {
				return false
			}
		}
		return true
	}
	log.Warnf("not implemented op: %v", filter.Operator)
	return false
}

func matchIntListFilter(filter *Filter, value *interface{}) bool {
	if *value == nil || reflect.TypeOf(*value).Kind() != reflect.Slice {
		*value = make([]float64, 0)
	}
	list := reflect.ValueOf(*value)
	listLen := list.Len()
	switch filter.Operator {
	case Equal:
		return filter.IsEmpty && listLen == 0
	case Unequal:
		return filter.IsEmpty && listLen != 0
	case GreaterThan:
		for i := 0; i < listLen; i++ {
			val := list.Index(i).Interface()
			if filter.FloatValue == numberToFloat(&val) {
				return true
			}
		}
		return false
	case GroupContainsNot:
		for i := 0; i < listLen; i++ {
			val := list.Index(i).Interface()
			if filter.FloatValue == numberToFloat(&val) {
				return false
			}
		}
		return true
	}
	log.Warnf("not implemented op: %v", filter.Operator)
	return false
}

func matchCustomVarFilter(filter *Filter, value *interface{}) bool {
	custommap := interfaceToCustomVarHash(value)
	val, ok := (*custommap)[filter.CustomTag]
	if !ok {
		val = ""
	}
	return matchStringValueOperator(filter.Operator, &val, &filter.StrValue, filter.Regexp)
}

// interfaceToCustomVarHash converts an interface to a hashmap
//
// usually custom variables come in the form of a simple hash:
// {key: value}
// however icinga2 sends a list which can be any of:
// [], [null], [[key, value]]
//
func interfaceToCustomVarHash(in *interface{}) *map[string]interface{} {
	if custommap, ok := (*in).(map[string]interface{}); ok {
		return (&custommap)
	}
	if customlist, ok := (*in).([]interface{}); ok {
		val := make(map[string]interface{})
		for _, tupelInterface := range customlist {
			if tupel, ok := tupelInterface.([]interface{}); ok {
				if len(tupel) == 2 {
					if s, ok := tupel[1].(string); ok {
						val[strings.ToUpper(tupel[0].(string))] = s
					} else {
						val[strings.ToUpper(tupel[0].(string))] = fmt.Sprintf("%v", tupel[1])
					}
				}
			}
		}
		return (&val)
	}
	val := make(map[string]interface{})
	return &val
}

func numberToFloat(in *interface{}) float64 {
	switch v := (*in).(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case bool:
		if v {
			return 1
		}
	case string:
		val, _ := strconv.ParseFloat(v, 64)
		return val
	default:
		val, _ := strconv.ParseFloat(fmt.Sprintf("%v", v), 64)
		return val
	}
	return 0
}

// some broken clients request service_description instead of just description from the services table
// be nice to them as well...
func fixBrokenClientsRequestColumn(columnName *string, table string) bool {
	fixedColumnName := *columnName

	switch table {
	case SERVICES:
		fixedColumnName = strings.TrimPrefix(fixedColumnName, "service_")
	case HOSTS:
		fixedColumnName = strings.TrimPrefix(fixedColumnName, "host_")
	}

	if _, Ok := Objects.Tables[table].ColumnsIndex[fixedColumnName]; Ok {
		*columnName = fixedColumnName
		return true
	}

	return false
}
