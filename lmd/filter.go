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
	// filter can either be a single filter
	Column     Column
	Operator   Operator
	StrValue   string
	FloatValue float64
	Regexp     *regexp.Regexp
	CustomTag  string

	// or a group of filters
	Filter        []Filter
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
		for _, sub := range f.Filter {
			str += sub.String(prefix)
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
		break
	case Counter:
		str = fmt.Sprintf("Stats: %s %s%s\n", f.Column.Name, f.Operator.String(), strVal)
		break
	default:
		str = fmt.Sprintf("Stats: %s %s\n", f.StatsType.String(), f.Column.Name)
		break
	}
	return
}

func (f *Filter) strValue() (str string) {
	var value string
	colType := f.Column.Type
	if colType == VirtCol {
		colType = VirtKeyMap[f.Column.Name].Type
	}
	switch colType {
	case CustomVarCol:
		value = f.CustomTag + " " + f.StrValue
		break
	case TimeCol:
		fallthrough
	case IntListCol:
		fallthrough
	case IntCol:
		value = fmt.Sprintf("%d", int(f.FloatValue))
		break
	case FloatCol:
		value = fmt.Sprintf("%v", f.FloatValue)
		break
	case StringListCol:
		fallthrough
	case StringCol:
		value = f.StrValue
		break
	default:
		log.Panicf("not implemented column type: %v", f.Column.Type)
		break
	}

	str = fmt.Sprintf("%v", value)
	return
}

// ParseFilter parses a single line into a filter object.
// It returns any error encountered.
func ParseFilter(value string, line *string, table string, stack *[]Filter) (err error) {
	tmp := strings.SplitN(value, " ", 3)
	if len(tmp) < 2 {
		err = errors.New("bad request: filter header, must be Filter: <field> <operator> <value>")
		return
	}
	if len(tmp) == 2 {
		tmp = append(tmp, "")
	}
	isRegex := false
	var op Operator
	switch tmp[1] {
	case "=":
		op = Equal
		break
	case "=~":
		op = EqualNocase
		break
	case "~":
		op = RegexMatch
		isRegex = true
		break
	case "!~":
		op = RegexMatchNot
		isRegex = true
		break
	case "~~":
		op = RegexNoCaseMatch
		isRegex = true
		break
	case "!~~":
		op = RegexNoCaseMatchNot
		isRegex = true
		break
	case "!=":
		op = Unequal
		break
	case "!=~":
		op = UnequalNocase
		break
	case "<":
		op = Less
		break
	case "<=":
		op = LessThan
		break
	case ">":
		op = Greater
		break
	case ">=":
		op = GreaterThan
		break
	case "!>=":
		op = GroupContainsNot
		break
	default:
		err = errors.New("bad request: unrecognized filter operator: " + tmp[1] + " in " + *line)
		return
	}
	// convert value to type of column
	i, Ok := Objects.Tables[table].ColumnsIndex[tmp[0]]
	if !Ok {
		err = errors.New("bad request: unrecognized column from filter: " + tmp[0] + " in " + *line)
		return
	}
	col := Objects.Tables[table].Columns[i]
	filter := Filter{Operator: op, Column: col}

	colType := col.Type
	if colType == VirtCol {
		colType = VirtKeyMap[col.Name].Type
	}
	switch colType {
	case IntListCol:
		fallthrough
	case TimeCol:
		fallthrough
	case IntCol:
		filtervalue, cerr := strconv.Atoi(tmp[2])
		if cerr != nil {
			err = errors.New("bad request: could not convert " + tmp[2] + " to integer from filter: " + *line)
			return
		}
		filter.FloatValue = float64(filtervalue)
		break
	case FloatCol:
		filtervalue, cerr := strconv.ParseFloat(tmp[2], 64)
		if cerr != nil {
			err = errors.New("bad request: could not convert " + tmp[2] + " to float from filter: " + *line)
			return
		}
		filter.FloatValue = filtervalue
		break
	case CustomVarCol:
		vars := strings.SplitN(tmp[2], " ", 2)
		if len(vars) < 2 {
			err = errors.New("bad request: custom variable filter must have form \"Filter: custom_variables <op> <variable> <value>\" in " + *line)
			return
		}
		filter.StrValue = vars[1]
		filter.CustomTag = vars[0]
		break
	case StringListCol:
		fallthrough
	case StringCol:
		filter.StrValue = tmp[2]
		break
	default:
		log.Panicf("not implemented column type: %v", colType)
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
	*stack = append(*stack, filter)
	return
}

// ParseStats parses a text line into a stats object.
// It returns any error encountered.
func ParseStats(value string, line *string, table string, stack *[]Filter) (err error) {
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
		break
	case "min":
		op = Min
		startWith = -1
		break
	case "max":
		op = Max
		break
	case "sum":
		op = Sum
		break
	default:
		ParseFilter(value, line, table, stack)
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

	stats := Filter{Column: col, StatsType: op, Stats: startWith, StatsCount: 0}
	*stack = append(*stack, stats)
	return
}

// ParseFilterOp parses a text line into a filter group operator like And: <nr>.
// It returns any error encountered.
func ParseFilterOp(header string, value string, line *string, stack *[]Filter) (err error) {
	num, cerr := strconv.Atoi(value)
	if cerr != nil || num < 1 {
		err = fmt.Errorf("bad request: %s must be a positive number in: %s", header, *line)
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
	stackedFilter := Filter{Filter: groupedStack, GroupOperator: op}
	*stack = []Filter{}
	*stack = append(*stack, remainingStack...)
	*stack = append(*stack, stackedFilter)
	return
}

// MatchFilter returns true if the given filter matches the given datarow.
func (peer *Peer) MatchFilter(table *Table, refs *map[string][][]interface{}, inputRowLen int, filter *Filter, row *[]interface{}, rowNum int) bool {
	// recursive group filter
	if len(filter.Filter) > 0 {
		for _, f := range filter.Filter {
			subresult := peer.MatchFilter(table, refs, inputRowLen, &f, row, rowNum)
			if subresult == false && filter.GroupOperator == And {
				return false
			}
			if subresult == true && filter.GroupOperator == Or {
				return true
			}
		}
		// if we did not return yet, this means all AND filter have matched
		if filter.GroupOperator == And {
			return true
		}
		// if we did not return yet, this means no OR filter have matched
		return false
	}

	// normal field filter
	var value interface{}
	if filter.Column.Index < inputRowLen {
		value = (*row)[filter.Column.Index]
	} else {
		value = peer.GetRowValue(filter.Column.Index, row, rowNum, table, refs, inputRowLen)
	}
	colType := filter.Column.Type
	if colType == VirtCol {
		colType = VirtKeyMap[filter.Column.Name].Type
	}
	switch colType {
	case StringCol:
		return matchStringFilter(filter, &value)
	case CustomVarCol:
		return matchCustomVarFilter(filter, &value)
	case TimeCol:
		fallthrough
	case IntCol:
		fallthrough
	case FloatCol:
		if v, ok := value.(float64); ok {
			return matchNumberFilter(filter.Operator, v, filter.FloatValue)
		}
		return matchNumberFilter(filter.Operator, numberToFloat(value), filter.FloatValue)
	case StringListCol:
		return matchStringListFilter(filter, &value)
	case IntListCol:
		return matchIntListFilter(filter, &value)
	}
	log.Panicf("not implemented filter type: %v", filter.Column.Type)
	return false
}

func matchNumberFilter(op Operator, valueA float64, valueB float64) bool {
	switch op {
	case Equal:
		if valueA == valueB {
			return true
		}
		break
	case Unequal:
		if valueA != valueB {
			return true
		}
		break
	case Less:
		if valueA < valueB {
			return true
		}
		break
	case LessThan:
		if valueA <= valueB {
			return true
		}
		break
	case Greater:
		if valueA > valueB {
			return true
		}
		break
	case GreaterThan:
		if valueA >= valueB {
			return true
		}
		break
	default:
		log.Errorf("not implemented op: %v", op)
		return false
	}
	return false
}

func matchStringFilter(filter *Filter, value *interface{}) bool {
	return matchStringValueOperator(filter.Operator, value, &filter.StrValue, filter.Regexp)
}

func matchStringValueOperator(op Operator, valueA *interface{}, valueB *string, regex *regexp.Regexp) bool {
	strA := fmt.Sprintf("%v", *valueA)
	strB := *valueB
	switch op {
	case Equal:
		if strA == strB {
			return true
		}
	case Unequal:
		if strA != strB {
			return true
		}
	case EqualNocase:
		if strings.ToLower(strA) == strings.ToLower(strB) {
			return true
		}
	case UnequalNocase:
		if strings.ToLower(strA) != strings.ToLower(strB) {
			return true
		}
	case RegexMatch:
		if (*regex).MatchString(strA) {
			return true
		}
	case RegexMatchNot:
		if (*regex).MatchString(strA) {
			return false
		}
	case RegexNoCaseMatch:
		if (*regex).MatchString(strings.ToLower(strA)) {
			return true
		}
	case RegexNoCaseMatchNot:
		if (*regex).MatchString(strings.ToLower(strA)) {
			return false
		}
	case Less:
		if strA < strB {
			return true
		}
	case LessThan:
		if strA <= strB {
			return true
		}
	case Greater:
		if strA > strB {
			return true
		}
	case GreaterThan:
		if strA >= strB {
			return true
		}
	default:
		log.Errorf("not implemented op: %v", op)
		return false
	}
	return false
}

func matchStringListFilter(filter *Filter, value *interface{}) bool {
	list := reflect.ValueOf(*value)
	listLen := list.Len()
	switch filter.Operator {
	case Equal:
		if filter.StrValue == "" && listLen == 0 {
			return true
		}
		return false
	case Unequal:
		if filter.StrValue == "" && listLen != 0 {
			return true
		}
		return false
	case GreaterThan:
		for i := 0; i < listLen; i++ {
			if filter.StrValue == list.Index(i).Interface().(string) {
				return true
			}
		}
		return false
	case GroupContainsNot:
		for i := 0; i < listLen; i++ {
			if filter.StrValue == list.Index(i).Interface().(string) {
				return false
			}
		}
		return true
	}
	log.Errorf("not implemented op: %v", filter.Operator)
	return false
}

func matchIntListFilter(filter *Filter, value *interface{}) bool {
	list := reflect.ValueOf(*value)
	listLen := list.Len()
	switch filter.Operator {
	case Equal:
		if filter.StrValue == "" && listLen == 0 {
			return true
		}
		return false
	case Unequal:
		if filter.StrValue == "" && listLen != 0 {
			return true
		}
		return false
	case GreaterThan:
		for i := 0; i < listLen; i++ {
			if filter.FloatValue == numberToFloat(list.Index(i).Interface()) {
				return true
			}
		}
		return false
	case GroupContainsNot:
		for i := 0; i < listLen; i++ {
			if filter.FloatValue == numberToFloat(list.Index(i).Interface()) {
				return false
			}
		}
		return true
	}
	log.Errorf("not implemented op: %v", filter.Operator)
	return false
}

func matchCustomVarFilter(filter *Filter, value *interface{}) bool {
	custommap := (*value).(map[string]interface{})
	val, ok := custommap[filter.CustomTag]
	if !ok {
		val = ""
	}
	return matchStringValueOperator(filter.Operator, &val, &filter.StrValue, filter.Regexp)
}

func numberToFloat(in interface{}) float64 {
	switch v := in.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case bool:
		if v {
			return 1
		}
	}
	return 0
}
