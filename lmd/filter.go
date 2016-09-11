package main

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type StatsType int

const (
	UnknownStatsType StatsType = iota
	Counter
	Sum     // sum
	Average // avg
	Min     // min
	Max     // max
)

type Filter struct {
	// filter can either be a single filter
	Column    Column
	Operator  Operator
	Value     interface{}
	Regexp    *regexp.Regexp
	CustomTag string

	// or a group of filters
	Filter        []Filter
	GroupOperator GroupOperator

	// stats query
	Stats      float64
	StatsCount int
	StatsType  StatsType
}

type Operator int

const (
	UnknownOperator Operator = iota
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

func (f *Filter) String(prefix string) (str string) {
	if len(f.Filter) > 0 {
		for _, sub := range f.Filter {
			str += sub.String(prefix)
		}
		if f.GroupOperator == And {
			str += fmt.Sprintf("%sAnd: %d\n", prefix, len(f.Filter))
		} else {
			str += fmt.Sprintf("%sOr: %d\n", prefix, len(f.Filter))
		}
	} else {
		value := f.Value
		if f.Column.Type == CustomVarCol {
			value = f.CustomTag + " " + f.Value.(string)
		}
		if f.StatsType != UnknownStatsType {
			if f.StatsType == Counter {
				str = fmt.Sprintf("Stats: %s %s %v\n", f.Column.Name, OperatorString(f.Operator), value)
			} else {
				str = fmt.Sprintf("Stats: %s %s\n", StatsTypeString(f.StatsType), f.Column.Name)
			}
		} else {
			if prefix == "" {
				str = fmt.Sprintf("Filter: %s %s %v\n", f.Column.Name, OperatorString(f.Operator), value)
			} else {
				str = fmt.Sprintf("%s: %s %s %v\n", prefix, f.Column.Name, OperatorString(f.Operator), value)
			}
		}
	}
	return
}

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
	op := UnknownOperator
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
	var filtervalue interface{}
	var filtertagname string
	col := Objects.Tables[table].Columns[i]

	colType := col.Type
	if colType == VirtCol {
		colType = VirtKeyMap[col.Name].Type
	}
	switch colType {
	case IntCol:
		var cerr error
		filtervalue, cerr = strconv.Atoi(tmp[2])
		if cerr != nil {
			err = errors.New("bad request: could not convert " + tmp[2] + " to integer from filter: " + *line)
			return
		}
		break
	case FloatCol:
		var cerr error
		filtervalue, cerr = strconv.ParseFloat(tmp[2], 64)
		if cerr != nil {
			err = errors.New("bad request: could not convert " + tmp[2] + " to float from filter: " + *line)
			return
		}
		break
	case CustomVarCol:
		vars := strings.SplitN(tmp[2], " ", 2)
		if len(vars) < 2 {
			err = errors.New("bad request: custom variable filter must have form \"Filter: custom_variables <variable> <op> <value>\" in " + *line)
			return
		}
		filtertagname = vars[0]
		filtervalue = vars[1]
	default:
		filtervalue = tmp[2]
	}
	var regex *regexp.Regexp
	if isRegex {
		var rerr error
		val := filtervalue.(string)
		if op == RegexNoCaseMatchNot || op == RegexNoCaseMatch {
			val = strings.ToLower(val)
		}
		regex, rerr = regexp.Compile(val)
		if rerr != nil {
			err = errors.New("bad request: invalid regular expression: " + rerr.Error() + " in filter " + *line)
			return
		}
	}
	filter := Filter{Operator: op, Value: filtervalue, Column: col, Regexp: regex, CustomTag: filtertagname}
	*stack = append(*stack, filter)
	return
}

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

func ParseFilterOp(header string, value string, line *string, stack *[]Filter) (err error) {
	num, cerr := strconv.Atoi(value)
	if cerr != nil || num < 1 {
		err = errors.New("bad request: " + header + " must be a positive number in" + *line)
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
	stackedFilter := Filter{Filter: groupedStack, GroupOperator: op, Value: nil}
	*stack = []Filter{}
	*stack = append(*stack, remainingStack...)
	*stack = append(*stack, stackedFilter)
	return
}

func (peer *Peer) matchFilter(table *Table, refs *map[string][][]interface{}, inputRowLen int, filter Filter, row *[]interface{}, rowNum int) bool {
	// recursive group filter
	if len(filter.Filter) > 0 {
		for _, f := range filter.Filter {
			subresult := peer.matchFilter(table, refs, inputRowLen, f, row, rowNum)
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
	value := peer.getRowValue(filter.Column.Index, row, rowNum, table, refs, inputRowLen)
	if value == nil {
		panic("shouldn happen")
	}
	colType := filter.Column.Type
	if colType == VirtCol {
		colType = VirtKeyMap[filter.Column.Name].Type
	}
	switch colType {
	case StringCol:
		return matchStringFilter(&filter, &value)
	case CustomVarCol:
		return matchCustomVarFilter(&filter, &value)
	case TimeCol:
		fallthrough
	case IntCol:
		fallthrough
	case FloatCol:
		valueA := NumberToFloat(value)
		valueB := float64(filter.Value.(int))
		return matchNumberFilter(&filter, valueA, valueB)
	case StringListCol:
		return matchStringListFilter(&filter, &value)
	}
	log.Errorf("not implemented type: %v", filter.Column.Type)
	return false
}

func matchNumberFilter(filter *Filter, valueA float64, valueB float64) bool {
	switch filter.Operator {
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
		log.Errorf("not implemented op: %v", filter.Operator)
		return false
	}
	return false
}

func matchStringFilter(filter *Filter, value *interface{}) bool {
	return matchStringValueOperator(filter.Operator, value, &filter.Value, filter.Regexp)
}

func matchStringValueOperator(op Operator, valueA *interface{}, valueB *interface{}, regex *regexp.Regexp) bool {
	switch op {
	case Equal:
		if (*valueA).(string) == (*valueB).(string) {
			return true
		}
	case Unequal:
		if (*valueA).(string) != (*valueB).(string) {
			return true
		}
	case EqualNocase:
		if strings.ToLower((*valueA).(string)) == strings.ToLower((*valueB).(string)) {
			return true
		}
	case UnequalNocase:
		if strings.ToLower((*valueA).(string)) != strings.ToLower((*valueB).(string)) {
			return true
		}
	case RegexMatch:
		if (*regex).MatchString((*valueA).(string)) {
			return true
		}
	case RegexMatchNot:
		if (*regex).MatchString((*valueA).(string)) {
			return false
		}
	case RegexNoCaseMatch:
		if (*regex).MatchString(strings.ToLower((*valueA).(string))) {
			return true
		}
	case RegexNoCaseMatchNot:
		if (*regex).MatchString(strings.ToLower((*valueA).(string))) {
			return false
		}
	case Less:
		if (*valueA).(string) < (*valueB).(string) {
			return true
		}
	case LessThan:
		if (*valueA).(string) <= (*valueB).(string) {
			return true
		}
	case Greater:
		if (*valueA).(string) > (*valueB).(string) {
			return true
		}
	case GreaterThan:
		if (*valueA).(string) >= (*valueB).(string) {
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
		if filter.Value.(string) == "" && listLen == 0 {
			return true
		}
		return false
	case Unequal:
		if filter.Value.(string) == "" && listLen != 0 {
			return true
		}
		return false
	case GreaterThan:
		for i := 0; i < listLen; i++ {
			if filter.Value.(string) == list.Index(i).Interface().(string) {
				return true
			}
		}
		return false
	case GroupContainsNot:
		for i := 0; i < listLen; i++ {
			if filter.Value.(string) == list.Index(i).Interface().(string) {
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
	return matchStringValueOperator(filter.Operator, &val, &filter.Value, filter.Regexp)
}

func NumberToFloat(in interface{}) (out float64) {
	if v, ok := in.(int); ok {
		out = float64(v)
	} else if v, ok := in.(bool); ok {
		if v {
			out = 1
		} else {
			out = 0
		}
	} else {
		out = in.(float64)
	}
	return
}
