// Code generated by "stringer -type=DataType"; DO NOT EDIT.

package lmd

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[StringCol-1]
	_ = x[StringListCol-2]
	_ = x[IntCol-3]
	_ = x[Int64Col-4]
	_ = x[Int64ListCol-5]
	_ = x[FloatCol-6]
	_ = x[JSONCol-7]
	_ = x[CustomVarCol-8]
	_ = x[ServiceMemberListCol-9]
	_ = x[InterfaceListCol-10]
	_ = x[StringLargeCol-11]
}

const _DataType_name = "StringColStringListColIntColInt64ColInt64ListColFloatColJSONColCustomVarColServiceMemberListColInterfaceListColStringLargeCol"

var _DataType_index = [...]uint8{0, 9, 22, 28, 36, 48, 56, 63, 75, 95, 111, 125}

func (i DataType) String() string {
	i -= 1
	if i >= DataType(len(_DataType_index)-1) {
		return "DataType(" + strconv.FormatInt(int64(i+1), 10) + ")"
	}
	return _DataType_name[_DataType_index[i]:_DataType_index[i+1]]
}
