package lmd

import "unicode/utf8"

// bytes.ToValidUTF8 replaces invalid utf8 characters
// taken from go 1.13 beta
// https://github.com/golang/go/commit/3259bc441957bf74f069cf7df961367a3472afb2
// https://github.com/golang/go/issues/25805
// enhanced with removing ctrl characters like in
// https://rosettacode.org/wiki/Strip_control_codes_and_extended_characters_from_a_string#Go
func bytesToValidUTF8(src, replacement []byte) []byte {
	result := make([]byte, 0, len(src)+len(replacement))
	invalid := false // previous byte was from an invalid UTF-8 sequence
	for idx := 0; idx < len(src); {
		c := src[idx]
		if c < utf8.RuneSelf && c >= 32 && c != 127 {
			idx++
			invalid = false
			result = append(result, c)

			continue
		}
		_, wid := utf8.DecodeRune(src[idx:])
		if wid == 1 {
			idx++
			if !invalid {
				invalid = true
				result = append(result, replacement...)
			}

			continue
		}
		invalid = false
		result = append(result, src[idx:idx+wid]...)
		idx += wid
	}

	return result
}
