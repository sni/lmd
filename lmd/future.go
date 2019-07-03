package main

import "unicode/utf8"

// bytes.ToValidUTF8 replaces invalid utf8 characters
// taken from go 1.13 beta
// https://github.com/golang/go/commit/3259bc441957bf74f069cf7df961367a3472afb2
// https://github.com/golang/go/issues/25805
// enhanced with removing ctrl characters like in
// https://rosettacode.org/wiki/Strip_control_codes_and_extended_characters_from_a_string#Go
func bytesToValidUTF8(s, replacement []byte) []byte {
	b := make([]byte, 0, len(s)+len(replacement))
	invalid := false // previous byte was from an invalid UTF-8 sequence
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf && c >= 32 && c != 127 {
			i++
			invalid = false
			b = append(b, c)
			continue
		}
		_, wid := utf8.DecodeRune(s[i:])
		if wid == 1 {
			i++
			if !invalid {
				invalid = true
				b = append(b, replacement...)
			}
			continue
		}
		invalid = false
		b = append(b, s[i:i+wid]...)
		i += wid
	}
	return b
}
