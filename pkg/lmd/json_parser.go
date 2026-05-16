package lmd

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/willabides/rjson"
)

const DefaultJSONBufferSize = 1e6 // use 1mb buffer

var jsonReadBufferSize atomic.Int32

func init() {
	jsonReadBufferSize.Store(DefaultJSONBufferSize)
}

// parses json resultset from given bytes. Returns the resultset, remaining bytes and error if any.
func parseJSONResult(data []byte) (res ResultSet, remaining []byte, err error) {
	res = make(ResultSet, 0)
	rowNum := 0

	finalPos := 0
	data, trim := trimLeftTracking(data)
	finalPos += trim

	if len(data) < 1 {
		return nil, nil, fmt.Errorf("empty json data")
	}
	if data[0] != '[' {
		return nil, nil, fmt.Errorf("json data should start with '['")
	}

	// remove leading '['
	data, trim = trimLeftTracking(data[1:])
	finalPos += trim + 1
	var linePos int

	json := &rjson.ValueReader{}
	for {
		// final ] marks end of the outer array
		if len(data) >= 1 && data[0] == ']' {
			data, trim = trimLeftTracking(data[1:])
			finalPos += trim + 1

			break
		}

		rowNum++
		row, pos, jErr := json.ReadArray(data)
		linePos = pos
		if jErr != nil {
			var newPos int
			errPos := pos
			for {
				tryRecoverJSON(data[errPos:], jErr)
				row, newPos, jErr = json.ReadArray(data)
				if jErr == nil {
					err = nil

					break
				}
				err = jErr
				// position of error did not advance
				if newPos == errPos {
					break
				}
			}
			pos = newPos
			linePos = pos
			finalPos += pos
			if err != nil {
				break
			}
		}
		finalPos += pos
		res = append(res, row)

		data, trim = trimLeftTracking(data[pos:])
		finalPos += trim

		// trim trailing comma
		if len(data) >= 1 && data[0] == ',' {
			data, trim = trimLeftTracking(data[1:])
			finalPos += trim + 1
		}
	}

	if err != nil {
		return nil,
			nil,
			fmt.Errorf(
				"json parse error at row %d pos %d (byte offset %d): %s",
				rowNum,
				linePos,
				finalPos+1,
				err.Error(),
			)
	}

	return res, data, nil
}

// parse response from bytes when using the wrapped json format.
func parseWrappedJSONMeta(resBytes []byte, meta *ResultMetaData, req *Request) (res ResultSet, err error) {
	resBytes, _ = trimLeftTracking(resBytes)
	if len(resBytes) == 0 || resBytes[0] != '{' {
		return nil, &PeerError{msg: "json parse error: expected {", kind: ResponseError, req: req, resBytes: resBytes}
	}

	pre, resData, found := bytes.Cut(resBytes, []byte("\"data\":"))
	if !found {
		return nil, &PeerError{msg: "json parse error: expected \"data\":", kind: ResponseError, req: req, resBytes: resBytes}
	}

	res, post, err := parseJSONResult(resData)
	if err != nil {
		return nil, &PeerError{msg: fmt.Sprintf("json parse error: %s", err.Error()), kind: ResponseError, req: req, resBytes: resBytes}
	}
	post, _ = trimLeftTracking(post)
	if (len(pre) < 3 && len(post) > 1) && post[0] == ',' {
		post = post[1:]
		post, _ = trimLeftTracking(post)
	}

	json := &rjson.ValueReader{}
	totalRemainingLen := len(pre) + len(post)
	if totalRemainingLen > MaxJSONMetaResponseSize { // should not be very large
		return nil, &PeerError{
			msg:      fmt.Sprintf("json parse error: meta response size exceeds maximum allowed size: %d", totalRemainingLen),
			kind:     ResponseError,
			req:      req,
			resBytes: resBytes,
		}
	}
	remaining := make([]byte, 0, totalRemainingLen)
	remaining = append(remaining, pre...)
	remaining = append(remaining, post...)
	wrapped, _, err := json.ReadObject(remaining)
	if err != nil {
		return nil, &PeerError{msg: fmt.Sprintf("json parse error: %s", err.Error()), kind: ResponseError, req: req, resBytes: resBytes}
	}

	for key, value := range wrapped {
		switch key {
		case "total_count":
			meta.Total = interface2int64(value)
		case "columns":
			meta.Columns = interface2stringListNoDedup(value)
		case "rows_scanned":
			meta.RowsScanned = interface2int64(value)
		case "failed":
			// ignored, contains backend ids which failed
		}
	}

	return res, nil
}

// parseResponseFixedSize parses response from connection with fixed expected size.
func parseResponseFixedSize(conn io.Reader, clb RowResultCB, maxReadSize int) (data []byte, totalBytesRead int, err error) {
	return parseResponseMaxSizeCB(conn, clb, maxReadSize)
}

// parseResponseUndefinedSize parses response from connection with unknown size.
func parseResponseUndefinedSize(conn io.Reader, clb RowResultCB) (data []byte, totalBytesRead int, err error) {
	return parseResponseMaxSizeCB(conn, clb, -1)
}

// parseResponseMaxSize parses response from connection with unknown size up to max size (can be -1 for unlimited).
func parseResponseMaxSize(conn io.Reader, maxReadSize int) (data []byte, totalBytesRead int, err error) {
	body := new(bytes.Buffer)
	for {
		_, cErr := io.CopyN(body, conn, int64(maxReadBytes(totalBytesRead, maxReadSize, 65536)))
		if cErr != nil {
			if !errors.Is(cErr, io.EOF) {
				return nil, 0, fmt.Errorf("io.CopyN: %w", cErr)
			}

			break
		}
	}

	return body.Bytes(), body.Len(), nil
}

// parseResponseMaxSizeCB parses response from connection with unknown size up to max size (can be -1 for unlimited).
//
//nolint:gocognit,gocyclo // cannot be refactored easily
func parseResponseMaxSizeCB(conn io.Reader, clb RowResultCB, maxReadSize int) (data []byte, totalBytesRead int, err error) {
	if clb == nil {
		return parseResponseMaxSize(conn, maxReadSize)
	}

	bytesReadForRow := 0
	started := false
	defaultBufferSize := int(jsonReadBufferSize.Load())
	bufferSize := min(defaultBufferSize, maxReadSize)
	if maxReadSize == -1 {
		bufferSize = defaultBufferSize
	}
	buf := bufio.NewReaderSize(conn, bufferSize)
	json := &rjson.ValueReader{}
	var row []any

reading:
	for {
		data, err = streamReadMore(buf, data, totalBytesRead+bytesReadForRow, maxReadSize, bufferSize)
		if err != nil {
			return nil, 0, fmt.Errorf("read error: %s", err.Error())
		}
		if len(data) == 0 {
			break
		}

		var trim int
		data, trim = trimLeftTracking(data)
		bytesReadForRow += trim

		// must start with opening bracket
		if !started {
			if len(data) < 1 {
				return nil, 0, fmt.Errorf("empty json data")
			}
			if data[0] != '[' {
				return nil, 0, fmt.Errorf("json data should start with '['")
			}

			// remove leading '['
			data, trim = trimLeftTracking(data[1:])
			bytesReadForRow += trim + 1
			started = true
		}

		if len(data) == 0 {
			continue
		}

		for {
			// final ] marks end of the outer array
			if len(data) >= 1 && data[0] == ']' {
				_, trim = trimLeftTracking(data[1:])
				bytesReadForRow += trim + 1

				break reading
			}

			// send previous row along with the num of bytes parsed till here
			if row != nil {
				if cErr := clb(row, bytesReadForRow); cErr != nil {
					return nil, 0, cErr
				}
				totalBytesRead += bytesReadForRow
				bytesReadForRow = 0 // reset counter
				row = nil           //nolint:ineffassign,wastedassign // clean memory early
			}

			var jErr error
			var pos int
			row, pos, jErr = json.ReadArray(data)
			if jErr != nil { //nolint:nestif // cannot be refactored easily
				var newPos int
				errPos := pos
				for {
					// try to append more data, error might result from incomplete data for current row
					data, err = streamReadMore(buf, data, totalBytesRead+bytesReadForRow, maxReadSize, bufferSize)
					if err != nil {
						return nil, 0, fmt.Errorf("read error: %s", err.Error())
					}
					if data == nil {
						return nil, 0, fmt.Errorf("read error: data is nil")
					}

					tryRecoverJSON(data[errPos:], jErr)
					row, newPos, jErr = json.ReadArray(data)
					if jErr == nil {
						err = nil

						break
					}
					err = jErr
					// position of error did not advance
					if newPos == errPos {
						break
					}
				}
				pos = newPos
				if err != nil {
					break
				}
			}
			bytesReadForRow += pos

			data, trim = trimLeftTracking(data[pos:])
			bytesReadForRow += trim

			// trim trailing comma
			if len(data) >= 1 && data[0] == ',' {
				data, trim = trimLeftTracking(data[1:])
				bytesReadForRow += trim + 1
			}

			if len(data) == 0 {
				break
			}
		}
	}

	if row != nil {
		// send final row along with the num of bytes parsed till here
		if cErr := clb(row, bytesReadForRow); cErr != nil {
			return nil, 0, cErr
		}
	}

	totalBytesRead += bytesReadForRow

	return nil, totalBytesRead, nil
}

// returns max number of bytes to read.
func maxReadBytes(totalBytesRead, maxReadSize, defaultSize int) int {
	// maxReadSize of -1 disabled the limit
	if maxReadSize < 0 {
		return defaultSize
	}

	remaining := maxReadSize - totalBytesRead
	if remaining <= 0 {
		return 0
	}

	if remaining < defaultSize {
		return remaining
	}

	return defaultSize
}

// read next chunks from buffer and append to existing data if required.
func streamReadMore(buf *bufio.Reader, data []byte, numReadAlready, maxReadSize, bufferSize int) ([]byte, error) {
	// if data is empty and we can read more than buffer size, read directly to data buffer to avoid extra copy
	if maxReadSize != -1 && (maxReadSize-numReadAlready) <= bufferSize {
		return streamReadMoreDirect(buf, data, numReadAlready, maxReadSize)
	}

	readMore := true
	readMax := maxReadBytes(numReadAlready, maxReadSize, bufferSize)

	// concatenate more lines until eof or newline reached
	for readMore && readMax > 0 {
		partialLine, err := buf.ReadSlice('\n')
		switch {
		case errors.Is(err, io.EOF):
			if len(data) == 0 {
				return partialLine, nil
			}

			data = append(data, partialLine...)

			return data, nil
		case errors.Is(err, bufio.ErrBufferFull):
			readMore = true
		case err != nil:
			return nil, fmt.Errorf("json read: %w", err)
		default:
			readMore = false
		}

		readMax -= len(partialLine)
		if readMax == -1 {
			LogErrors(buf.UnreadByte())
			partialLine = partialLine[0 : len(partialLine)-1]
			readMax++
		}

		if !readMore {
			if len(data) == 0 {
				return partialLine, nil
			}

			data = append(data, partialLine...)

			return data, nil
		}

		if len(data) == 0 {
			data = bytes.Clone(partialLine)
		} else {
			data = append(data, partialLine...)
		}
	}

	return data, nil
}

// read next chunk directly into buffer and append to existing data if required.
func streamReadMoreDirect(buf *bufio.Reader, data []byte, numReadAlready, maxReadSize int) ([]byte, error) {
	toRead := maxReadSize - numReadAlready
	if toRead <= 0 {
		return nil, nil
	}

	partialLine := make([]byte, toRead)
	num, err := io.ReadFull(buf, partialLine)
	if num == toRead {
		if len(partialLine) == 0 {
			return partialLine, nil
		}
		data = append(data, partialLine...)

		return data, nil
	}
	if err != nil {
		return nil, fmt.Errorf("json read: %w", err)
	}

	return nil, fmt.Errorf("json read: could not read all remaining %d bytes, only read %d", toRead, num)
}

// parseResponseHeader parses the return code and content length from the first line of livestatus answer.
// It returns the body size or an error if parsing fails.
func parseResponseHeader(resBytes *[]byte) (code, expSize int, err error) {
	resSize := len(*resBytes)
	if resSize == 0 {
		return 0, 0, fmt.Errorf("empty response, got 0 bytes")
	}
	if resSize < 16 {
		return 0, 0, fmt.Errorf("incomplete response header: '%s'", string(*resBytes))
	}
	header := string((*resBytes)[0:15])
	matched := reResponseHeader.FindStringSubmatch(header)
	if len(matched) != 3 {
		if len(*resBytes) > ErrorContentPreviewSize {
			*resBytes = (*resBytes)[:ErrorContentPreviewSize]
		}

		return 0, 0, fmt.Errorf("incorrect response header: '%s'", string((*resBytes)))
	}
	code, err = strconv.Atoi(matched[1])
	if err != nil {
		return 0, 0, fmt.Errorf("header parse error - %s: %s", err.Error(), string(*resBytes))
	}
	expSize64, err := strconv.ParseInt(matched[2], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("header parse error - %s: %s", err.Error(), string(*resBytes))
	}

	if expSize64 > MaxJSONResponseSize {
		return 0, 0, fmt.Errorf("response size exceeds maximum allowed size: %d", expSize64)
	}

	expSize = interface2int(expSize64)

	return code, expSize, nil
}

// trim leading whitespace bytes and return the number of trimmed bytes.
func trimLeftTracking(data []byte) (dat []byte, numTrimmed int) {
	whiteSpace := "\t\n\v\f\r "
	len1 := len(data)
	data = bytes.TrimLeft(data, whiteSpace)
	len2 := len(data)

	return data, len1 - len2
}

// replace invalid json characters inline, does not change length of the byte array.
func tryRecoverJSON(data []byte, err error) {
	// try to fix invalid escape sequences and unknown utf8 characters
	if strings.Contains(err.Error(), "invalid json string") {
		replaceInvalidUTF8(data)
	}

	// try to fix invalid nan for numbers (simply replace with 0)
	if strings.Contains(err.Error(), "not null") {
		for idx := 0; idx != -1; idx = bytes.Index(data, []byte("nan,")) {
			data[idx] = 32
			data[idx+1] = 48
			data[idx+2] = 32
		}
		for idx := 0; idx != -1; idx = bytes.Index(data, []byte("nan]")) {
			data[idx] = 32
			data[idx+1] = 48
			data[idx+2] = 32
		}
	}
}

// bytes.ToValidUTF8 replaces invalid utf8 characters
// taken from go 1.13 beta
// https://github.com/golang/go/commit/3259bc441957bf74f069cf7df961367a3472afb2
// https://github.com/golang/go/issues/25805
// enhanced with removing ctrl characters like in
// https://rosettacode.org/wiki/Strip_control_codes_and_extended_characters_from_a_string#Go
// replaces characters inline with spaces so length is not changed.
func replaceInvalidUTF8(src []byte) {
	invalid := false // previous byte was from an invalid UTF-8 sequence

	for idx := 0; idx < len(src); {
		chr := src[idx]

		// stop at the first newline
		if chr == '\n' {
			return
		}

		// the byte range from 32 to 126 contains valid one byte size characters
		// excluding control characters (0-31)
		// and the del character (127)
		if chr >= 32 && chr <= 126 {
			idx++
			invalid = false

			continue
		}

		// try to read a utf8 character, returns (err, 1) if invalid
		_, wid := utf8.DecodeRune(src[idx:])
		if wid == 1 {
			if !invalid {
				invalid = true
				src[idx] = '.'
			}

			idx++

			continue
		}

		// read valid utf8 character
		invalid = false
		idx += wid
	}
}
