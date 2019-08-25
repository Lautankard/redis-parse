package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	SIMPLE_STRING = '+'
	BULK_STRING   = '$'
	INTEGER       = ':'
	ARRAY         = '*'
	ERROR         = '-'
)

var (
	ErrInvalidSyntax = errors.New("resp: invalid syntax")
)

type RESPReader struct {
	*bufio.Reader
}

func NewReader(reader io.Reader) *RESPReader {
	return &RESPReader{
		Reader: bufio.NewReaderSize(reader, 32*1024),
	}
}

func (r *RESPReader) ReadPipline() ([][]byte, error) {
	var lines [][]byte
	for {
		line, err := r.ReadObject()
		if err == io.EOF {
			return lines, nil
		}
		if err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
}

func (r *RESPReader) ReadObject() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case SIMPLE_STRING, INTEGER, ERROR:
		return line[1 : len(line)-2], nil
	case BULK_STRING:
		buf, err := r.readBulkString(line)
		if err != nil {
			return nil, err
		}
		return buf[:len(buf)-2], nil
	case ARRAY:
		return r.readArray(line)
	default:
		return nil, ErrInvalidSyntax
	}
}

func (r *RESPReader) readLine() (line []byte, err error) {
	line, err = r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) > 1 && line[len(line)-2] == '\r' {
		return line, nil
	} else {
		// Line was too short or \n wasn't preceded by \r.
		return nil, ErrInvalidSyntax
	}
}
func (r *RESPReader) readBulkString(line []byte) ([]byte, error) {
	count, err := r.getCount(line)
	if err != nil {
		return nil, err
	}
	if count == -1 {
		return line, nil
	}
	buf := make([]byte, count+2)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (r *RESPReader) getCount(line []byte) (int, error) {
	end := bytes.IndexByte(line, '\r')
	return strconv.Atoi(string(line[1:end]))
}

func (r *RESPReader) readArray(line []byte) ([]byte, error) {
	count, err := r.getCount(line)
	if err != nil {
		return nil, err
	}
	var bufs []byte
	for i := 0; i < count; i++ {
		buf, err := r.ReadObject()
		if err != nil {
			return nil, err
		}
		if len(bufs) > 0 {
			bufs = append(bufs, ' ')
		}
		bufs = append(bufs, buf...)
	}
	return bufs, nil
}

func main() {
	p1 := []byte("*2\r\n$3\r\nGET\r\n$1\r\nA\r\n*2\r\n$3\r\nGET\r\n$1\r\nB\r\n")
	// p1 := []byte("$11\r\nHello,world\r\n$11\r\nHello,world\r\n")
	reader := bytes.NewReader(p1)
	respReader := NewReader(reader)
	bys, err := respReader.ReadPipline()
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, by := range bys {
		fmt.Println(string(by))
	}

}
