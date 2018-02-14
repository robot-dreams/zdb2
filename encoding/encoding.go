package encoding

import (
	"bufio"
	"encoding/binary"

	"github.com/dropbox/godropbox/errors"
	"github.com/robot-dreams/zdb2"
)

var ByteOrder = binary.LittleEndian

// Use null-terminated strings.
var StringTerminator uint8 = 0

func ReadValue(r *bufio.Reader, type_ zdb2.Type) (interface{}, error) {
	switch type_ {
	case zdb2.Int32:
		var x int32
		err := binary.Read(r, ByteOrder, &x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case zdb2.Float64:
		var x float64
		err := binary.Read(r, ByteOrder, &x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case zdb2.String:
		s, err := ReadTerminatedString(r)
		if err != nil {
			return nil, err
		}
		return s, nil
	default:
		return nil, errors.Newf("Unsupported type %v", type_)
	}
}

func ReadTerminatedString(r *bufio.Reader) (string, error) {
	s, err := r.ReadString(StringTerminator)
	if err != nil {
		return "", err
	}
	return s[:len(s)-1], nil
}

// TODO: Will handling the panic slow down performance?
func WriteValue(w *bufio.Writer, type_ zdb2.Type, value interface{}) error {
	switch type_ {
	case zdb2.Int32:
		return binary.Write(w, ByteOrder, value.(int32))
	case zdb2.Float64:
		return binary.Write(w, ByteOrder, value.(float64))
	case zdb2.String:
		return WriteTerminatedString(w, value.(string))
	default:
		return errors.Newf("Unsupported type %v", type_)
	}
}

func WriteTerminatedString(w *bufio.Writer, s string) error {
	_, err := w.WriteString(s)
	if err != nil {
		return nil
	}
	return w.WriteByte(StringTerminator)
}
