package zdb2

import (
	"bufio"
	"bytes"
	"encoding/binary"

	"github.com/dropbox/godropbox/errors"
)

var ByteOrder = binary.LittleEndian

func ReadValue(r *bufio.Reader, type_ Type) (interface{}, error) {
	switch type_ {
	case Int32:
		var x int32
		err := binary.Read(r, ByteOrder, &x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case Float64:
		var x float64
		err := binary.Read(r, ByteOrder, &x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case String:
		s, err := ReadString(r)
		if err != nil {
			return nil, err
		}
		return s, nil
	default:
		return nil, errors.Newf("Unsupported type %v", type_)
	}
}

func ReadString(r *bufio.Reader) (string, error) {
	var n uint8
	err := binary.Read(r, ByteOrder, &n)
	if err != nil {
		return "", err
	}
	b := make([]byte, n)
	_, err = r.Read(b)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func SerializeValue(type_ Type, value interface{}) ([]byte, error) {
	var err error
	var buf bytes.Buffer
	switch type_ {
	case Int32, Float64:
		err = binary.Write(&buf, ByteOrder, value)
	case String:
		s := value.(string)
		err = buf.WriteByte(uint8(len(s)))
		if err != nil {
			return nil, err
		}
		_, err = buf.WriteString(s)
	default:
		err = errors.Newf("Unsupported type %v", type_)
	}
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func WriteValue(w *bufio.Writer, type_ Type, value interface{}) error {
	switch type_ {
	case Int32:
		return binary.Write(w, ByteOrder, value)
	case Float64:
		return binary.Write(w, ByteOrder, value)
	case String:
		return WriteString(w, value.(string))
	default:
		return errors.Newf("Unsupported type %v", type_)
	}
}

func WriteString(w *bufio.Writer, s string) error {
	err := w.WriteByte(uint8(len(s)))
	if err != nil {
		return err
	}
	_, err = w.WriteString(s)
	return err
}
