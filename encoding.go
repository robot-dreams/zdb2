package zdb2

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/dropbox/godropbox/errors"
)

var ByteOrder = binary.LittleEndian

func ReadValue(r io.Reader, type_ Type) (interface{}, error) {
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

func ReadString(r io.Reader) (string, error) {
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

func WriteValue(w io.Writer, type_ Type, value interface{}) error {
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

func WriteString(w io.Writer, s string) error {
	err := binary.Write(w, ByteOrder, uint8(len(s)))
	if err != nil {
		return err
	}
	_, err = io.WriteString(w, s)
	return err
}

func ReadField(r io.Reader) (*Field, error) {
	name, err := ReadString(r)
	if err != nil {
		return nil, err
	}
	var b uint8
	err = binary.Read(r, ByteOrder, &b)
	if err != nil {
		return nil, err
	}
	return &Field{
		Name: name,
		Type: Type(b),
	}, nil
}

func WriteField(w io.Writer, f *Field) error {
	err := WriteString(w, f.Name)
	if err != nil {
		return err
	}
	return binary.Write(w, ByteOrder, uint8(f.Type))
}

func ReadTableHeader(r io.Reader) (*TableHeader, error) {
	name, err := ReadString(r)
	if err != nil {
		return nil, err
	}
	var b uint8
	err = binary.Read(r, ByteOrder, &b)
	if err != nil {
		return nil, err
	}
	numFields := int(b)
	fields := make([]*Field, numFields)
	for i := 0; i < numFields; i++ {
		field, err := ReadField(r)
		if err != nil {
			return nil, err
		}
		fields[i] = field
	}
	return &TableHeader{
		Name:   name,
		Fields: fields,
	}, nil
}

func WriteTableHeader(w io.Writer, t *TableHeader) error {
	err := WriteString(w, t.Name)
	if err != nil {
		return err
	}
	err = binary.Write(w, ByteOrder, uint8(len(t.Fields)))
	if err != nil {
		return err
	}
	for _, field := range t.Fields {
		err = WriteField(w, field)
		if err != nil {
			return err
		}
	}
	return nil
}

func ReadRecord(r io.Reader, t *TableHeader) (Record, error) {
	record := make(Record, len(t.Fields))
	for i, fieldHeader := range t.Fields {
		value, err := ReadValue(r, fieldHeader.Type)
		if err != nil {
			return nil, err
		}
		record[i] = value
	}
	return record, nil
}

// Preconditions:
//     len(record) == len(t.Fields)
//     record[i] matches t.Fields[i].Type for 0 <= i < len(record)
func WriteRecord(w io.Writer, t *TableHeader, record Record) error {
	for i, value := range record {
		err := WriteValue(w, t.Fields[i].Type, value)
		if err != nil {
			return err
		}
	}
	return nil
}
