package stream

import (
	"encoding/binary"
	"io"

	"github.com/robot-dreams/zdb2"
)

func ReadField(r io.Reader) (*zdb2.Field, error) {
	name, err := zdb2.ReadString(r)
	if err != nil {
		return nil, err
	}
	var b uint8
	err = binary.Read(r, zdb2.ByteOrder, &b)
	if err != nil {
		return nil, err
	}
	return &zdb2.Field{
		Name: name,
		Type: zdb2.Type(b),
	}, nil
}

func WriteField(w io.Writer, f *zdb2.Field) error {
	err := zdb2.WriteString(w, f.Name)
	if err != nil {
		return err
	}
	return binary.Write(w, zdb2.ByteOrder, uint8(f.Type))
}

func ReadTableHeader(r io.Reader) (*zdb2.TableHeader, error) {
	name, err := zdb2.ReadString(r)
	if err != nil {
		return nil, err
	}
	var b uint8
	err = binary.Read(r, zdb2.ByteOrder, &b)
	if err != nil {
		return nil, err
	}
	numFields := int(b)
	fields := make([]*zdb2.Field, numFields)
	for i := 0; i < numFields; i++ {
		field, err := ReadField(r)
		if err != nil {
			return nil, err
		}
		fields[i] = field
	}
	return &zdb2.TableHeader{
		Name:   name,
		Fields: fields,
	}, nil
}

func WriteTableHeader(w io.Writer, t *zdb2.TableHeader) error {
	err := zdb2.WriteString(w, t.Name)
	if err != nil {
		return err
	}
	err = binary.Write(w, zdb2.ByteOrder, uint8(len(t.Fields)))
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

func ReadRecord(r io.Reader, t *zdb2.TableHeader) (zdb2.Record, error) {
	record := make(zdb2.Record, len(t.Fields))
	for i, fieldHeader := range t.Fields {
		value, err := zdb2.ReadValue(r, fieldHeader.Type)
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
func WriteRecord(w io.Writer, t *zdb2.TableHeader, record zdb2.Record) error {
	for i, value := range record {
		err := zdb2.WriteValue(w, t.Fields[i].Type, value)
		if err != nil {
			return err
		}
	}
	return nil
}
