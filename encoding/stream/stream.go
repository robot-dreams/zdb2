package stream

import (
	"bufio"

	"github.com/robot-dreams/zdb2"
	"github.com/robot-dreams/zdb2/encoding"
)

func ReadField(r *bufio.Reader) (*zdb2.Field, error) {
	name, err := encoding.ReadTerminatedString(r)
	if err != nil {
		return nil, err
	}
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	return &zdb2.Field{
		Name: name,
		Type: zdb2.Type(b),
	}, nil
}

func WriteField(w *bufio.Writer, f *zdb2.Field) error {
	err := encoding.WriteTerminatedString(w, f.Name)
	if err != nil {
		return err
	}
	return w.WriteByte(uint8(f.Type))
}

func ReadTableHeader(r *bufio.Reader) (*zdb2.TableHeader, error) {
	name, err := encoding.ReadTerminatedString(r)
	if err != nil {
		return nil, err
	}
	b, err := r.ReadByte()
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

func WriteTableHeader(w *bufio.Writer, t *zdb2.TableHeader) error {
	err := encoding.WriteTerminatedString(w, t.Name)
	if err != nil {
		return err
	}
	err = w.WriteByte(uint8(len(t.Fields)))
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

func ReadRecord(r *bufio.Reader, t *zdb2.TableHeader) (zdb2.Record, error) {
	record := make(zdb2.Record, len(t.Fields))
	for i, fieldHeader := range t.Fields {
		value, err := encoding.ReadValue(r, fieldHeader.Type)
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
func WriteRecord(w *bufio.Writer, t *zdb2.TableHeader, record zdb2.Record) error {
	for i, value := range record {
		err := encoding.WriteValue(w, t.Fields[i].Type, value)
		if err != nil {
			return err
		}
	}
	return nil
}
