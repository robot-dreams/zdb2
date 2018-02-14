package stream

import (
	"bufio"

	"github.com/robot-dreams/zdb2"
)

type FieldHeader struct {
	Name string
	Type zdb2.Type
}

func ReadFieldHeader(r *bufio.Reader) (*FieldHeader, error) {
	name, err := zdb2.ReadTerminatedString(r)
	if err != nil {
		return nil, err
	}
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	return &FieldHeader{
		Name: name,
		Type: zdb2.Type(b),
	}, nil
}

func (f *FieldHeader) Write(w *bufio.Writer) error {
	err := zdb2.WriteTerminatedString(w, f.Name)
	if err != nil {
		return err
	}
	return w.WriteByte(uint8(f.Type))
}

type Header struct {
	Name string
	// Invariant: len(FieldHeaders) <= 0xFF
	FieldHeaders []*FieldHeader
}

func ReadHeader(r *bufio.Reader) (*Header, error) {
	name, err := zdb2.ReadTerminatedString(r)
	if err != nil {
		return nil, err
	}
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	numFields := int(b)
	fieldHeaders := make([]*FieldHeader, numFields)
	for i := 0; i < numFields; i++ {
		fieldHeader, err := ReadFieldHeader(r)
		if err != nil {
			return nil, err
		}
		fieldHeaders[i] = fieldHeader
	}
	return &Header{
		Name:         name,
		FieldHeaders: fieldHeaders,
	}, nil
}

func (h *Header) Write(w *bufio.Writer) error {
	err := zdb2.WriteTerminatedString(w, h.Name)
	if err != nil {
		return err
	}
	err = w.WriteByte(uint8(len(h.FieldHeaders)))
	if err != nil {
		return err
	}
	for _, fieldHeader := range h.FieldHeaders {
		err = fieldHeader.Write(w)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *Header) ReadRecord(r *bufio.Reader) (zdb2.Record, error) {
	record := make(zdb2.Record, len(h.FieldHeaders))
	for i, fieldHeader := range h.FieldHeaders {
		value, err := zdb2.ReadValue(r, fieldHeader.Type)
		if err != nil {
			return nil, err
		}
		record[i] = value
	}
	return record, nil
}

// Preconditions:
//     len(record) == len(t.FieldHeaders)
//     record[i] matches t.FieldHeaders[i].Type for 0 <= i < len(record)
func (h *Header) WriteRecord(w *bufio.Writer, record zdb2.Record) error {
	for i, value := range record {
		err := zdb2.WriteValue(w, h.FieldHeaders[i].Type, value)
		if err != nil {
			return err
		}
	}
	return nil
}
