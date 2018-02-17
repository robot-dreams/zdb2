package index

type RecordID struct {
	PageID int32
	SlotID uint16
}

type Entry struct {
	Key int32
	RID RecordID
}

type Iterator interface {
	Next() (Entry, error)
}
