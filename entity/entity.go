package entity

import "encoding/json"

type AtomicData struct {
	Key       string
	Value     []byte
	IsDeleted bool
}

type SearchResult int

const (
	NotExist SearchResult = iota
	Deleted
	Exist
)

// Encode AtomicData编码
func Encode(atomicData *AtomicData) ([]byte, error) {
	bytes, err := json.Marshal(atomicData)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// Decode AtomicData解码
func Decode(bytes []byte) (*AtomicData, error) {
	var atomicData AtomicData
	if err := json.Unmarshal(bytes, &atomicData); err != nil {
		return nil, err
	}
	return &atomicData, nil
}
