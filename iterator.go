package bitstar

type Iterator interface {
	Next() bool
	Value() EventKey
	Seek(key EventKey)
	Valid() bool
}
