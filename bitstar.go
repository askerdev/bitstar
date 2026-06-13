package bitstar

type Pair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type BytePair struct {
	Key []byte
	Val []byte
}
