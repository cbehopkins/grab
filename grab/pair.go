package grab

// Pair encapsulates a seach kay value pair
type Pair struct {
	Key   string
	Value int
}

// PairList is a chicken! No sorry, it's a list of Pairs - sorry.
type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
