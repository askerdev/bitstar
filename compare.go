package bitstar

type eventKeyMinHeap []EventKey

func (h eventKeyMinHeap) Len() int { return len(h) }
func (h eventKeyMinHeap) Less(i, j int) bool {
	timeI := h[i].StartTime
	timeJ := h[j].StartTime

	if timeI.Before(timeJ) {
		return true
	}
	if timeI.After(timeJ) {
		return false
	}

	return h[i].ID < h[j].ID
}

func (h eventKeyMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventKeyMinHeap) Push(x any)   { *h = append(*h, x.(EventKey)) }
func (h *eventKeyMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func compareEventKey(a, b EventKey) int {
	if a.StartTime.After(b.StartTime) {
		return 1
	}
	if a.StartTime.Before(b.StartTime) {
		return -1
	}

	if a.ID > b.ID {
		return 1
	}

	if a.ID < b.ID {
		return -1
	}

	return 0
}
