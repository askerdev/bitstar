package bitstar

type heapItem struct {
	item *memTableItem
}

type eventMinHeap []*heapItem

func (h eventMinHeap) Len() int { return len(h) }
func (h eventMinHeap) Less(i, j int) bool {
	timeI := h[i].item.event.StartTime.AsTime()
	timeJ := h[j].item.event.StartTime.AsTime()

	if timeI.Before(timeJ) {
		return true
	}
	if timeI.After(timeJ) {
		return false
	}

	return h[i].item.event.Id < h[j].item.event.Id
}

func (h eventMinHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *eventMinHeap) Push(x interface{}) { *h = append(*h, x.(*heapItem)) }
func (h *eventMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func isNewer(a, b *memTableItem) bool {
	timeA := a.event.StartTime.AsTime()
	timeB := b.event.StartTime.AsTime()
	if timeA.After(timeB) {
		return true
	}
	if timeA.Before(timeB) {
		return false
	}
	return a.event.Id > b.event.Id
}
