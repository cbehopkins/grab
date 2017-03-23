package grab

// One multi-Fetcher does all files!
type MultiFetch struct {
	InChan     chan Url      // Write Urls to here
	scram_chan chan struct{} // Simply close this channel to scram to filename
	fifo       *UrlStore     // stored here
	filename   string
}

func NewMultiFetch() *MultiFetch {
	itm := new(MultiFetch)
	itm.fifo = NewUrlStore()
	itm.InChan = itm.fifo.PushChannel
	return itm
}

func (mf *MultiFetch) SetFileName(fn string) {
	mf.filename = fn
}

func (mf *MultiFetch) Scram() {
	close(mf.InChan)
	close(mf.scram_chan)
}
func (mf *MultiFetch) Close() {
	close(mf.InChan)
}
