package scanner

const avgLogSize = 256

type StdReadScan struct {
	sz    int
	maxSz int
	clip  bool
	logs  []LogEntry
}

func NewStdReadScan(maxSz int) *StdReadScan {
	return &StdReadScan{
		maxSz: maxSz,
		logs:  make([]LogEntry, 0, maxSz/avgLogSize),
	}
}

func (sr *StdReadScan) Scan(entry LogEntry) bool {
	sz := entry.UpperBound()
	if sr.sz += sz; sr.sz > sr.maxSz {
		sr.clip = true
		sr.sz -= sz
		return true
	}

	sr.logs = append(sr.logs, entry)
	return false
}

func (sr *StdReadScan) Bind() ScanFuncT {
	return sr.Scan
}

func (sr *StdReadScan) Result() ScanResultT {
	return ScanResultT{
		Sz:   sr.sz,
		Clip: sr.clip,
		Logs: sr.logs,
	}
}
