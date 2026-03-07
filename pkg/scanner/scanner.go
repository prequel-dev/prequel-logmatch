package scanner

type ScanResultT struct {
	Sz   int
	Clip bool
	Logs []LogEntry
}

type Scanner interface {
	Bind() ScanFuncT
	Result() ScanResultT
}
