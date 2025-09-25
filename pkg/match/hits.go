package match

type Hits struct {
	Cnt   int
	Logs  []LogEntry
	Props map[string]any
}

func (h *Hits) PopFront() []LogEntry {
	if h.Cnt <= 0 {
		return nil
	}

	var (
		sz   = len(h.Logs) / h.Cnt
		logs = h.Logs[:sz]
	)

	h.Cnt -= 1
	h.Logs = h.Logs[sz:]
	return logs
}

func (h Hits) Last() []LogEntry {
	return h.Index(h.Cnt - 1)
}

func (h Hits) Index(i int) []LogEntry {
	if i < 0 || i >= h.Cnt {
		return nil
	}
	var (
		nLogs = len(h.Logs)
		sz    = nLogs / h.Cnt
		off   = i * sz
	)
	return h.Logs[off : off+sz]
}
