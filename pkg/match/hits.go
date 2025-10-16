package match

type PropKey struct {
	Idx int
	Key string
}

type Hits struct {
	Cnt   int
	Logs  []LogEntry
	Props map[PropKey]any
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

// IndexProps returns a map of properties for the given index i, aggregating all entries in h.Props
// where PropKey.Idx == i. If h.Props is nil or no properties match, it returns nil (not an empty map).
// Out-of-range indices are not explicitly checked; if no properties match, nil is returned.

func (h Hits) IndexProps(i int) map[string]any {
	if h.Props == nil || i < 0 || i >= h.Cnt {
		return nil
	}

	var m map[string]any

	for k, v := range h.Props {
		if k.Idx == i {
			if m == nil {
				m = make(map[string]any)
			}
			m[k.Key] = v
		}
	}
	return m
}
