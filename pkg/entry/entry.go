package entry

import "github.com/tinylib/msgp/msgp"

//go:generate msgp

type LogEntry struct {
	Line      string  `msg:"l" json:"l"`
	Stream    string  `msg:"s" json:"s"`
	Timestamp int64   `msg:"t" json:"t"`
	Matches   [][]int `msg:"m,omitempty" json:"m,omitempty"`
}

// Uses msgpack size as an estimate;  not exactly right.
// Cannot use e.MsgSize() because it doesn't properly account for omitted matches

func (z LogEntry) UpperBound() (s int) {
	// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
	s = 1 + 2 + msgp.StringPrefixSize + len(z.Line) + 2 + msgp.StringPrefixSize + len(z.Stream) + 2 + msgp.Int64Size

	if z.Matches != nil {
		s += 2 + msgp.ArrayHeaderSize
		for za0001 := range z.Matches {
			s += msgp.ArrayHeaderSize + (len(z.Matches[za0001]) * (msgp.IntSize))
		}
	}
	return

	//return e.Msgsize()
}

type LogList []LogEntry
