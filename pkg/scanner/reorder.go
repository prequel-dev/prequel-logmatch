package scanner

// Reorder is a simple FIFO queue that reorders log entries
// based on their timestamps.  It is used to ensure that
// log entries are processed in the order they were
// generated, even if they arrive out of order.  This is
// important for log entries that are processed in a
// distributed system, where log entries may arrive at
// different times due to network latency or other factors.
//
// The reorder queue is implemented using two linked lists,
// one for in order entries and one for out of order entries.
// The in order entries are added to the inOrder list, which
// is the typical behaviour.  An out of order entry is added
// to the out of order list.  The lists are correlated by
// timestamp as entries roll out of the time window enforcing
// in order delivery.  This technique works well if the
// the lists are generally well ordered, but will be inefficient
// if the order is random.

import (
	"errors"
	"math"
	"sync"

	"github.com/rs/zerolog/log"
)

var (
	ErrInvalidWindow   = errors.New("invalid window")
	ErrInvalidCallback = errors.New("invalid callback")
)

type ReorderT struct {
	cb     ScanFuncT
	window int64
	clock  int64
	mUsed  int
	mLimit int
	inList *rListT
	ooList *rListT
}

type roptT struct {
	memlimit int
}

type ROpt func(*roptT)

func WithMemoryLimit(limit int) ROpt {
	return func(o *roptT) {
		o.memlimit = limit
	}
}

func parseROpts(opts ...ROpt) roptT {
	o := roptT{
		memlimit: math.MaxInt,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Specify lookback window in nanoseconds; entries will
// be reordred within this window.  This implies that
// entries will not be delivered to 'cb' until they shift
// outside the window.
//
// Optionally provide a memory limit for the reorder buffer.
// This is useful for limiting memory usage in the case
// of a large number of entries within the time window.  The
// reorder buffer is a FIFO queue, so the oldest entries
// will be delivered on memory limit threshold, effectively
// shifting the window forward in time.  A side effect of
// this shift is that a subsequent out of order event may be
// dropped because it no longer falls within the shifted window.

func NewReorder(window int64, cb ScanFuncT, opts ...ROpt) (*ReorderT, error) {
	o := parseROpts(opts...)

	if window <= 0 {
		return nil, ErrInvalidWindow
	}
	if cb == nil {
		return nil, ErrInvalidCallback
	}

	return &ReorderT{
		cb:     cb,
		window: window,
		mLimit: o.memlimit,
		inList: newRList(),
		ooList: newRList(),
	}, nil
}

func (r *ReorderT) Append(entry LogEntry) (done bool) {
	done = r._append(entry)
	if r.mUsed > r.mLimit && !done {
		done = r._trim()
	}
	return
}

func (r *ReorderT) _append(entry LogEntry) bool {

	// Check entry is out of order
	if entry.Timestamp < r.clock {
		r.queueOutofOrder(entry)
		return false
	}

	// We are in order, queue and continue.
	// This is the normal case.
	r.clock = entry.Timestamp
	node := r.inList.pushBack(entry)
	r.mUsed += node.Size()

	return r._flush()
}

func (r *ReorderT) _flush() bool {
	ooHead := r.ooList.front()
	if ooHead == nil {
		// No out of order entries, so we can
		// use the fast path.
		// This is the normal case.
		return r.fastPath()
	}
	return r.slowPath(ooHead.entry.Timestamp)
}

func (r *ReorderT) fastPath() bool {

	deadline := r.clock - r.window
	for head := r.inList.front(); head != nil; head = r.inList.front() {
		if head.entry.Timestamp > deadline {
			break
		}

		r.mUsed -= head.Size()
		if done := r.cb(head.entry); done {
			r.drain()
			return true
		}

		r.inList.remove(head)
		rPoolFree(head)
	}

	return false
}

func (r *ReorderT) slowPath(ooTimestamp int64) bool {

	var (
		deadline = r.clock - r.window
	)

	// Iterate across pending entries
	for inHead := r.inList.front(); inHead != nil; inHead = r.inList.front() {
		// Bail once was pass the deadline
		if inHead.entry.Timestamp > deadline {
			break
		}

		// Entry is outside window and should be delivered.
		// Deliver any pending out of order entries that are
		// older than the in inHead entry.
		for ooTimestamp < inHead.entry.Timestamp {
			var (
				node = r.ooList.popFront()
				done = r.cb(node.entry)
			)
			r.mUsed -= node.Size()
			rPoolFree(node)

			if done {
				r.drain()
				return true
			}

			if ooHead := r.ooList.front(); ooHead == nil {
				ooTimestamp = math.MaxInt64
			} else {
				ooTimestamp = ooHead.entry.Timestamp
			}
		}

		// Deliver the inHead entry
		r.mUsed -= inHead.Size()
		if done := r.cb(inHead.entry); done {
			r.drain()
			return true
		}

		r.inList.remove(inHead)
		rPoolFree(inHead)
	}

	// Flush out any out of order entries that are older than window
	// Entry is outside window and should be delivered.
	for ooTimestamp <= deadline {
		var (
			node = r.ooList.popFront()
			done = r.cb(node.entry)
		)
		r.mUsed -= node.Size()
		rPoolFree(node)

		if done {
			r.drain()
			return true
		}

		if ooHead := r.ooList.front(); ooHead == nil {
			ooTimestamp = math.MaxInt64
		} else {
			ooTimestamp = ooHead.entry.Timestamp
		}
	}

	return false
}

// _trim is called when the memory limit is reached.
// Similar to slow path except different stop condition.
// Also has side effect of advancing the clock.  This is
// necessary to prevent a future out of order delivery on
// entries before entries delivered during the trim.

func (r *ReorderT) _trim() bool {

	var ooTimestamp int64 = math.MaxInt64
	if ooHead := r.ooList.front(); ooHead != nil {
		ooTimestamp = ooHead.entry.Timestamp
	}

	// Iterate across pending entries, removing
	// until we are back within the limit.
	// This will by its very nature remove entries
	// that are still inside the window, but is necessary
	// to remain with memory constraints.
LOOP:
	for inHead := r.inList.front(); inHead != nil; inHead = r.inList.front() {
		if r.mUsed <= r.mLimit {
			break LOOP
		}

		for ooTimestamp < inHead.entry.Timestamp {
			var (
				node = r.ooList.popFront()
				done = r.cb(node.entry)
			)
			r.mUsed -= node.Size()
			r.clock = node.entry.Timestamp + r.window

			rPoolFree(node)

			if done {
				r.drain()
				return true
			}

			if ooHead := r.ooList.front(); ooHead == nil {
				ooTimestamp = math.MaxInt64
			} else {
				ooTimestamp = ooHead.entry.Timestamp
			}

			// If we are back within range, exit the inLoop entirely
			if r.mUsed <= r.mLimit {
				break LOOP
			}
		}

		done := r.cb(inHead.entry)
		r.mUsed -= inHead.Size()
		r.clock = inHead.entry.Timestamp + r.window
		r.inList.remove(inHead)
		rPoolFree(inHead)

		if done {
			r.drain()
			return true
		}
	}

	if r.mUsed <= r.mLimit {
		return false
	}

	// If we are still over the limit after removing
	// all inList entries. Deliver the oldest out of order entries.
	// This can happen when the clock is advanced past the timestamp
	// of the subsequent delivered entries and so they all appear out
	// of order.

LOOP2:
	for node := r.ooList.popFront(); node != nil; node = r.ooList.popFront() {

		done := r.cb(node.entry)
		r.mUsed -= node.Size()
		r.clock = node.entry.Timestamp + r.window
		rPoolFree(node)

		if done {
			r.drain()
			return true
		}

		if r.mUsed <= r.mLimit {
			break LOOP2
		}
	}

	return false
}

func (r *ReorderT) AdvanceClock(stamp int64) bool {
	if stamp < r.clock {
		log.Info().
			Int64("clock", r.clock).
			Int64("stamp", stamp).
			Msg("Reorder: ignore  setclock stamp regression")
		return false
	}
	r.clock = stamp
	return r._flush()
}

func (r *ReorderT) Flush() (done bool) {
	done = r.AdvanceClock(math.MaxInt64)
	r.drain()
	return done
}

// O(n): Maintain order invariant on insert.
// This is linear, but given how the data typically arrives,
// it is unlikely to be a problem.  Could use a tree structure
// if inserts become expensive.
func (r *ReorderT) queueOutofOrder(entry LogEntry) {
	deadline := r.clock - r.window
	if entry.Timestamp < deadline {
		log.Debug().
			Int64("clock", r.clock).
			Int64("stamp", entry.Timestamp).
			Int64("deadline", deadline).
			Str("line", entry.Line).
			Msg("Reorder: ignore out of order entry")
		return
	}

	node := r.ooList.back()
	for ; node != nil; node = r.ooList.prev(node) {
		if node.entry.Timestamp <= entry.Timestamp {
			break
		}
	}
	if node == nil {
		node = r.ooList.pushFront(entry)
	} else {
		r.ooList.insert(entry, node)
	}

	r.mUsed += node.Size()
}

func (r *ReorderT) drain() {
	r.ooList.free()
	r.inList.free()
	r.clock = 0
	r.mUsed = 0
}

// ----

var rpool = sync.Pool{
	New: func() any {
		return new(rnodeT)
	},
}

func rPoolAlloc() *rnodeT {
	return rpool.Get().(*rnodeT)
}
func rPoolFree(ptr *rnodeT) {
	ptr.next = nil
	ptr.prev = nil
	rpool.Put(ptr)
}

type rnodeT struct {
	entry LogEntry
	next  *rnodeT
	prev  *rnodeT
}

const nodeSize = 80

func (r *rnodeT) Size() int {
	return nodeSize + len(r.entry.Line)
}

type rListT struct {
	root rnodeT
}

func newRList() *rListT {
	ll := &rListT{}
	ll.root.next = &ll.root
	ll.root.prev = &ll.root
	return ll
}

// insert e after at
func (ll *rListT) _insert(e, at *rnodeT) *rnodeT {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	return e
}

func (ll *rListT) free() {
	for head := ll.popFront(); head != nil; head = ll.popFront() {
		rPoolFree(head)
	}
}

func (ll *rListT) popFront() *rnodeT {
	if ll.root.next == &ll.root {
		return nil
	}
	return ll.remove(ll.root.next)
}

func (ll *rListT) pushBack(v LogEntry) *rnodeT {
	return ll.insert(v, ll.root.prev)
}

func (ll *rListT) pushFront(v LogEntry) *rnodeT {
	return ll.insert(v, &ll.root)
}

func (ll *rListT) insert(v LogEntry, at *rnodeT) *rnodeT {
	e := rPoolAlloc()
	e.entry = v
	return ll._insert(e, at)
}

func (ll *rListT) remove(e *rnodeT) *rnodeT {
	e.prev.next = e.next
	e.next.prev = e.prev
	return e
}

func (ll *rListT) front() *rnodeT {
	if ll.root.next == &ll.root {
		return nil
	}
	return ll.root.next
}

func (ll *rListT) back() *rnodeT {
	if ll.root.prev == &ll.root {
		return nil
	}
	return ll.root.prev
}

func (ll *rListT) prev(e *rnodeT) *rnodeT {
	if p := e.prev; p != &ll.root {
		return p
	}
	return nil
}

func (ll *rListT) empty() bool {
	return ll.root.next == &ll.root
}
