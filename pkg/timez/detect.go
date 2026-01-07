package timez

import (
	"io"

	"github.com/prequel-dev/prequel-logmatch/pkg/format"
)

type FmtSpec struct {
	Pattern string
	Format  TimestampFmt
}

const (
	detectSampleSize = 16 * 1024
)

// Note: order matters particularly when matching similiar patterns
// Put the more specific variations before the more general.

var Defaults = []FmtSpec{
	// Example: {"level":"error","error":"context deadline exceeded","time":1744570895480541,"caller":"server.go:462"}
	{
		Format:  FmtEpochAny,
		Pattern: `"time":(\d{16,19})`,
	},

	// Example: 2006-01-02T15:04:05Z07:00 <log message>
	{
		Format:  FmtRfc3339,
		Pattern: `^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})) `,
	},

	// Example: 2006/01/02 03:04:05 <log message>
	{
		Format:  TimestampFmt("2006/01/02 03:04:05"),
		Pattern: `^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) `,
	},

	// Example: 2006-01-02 15:04:05.000000-0700 <log message>
	{
		Format:  TimestampFmt("2006-01-02 15:04:05.000000-0700"),
		Pattern: `^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}[+-]\d{4}) `,
	},

	// Example: 2006-01-02 15:04:05.000 <log message>
	// Source: ISO 8601
	{
		Format:  TimestampFmt("2006-01-02 15:04:05.000"),
		Pattern: `^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) `,
	},

	// Example: 2006-01-02 15:04:05 <log message>
	// Source: w3c, Postgres
	{
		Format:  TimestampFmt("2006-01-02 15:04:05"),
		Pattern: `^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) `,
	},

	// Example: Apr 30 23:36:47.715984 WRN <log message>
	// Source: RFC 3164 extended
	{
		Format:  TimestampFmt("Jan 2 15:04:05.000000"),
		Pattern: `^([A-Z][a-z]{2}\s{1,2}\d{1,2}\s\d{2}:\d{2}:\d{2}\.\d{6}) `,
	},

	// Example: Jan 2 15:04:05 <log message>
	// Source: RFC 3164
	{
		Format:  TimestampFmt("Jan 2 15:04:05"),
		Pattern: `^([A-Z][a-z]{2}\s{1,2}\d{1,2}\s\d{2}:\d{2}:\d{2}) `,
	},

	// Example: I0102 15:04:05.000000 <log message>
	// Source: go/klog
	{
		Format:  TimestampFmt("0102 15:04:05.000000"),
		Pattern: `^[IWEF](\d{4} \d{2}:\d{2}:\d{2}\.\d{6}) `,
	},

	// Example: [2006-01-02 15:04:05,000] <log message>
	{
		Format:  TimestampFmt("2006-01-02 15:04:05,000"),
		Pattern: `^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\] `,
	},

	// Example: 2006/01/02 15:04:05 <log message>
	{
		Format:  TimestampFmt("2006/01/02 15:04:05"),
		Pattern: `^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) `,
	},

	// Example: 01/02/2006, 15:04:05 <log message>
	// Source: IIS format
	{
		Format:  TimestampFmt("01/02/2006, 15:04:05"),
		Pattern: `^(\d{2}/\d{2}/\d{4}, \d{2}:\d{2}:\d{2}) `,
	},

	// Example: 02 Jan 2006 15:04:05.000 <log message>
	{
		Format:  TimestampFmt("02 Jan 2006 15:04:05.000"),
		Pattern: `^(\d{2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2}\.\d{3}) `,
	},

	// Example: 2006 Jan 02 15:04:05.000 <log message>
	{
		Format:  TimestampFmt("2006 Jan 02 15:04:05.000"),
		Pattern: `^(\d{4} [A-Z][a-z]{2} \d{2} \d{2}:\d{2}:\d{2}\.\d{3}) `,
	},

	// Example: 02/Jan/2006:15:04:05.000 <log message>
	{
		Format:  TimestampFmt("02/Jan/2006:15:04:05.000"),
		Pattern: `^(\d{2}/[A-Z][a-z]{2}/\d{4}:\d{2}:\d{2}:\d{2}\.\d{3}) `,
	},

	// Example: 01/02/2006 03:04:05 PM <log message>
	{
		Format:  TimestampFmt("01/02/2006 03:04:05 PM"),
		Pattern: `^(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2} (AM|PM)) `,
	},

	// Example: 2006 Jan 02 15:04:05 <log message>
	{
		Format:  TimestampFmt("2006 Jan 02 15:04:05"),
		Pattern: `^(\d{4} [A-Z][a-z]{2} \d{2} \d{2}:\d{2}:\d{2}) `,
	},

	// Example: {"timestamp":"2025-03-26T14:01:02Z","level":"info", "message":"..."}
	// Source: Postgres JSON output
	{
		Format:  FmtRfc3339,
		Pattern: `"timestamp"\s*:\s*"([^"]+)"`,
	},

	// Example: {"ts":"2025-03-26T14:01:02Z","level":"info", "message":"..."}
	// Source: metallb
	{
		Format:  FmtRfc3339,
		Pattern: `"ts"\s*:\s*"([^"]+)"`,
	},

	// Example: [7] 2025/04/25 02:01:04.339092 [ERR] ...
	// Source: NATS
	{
		Format:  TimestampFmt("2006/01/02 15:04:05.000000"),
		Pattern: `^\[\d+\]\s+(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{6}) `,
	},

	// Example: {"creationTimestamp":"2025-04-23T20:50:35Z",...}
	// Source: Kubernetes events, configmaps
	{
		Format:  FmtRfc3339,
		Pattern: `"creationTimestamp":"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z)"`,
	},

	// Example: 2025-04-24T21:55:08.535-0500 INFO ...
	// Source: ZAP dev
	{
		Format:  TimestampFmt("2006-01-02T15:04:05.000-0700"),
		Pattern: `^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{4})\t`,
	},

	// Example: {"level":"info","ts":1745549708.5355184,...}
	// Source: ZAP prod
	{
		Format:  FmtDotNotation,
		Pattern: `"ts"\s*:\s*([0-9]+(?:\.[0-9]+)?)`,
	},

	// Example: ts=2025-03-10T13:52:40.623431174Z ...
	// Source: Loki
	{
		Format:  TimestampFmt("2006-01-02T15:04:05.000000000Z"),
		Pattern: `ts=([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z)`,
	},

	// Example: {"event": "...", "timestamp": "2025-02-12T18:12:58.715528Z", ...}
	// Source: DataDog
	{
		Format:  TimestampFmt("2006-01-02T15:04:05.000000Z"),
		Pattern: `"timestamp"\s*:\s*"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z)"`,
	},

	// Example: {"TimeCreated":"\/Date(1743448267142)\/"}
	// Source; Windows events via Get-Events w/ JSON output
	{
		Format:  FmtEpochAny,
		Pattern: `/Date\((\d+)\)`,
	},

	// Example: time="2025-02-12T18:12:58.715528Z"
	// Source: argocd
	{
		Format:  FmtRfc3339,
		Pattern: `time="([^"]+)"`,
	},
}

func DetectFormat(rd io.Reader) (format.FactoryI, int64, error) {

	var buffer = make([]byte, detectSampleSize)

	n, err := io.ReadFull(rd, buffer)
	switch err {
	case nil, io.ErrUnexpectedEOF: // NOOP
	default:
		return nil, 0, err
	}
	buffer = buffer[:n]

	factory, stamp := TryTimestampFormats(Defaults, buffer, DefaultSkip)
	return factory, stamp, nil
}
