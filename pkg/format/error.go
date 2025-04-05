package format

import (
	"errors"
)

var (
	ErrFormatDetect   = errors.New("fail to detect log format")
	ErrNoTimestamp    = errors.New("no timestamp delimeter")
	ErrNoStreamType   = errors.New("no stream delimeter")
	ErrNoTag          = errors.New("no tag delimeter")
	ErrUnknownStream  = errors.New("unknown stream type")
	ErrParseTimestamp = errors.New("fail parse timestamp")
	ErrJsonTimeField  = errors.New("fail to extract time field")
	ErrJsonUnmarshal  = errors.New("fail JSON unmarshal")
	ErrMatchTimestamp = errors.New("fail match timestamp")
)
