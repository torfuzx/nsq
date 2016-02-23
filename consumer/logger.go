// Implements the Output.
package consumer

import (
	"hotpu.cn/xkefu/common/log"
)

// LogLevelDebug
type NsqDebugLogger struct{}

func (l NsqDebugLogger) Output(calldepth int, s string) error {
	log.Debug("common.nsq.consumer", "NsqDebugLogger.Output", s)
	return nil
}

// LogLevelInfo
type NsqInfoLogger struct{}

func (l NsqInfoLogger) Output(calldepth int, s string) error {
	log.Info("common.nsq.consumer", "NsqInfoLogger.Output", s)
	return nil
}

// LogLevelWarning
type NsqWarningLogger struct{}

func (l NsqWarningLogger) Output(calldepth int, s string) error {
	log.Warn("common.nsq.consumer", "NsqWarningLogger.Output", s)
	return nil
}

// LogLevelError
type NsqErrorLogger struct{}

func (l NsqErrorLogger) Output(calldepth int, s string) error {
	log.Error("common.nsq.consumer", "NsqErrorLogger.Output", s)
	return nil
}
