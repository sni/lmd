package lmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"sync/atomic"

	"github.com/kdar/factorlog"
)

// LogFormat sets the log format.
var LogFormat string

// DateTimeLogFormat sets the log format for the date/time portion.
var DateTimeLogFormat string

var LogMaxOutput atomic.Int32

// LogToolsOutput is the default log output for tools.
var LogToolsOutput string

func init() {
	DateTimeLogFormat = `[%{Date} %{Time "15:04:05.000"}]`
	LogFormat = `[%{Severity}][pid:%{Pid}][%{ShortFile}:%{Line}] %{Message}`
	LogMaxOutput.Store(DefaultMaxLogOutput)
	LogToolsOutput = LogColors + DateTimeLogFormat + LogFormat + LogColorReset
}

const (
	// LogColors sets colors for some log levels.
	LogColors = `%{Color "yellow" "WARN"}%{Color "red" "ERROR"}%{Color "red" "FATAL"}`

	// LogColorReset resets colors from LogColors.
	LogColorReset = `%{Color "reset"}`

	// LogVerbosityNone disables logging.
	LogVerbosityNone = 0

	// LogVerbosityDefault sets the default log level.
	LogVerbosityDefault = 1

	// LogVerbosityDebug sets the debug log level.
	LogVerbosityDebug = 2

	// LogVerbosityTrace sets trace log level.
	LogVerbosityTrace = 3
)

// initialize standard logger which will be configured later from the configuration file options.
var log = factorlog.New(os.Stdout, buildFormatter(LogFormat))

// InitLogging initializes the logging system.
func InitLogging(conf *Config) {
	var logFormatter factorlog.Formatter
	var targetWriter io.Writer
	var err error
	switch {
	case conf.LogFile == "" || conf.LogFile == "stdout":
		logFormatter = buildFormatter(LogToolsOutput)
		targetWriter = os.Stdout
	case strings.EqualFold(conf.LogFile, "stderr"):
		logFormatter = buildFormatter(LogToolsOutput)
		targetWriter = os.Stderr
	case conf.LogFile == "stdout-journal":
		logFormatter = buildFormatter(LogFormat)
		targetWriter = os.Stdout
	default:
		logFormatter = buildFormatter(DateTimeLogFormat + LogFormat)
		targetWriter, err = os.OpenFile(conf.LogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	}
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %s", err.Error()))
	}
	logLevel := "Warn"
	if conf.LogLevel != "" {
		logLevel = conf.LogLevel
	}

	// override log level with environment variable if set
	if os.Getenv("LMD_LOG_LEVEL") != "" {
		logLevel = os.Getenv("LMD_LOG_LEVEL")
	}

	logMaxOutput := LogMaxOutput.Load()
	if logMaxOutput != interface2int32(conf.LogMaxOutput) {
		LogMaxOutput.Store(interface2int32(conf.LogMaxOutput))
	}
	if strings.EqualFold(logLevel, "trace2") {
		logLevel = "trace"
		LogMaxOutput.Store(0)
	}

	log.SetFormatter(logFormatter)
	log.SetOutput(targetWriter)
	log.SetVerbosity(LogVerbosityDefault)
	if strings.EqualFold(logLevel, "off") {
		log.SetMinMaxSeverity(factorlog.StringToSeverity("PANIC"), factorlog.StringToSeverity("PANIC"))
		log.SetVerbosity(LogVerbosityNone)
	} else {
		log.SetMinMaxSeverity(factorlog.StringToSeverity(strings.ToUpper(logLevel)), factorlog.StringToSeverity("PANIC"))
		if strings.EqualFold(logLevel, "trace") {
			log.SetVerbosity(LogVerbosityTrace)
		}
		if strings.EqualFold(logLevel, "debug") {
			log.SetVerbosity(LogVerbosityDebug)
		}
	}
}

func GetLogger() *factorlog.FactorLog {
	return log
}

// LogErrors can be used as error handler, logs error with debug log level.
func LogErrors(v ...any) {
	logWith().LogErrors(v...)
}

func buildFormatter(format string) *factorlog.StdFormatter {
	format = strings.ReplaceAll(format, "%{Pid}", fmt.Sprintf("%d", os.Getpid()))

	return (factorlog.NewStdFormatter(format))
}

// LogWriter implements the io.Writer interface and simply logs everything with given level.
type LogWriter struct {
	level string
}

func adjustMaxLogOutput(data string) string {
	length := len(data)
	logMaxOutput := int(LogMaxOutput.Load())
	if logMaxOutput > 0 && length > logMaxOutput {
		data = data[:logMaxOutput]

		return data + fmt.Sprintf("...[skipped logging %d bytes]", length-logMaxOutput)
	}

	return data
}

func (l *LogWriter) Write(p []byte) (n int, err error) {
	msg := adjustMaxLogOutput(strings.TrimSpace(string(p)))
	switch strings.ToLower(l.level) {
	case "error":
		log.Error(msg)
	case "warn":
		log.Warn(msg)
	case "info":
		log.Info(msg)
	}

	return 0, nil
}

func NewLogWriter(level string) *LogWriter {
	l := new(LogWriter)
	l.level = level

	return l
}

type LogPrefixer struct {
	pre []any
}

const LoggerCalldepth = 2

func (l *LogPrefixer) Panicf(format string, v ...any) {
	log.Output(factorlog.PANIC, LoggerCalldepth, fmt.Sprintf(l.prefix()+" "+format, v...))
}

func (l *LogPrefixer) Fatalf(format string, v ...any) {
	log.Output(factorlog.FATAL, LoggerCalldepth, fmt.Sprintf(l.prefix()+" "+format, v...))
}

func (l *LogPrefixer) Errorf(format string, v ...any) {
	log.Output(factorlog.ERROR, LoggerCalldepth, fmt.Sprintf(l.prefix()+" "+format, v...))
}

func (l *LogPrefixer) Warnf(format string, v ...any) {
	log.Output(factorlog.WARN, LoggerCalldepth, fmt.Sprintf(l.prefix()+" "+format, v...))
}

func (l *LogPrefixer) Infof(format string, v ...any) {
	if !log.IsV(LogVerbosityDefault) {
		return
	}
	log.Output(factorlog.INFO, LoggerCalldepth, adjustMaxLogOutput(fmt.Sprintf(l.prefix()+" "+format, v...)))
}

func (l *LogPrefixer) Debugf(format string, v ...any) {
	if !log.IsV(LogVerbosityDebug) {
		return
	}
	log.Output(factorlog.DEBUG, LoggerCalldepth, adjustMaxLogOutput(fmt.Sprintf(l.prefix()+" "+format, v...)))
}

func (l *LogPrefixer) Tracef(format string, v ...any) {
	if !log.IsV(LogVerbosityTrace) {
		return
	}
	log.Output(factorlog.TRACE, LoggerCalldepth, adjustMaxLogOutput(fmt.Sprintf(l.prefix()+" "+format, v...)))
}

// LogErrors can be used as generic logger with a prefix.
func (l *LogPrefixer) LogErrors(v ...any) {
	if !log.IsV(LogVerbosityDebug) {
		return
	}
	for _, e := range v {
		err, ok := e.(error)
		if !ok {
			continue
		}
		if err == nil {
			continue
		}
		l.Debugf("got error: %e: %s", err, err.Error())
		l.Debugf("Stacktrace:\n%s", debug.Stack())
	}
}

func (l *LogPrefixer) prefix() (prefix string) {
	for _, pre := range l.pre {
		if val, ok := pre.(string); ok {
			prefix = fmt.Sprintf("%s[%s]", prefix, val)

			continue
		}

		if pre == nil || reflect.ValueOf(pre).Pointer() == 0 {
			prefix = fmt.Sprintf("%s[%T(nil)]", prefix, pre)

			continue
		}
		switch val := pre.(type) {
		case string:
			prefix = fmt.Sprintf("%s[%s]", prefix, val)
		case *Peer:
			prefix = fmt.Sprintf("%s[%s]", prefix, val.Name)
		case *Request:
			prefix = fmt.Sprintf("%s[%s]", prefix, val.ID())
		case *Response:
			prefix = fmt.Sprintf("%s[%s]", prefix, val.request.ID())
		case *ClientConnection:
			prefix = fmt.Sprintf("%s[%s->%s]", prefix, val.remoteAddr, val.localAddr)
		case *DataRow:
			prefix = fmt.Sprintf("%s[%s]", prefix, val.dataStore.peer.Name)
		case *DataStore:
			prefix = fmt.Sprintf("%s[%s]", prefix, val.peer.Name)
		case *DataStoreSet:
			prefix = fmt.Sprintf("%s[%s]", prefix, val.peer.Name)
		case context.Context:
			for _, key := range []ContextKey{CtxPeer, CtxClient, CtxRequest} {
				value := val.Value(key)
				if value == nil {
					continue
				}
				prefix = fmt.Sprintf("%s[%v]", prefix, value)
			}
		default:
			log.Panicf("unsupported prefix type: %#v (%T)", pre, pre)
		}
	}

	return prefix
}

// return logger with prefixed strings from given objects.
func logWith(pre ...any) *LogPrefixer {
	return &LogPrefixer{pre: pre}
}
