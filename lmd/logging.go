package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime/debug"
	"strings"

	"github.com/kdar/factorlog"
)

// LogFormat sets the log format
var LogFormat string

// DateTimeLogFormat sets the log format for the date/time portion
var DateTimeLogFormat string

func init() {
	DateTimeLogFormat = `[%{Date} %{Time "15:04:05.000"}]`
	LogFormat = `[%{Severity}][pid:` + fmt.Sprintf("%d", os.Getpid()) + `][%{ShortFile}:%{Line}] %{Message}`
}

const (
	// LogColors sets colors for some log levels
	LogColors = `%{Color "yellow" "WARN"}%{Color "red" "ERROR"}%{Color "red" "FATAL"}`

	// LogColorReset resets colors from LogColors
	LogColorReset = `%{Color "reset"}`

	// LogVerbosityNone disables logging
	LogVerbosityNone = 0

	// LogVerbosityDefault sets the default log level
	LogVerbosityDefault = 1

	// LogVerbosityDebug sets the debug log level
	LogVerbosityDebug = 2

	// LogVerbosityTrace sets trace log level
	LogVerbosityTrace = 3
)

// initialize standard logger which will be configured later from the configuration file options
var log = factorlog.New(os.Stdout, factorlog.NewStdFormatter(LogFormat))

// InitLogging initializes the logging system.
func InitLogging(conf *Config) {
	var logFormatter factorlog.Formatter
	var targetWriter io.Writer
	var err error
	switch {
	case conf.LogFile == "" || conf.LogFile == "stdout":
		logFormatter = factorlog.NewStdFormatter(LogColors + DateTimeLogFormat + LogFormat + LogColorReset)
		targetWriter = os.Stdout
	case strings.EqualFold(conf.LogFile, "stderr"):
		logFormatter = factorlog.NewStdFormatter(LogColors + DateTimeLogFormat + LogFormat + LogColorReset)
		targetWriter = os.Stderr
	case conf.LogFile == "stdout-journal":
		logFormatter = factorlog.NewStdFormatter(LogFormat)
		targetWriter = os.Stdout
	default:
		logFormatter = factorlog.NewStdFormatter(DateTimeLogFormat + LogFormat)
		targetWriter, err = os.OpenFile(conf.LogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	}
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %s", err.Error()))
	}
	var LogLevel = "Warn"
	if conf.LogLevel != "" {
		LogLevel = conf.LogLevel
	}
	log.SetFormatter(logFormatter)
	log.SetOutput(targetWriter)
	log.SetVerbosity(LogVerbosityDefault)
	if strings.EqualFold(LogLevel, "off") {
		log.SetMinMaxSeverity(factorlog.StringToSeverity("PANIC"), factorlog.StringToSeverity("PANIC"))
		log.SetVerbosity(LogVerbosityNone)
	} else {
		log.SetMinMaxSeverity(factorlog.StringToSeverity(strings.ToUpper(LogLevel)), factorlog.StringToSeverity("PANIC"))
		if strings.EqualFold(LogLevel, "trace") {
			log.SetVerbosity(LogVerbosityTrace)
		}
		if strings.EqualFold(LogLevel, "debug") {
			log.SetVerbosity(LogVerbosityDebug)
		}
	}
}

// LogErrors can be used as error handler, logs error with debug log level
func LogErrors(v ...interface{}) {
	logWith().LogErrors(v...)
}

// LogWriter implements the io.Writer interface and simply logs everything with given level
type LogWriter struct {
	level string
}

func (l *LogWriter) Write(p []byte) (n int, err error) {
	msg := strings.TrimSpace(string(p))
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
	pre []interface{}
}

const LoggerCalldepth = 2

func (l *LogPrefixer) Panicf(format string, v ...interface{}) {
	log.Output(factorlog.PANIC, LoggerCalldepth, fmt.Sprintf(l.prefix()+" "+format, v...))
}

func (l *LogPrefixer) Fatalf(format string, v ...interface{}) {
	log.Output(factorlog.FATAL, LoggerCalldepth, fmt.Sprintf(l.prefix()+" "+format, v...))
}

func (l *LogPrefixer) Errorf(format string, v ...interface{}) {
	log.Output(factorlog.ERROR, LoggerCalldepth, fmt.Sprintf(l.prefix()+" "+format, v...))
}

func (l *LogPrefixer) Warnf(format string, v ...interface{}) {
	log.Output(factorlog.WARN, LoggerCalldepth, fmt.Sprintf(l.prefix()+" "+format, v...))
}

func (l *LogPrefixer) Infof(format string, v ...interface{}) {
	if !log.IsV(LogVerbosityDefault) {
		return
	}
	log.Output(factorlog.INFO, LoggerCalldepth, fmt.Sprintf(l.prefix()+" "+format, v...))
}

func (l *LogPrefixer) Debugf(format string, v ...interface{}) {
	if !log.IsV(LogVerbosityDebug) {
		return
	}
	log.Output(factorlog.DEBUG, LoggerCalldepth, fmt.Sprintf(l.prefix()+" "+format, v...))
}

func (l *LogPrefixer) Tracef(format string, v ...interface{}) {
	if !log.IsV(LogVerbosityTrace) {
		return
	}
	log.Output(factorlog.TRACE, LoggerCalldepth, fmt.Sprintf(l.prefix()+" "+format, v...))
}

// LogErrors can be used as generic logger with a prefix
func (l *LogPrefixer) LogErrors(v ...interface{}) {
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
		l.Debugf("got error: %e", err)
		l.Debugf("Stacktrace:\n%s", debug.Stack())
	}
}

func (l *LogPrefixer) prefix() (prefix string) {
	for _, p := range l.pre {
		if v, ok := p.(string); ok {
			prefix = fmt.Sprintf("%s[%s]", prefix, v)
			continue
		}

		if p == nil || reflect.ValueOf(p).Pointer() == 0 {
			prefix = fmt.Sprintf("%s[%T(nil)]", prefix, p)
			continue
		}
		switch v := p.(type) {
		case string:
			prefix = fmt.Sprintf("%s[%s]", prefix, v)
		case *Peer:
			prefix = fmt.Sprintf("%s[%s]", prefix, v.Name)
		case *Request:
			prefix = fmt.Sprintf("%s[%s]", prefix, v.ID())
		case *Response:
			prefix = fmt.Sprintf("%s[%s]", prefix, v.Request.ID())
		case *ClientConnection:
			prefix = fmt.Sprintf("%s[%s->%s]", prefix, v.remoteAddr, v.localAddr)
		case *DataRow:
			prefix = fmt.Sprintf("%s[%s]", prefix, v.DataStore.PeerName)
		case *DataStore:
			prefix = fmt.Sprintf("%s[%s]", prefix, v.PeerName)
		case *DataStoreSet:
			prefix = fmt.Sprintf("%s[%s]", prefix, v.peer.Name)
		case context.Context:
			for _, key := range []ContextKey{CtxPeer, CtxClient, CtxRequest} {
				value := v.Value(key)
				if value == nil {
					continue
				}
				prefix = fmt.Sprintf("%s[%v]", prefix, value)
			}
		default:
			log.Panicf("unsupported prefix type: %#v (%T)", p, p)
		}
	}
	return prefix
}

// return logger with prefixed strings from given objects
func logWith(pre ...interface{}) *LogPrefixer {
	return &LogPrefixer{pre: pre}
}
