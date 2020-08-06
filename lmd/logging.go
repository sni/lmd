package main

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strings"

	"github.com/kdar/factorlog"
)

const (
	// LogFormat sets the log format
	LogFormat = `[%{Date} %{Time "15:04:05.000"}][%{Severity}][%{File}:%{Line}] %{Message}`

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
var log = factorlog.New(os.Stdout, factorlog.NewStdFormatter(`%{Date} %{Time "15:04:05.000"} %{File}:%{Line} %{Message}`))

// InitLogging initializes the logging system.
func InitLogging(conf *Config) {
	var logFormatter factorlog.Formatter
	var targetWriter io.Writer
	var err error
	switch {
	case conf.LogFile == "":
		logFormatter = factorlog.NewStdFormatter(LogColors + LogFormat + LogColorReset)
		targetWriter = os.Stdout
	case strings.EqualFold(conf.LogFile, "stderr"):
		logFormatter = factorlog.NewStdFormatter(LogColors + LogFormat + LogColorReset)
		targetWriter = os.Stderr
	default:
		logFormatter = factorlog.NewStdFormatter(LogFormat)
		targetWriter, err = os.OpenFile(conf.LogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
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

// can be used as error handler
func logDebugError(err error) {
	if err == nil {
		return
	}
	log.Debugf("got error: %e", err)
	log.Debugf("Stacktrace:\n%s", debug.Stack())
}

// can be used as error handler
func logDebugError2(_ interface{}, err error) {
	if err == nil {
		return
	}
	log.Debugf("got error: %e", err)
	log.Debugf("Stacktrace:\n%s", debug.Stack())
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
