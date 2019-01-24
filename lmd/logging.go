package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/kdar/factorlog"
)

const logFormat = `[%{Date} %{Time "15:04:05.000"}][%{Severity}][%{File}:%{Line}] %{Message}`
const logColors = `%{Color "yellow" "WARN"}%{Color "red" "ERROR"}%{Color "red" "FATAL"}`
const logColorReset = `%{Color "reset"}`

// initialize standard logger which will be configured later from the configuration file options
var log = factorlog.New(os.Stdout, factorlog.NewStdFormatter(`%{Date} %{Time "15:04:05.000"} %{File}:%{Line} %{Message}`))

// InitLogging initializes the logging system.
func InitLogging(conf *Config) {
	var logFormatter factorlog.Formatter
	var targetWriter io.Writer
	var err error
	if conf.LogFile == "" {
		logFormatter = factorlog.NewStdFormatter(logColors + logFormat + logColorReset)
		targetWriter = os.Stdout
	} else if strings.ToLower(conf.LogFile) == "stderr" {
		logFormatter = factorlog.NewStdFormatter(logColors + logFormat + logColorReset)
		targetWriter = os.Stderr
	} else {
		logFormatter = factorlog.NewStdFormatter(logFormat)
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
	log.SetVerbosity(1)
	if strings.ToLower(LogLevel) == "off" {
		log.SetMinMaxSeverity(factorlog.StringToSeverity("PANIC"), factorlog.StringToSeverity("PANIC"))
		log.SetVerbosity(0)
	} else {
		log.SetMinMaxSeverity(factorlog.StringToSeverity(strings.ToUpper(LogLevel)), factorlog.StringToSeverity("PANIC"))
		if strings.ToLower(LogLevel) == "trace" {
			log.SetVerbosity(3)
		}
		if strings.ToLower(LogLevel) == "debug" {
			log.SetVerbosity(2)
		}
	}
}
