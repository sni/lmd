package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/kdar/factorlog"
)

const logFormat = "[%{Date} %{Time}][%{Severity}][%{File}:%{Line}] %{Message}"
const logColors = "%{Color \"yellow\" \"WARN\"}%{Color \"red\" \"ERROR\"}"
const logColorReset = "%{Color \"reset\"}"

var log *factorlog.FactorLog

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
	logger := factorlog.New(targetWriter, logFormatter)
	if strings.ToLower(LogLevel) == "off" {
		logger.SetMinMaxSeverity(factorlog.StringToSeverity("PANIC"), factorlog.StringToSeverity("PANIC"))
	} else {
		logger.SetMinMaxSeverity(factorlog.StringToSeverity(strings.ToUpper(LogLevel)), factorlog.StringToSeverity("PANIC"))
	}
	log = logger
}
