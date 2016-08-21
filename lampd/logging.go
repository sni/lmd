package main

import (
	"fmt"
	"github.com/kdar/factorlog"
	"io"
	"os"
	"strings"
)

const logFormat = "[%{Date} %{Time}][%{Severity}][%{File}:%{Line}] %{Message}"
const logColors = "%{Color \"reset\"}%{Color \"yellow\" \"WARN\"}%{Color \"red\" \"ERROR\"}"

var log *factorlog.FactorLog

func InitLogging(conf *Config) {
	var logFormatter factorlog.Formatter
	var targetWriter io.Writer
	var err error
	if conf.LogFile == "" {
		logFormatter = factorlog.NewStdFormatter(logColors + logFormat)
		targetWriter = os.Stdout
	} else {
		logFormatter = factorlog.NewStdFormatter(logFormat)
		if _, err = os.Stat(conf.LogFile); err != nil {
			targetWriter, err = os.Create(conf.LogFile)
		} else {
			targetWriter, err = os.Open(conf.LogFile)
		}
	}
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger", err.Error()))
	}
	log = factorlog.New(targetWriter, logFormatter)
	var LogLevel = "Warn"
	if conf.LogLevel != "" {
		LogLevel = conf.LogLevel
	}
	log.SetMinMaxSeverity(factorlog.StringToSeverity(strings.ToUpper(LogLevel)), factorlog.StringToSeverity("PANIC"))
}
