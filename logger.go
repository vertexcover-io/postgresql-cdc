package cdc

import (
	"fmt"
	"log"
	"os"
)

type logLevel int

const (
	DEBUG = iota
	INFO
	WARNING
	ERROR
	PANIC
	FATAL
	NEVER_LOG
)

var LEVEL_TO_STR = map[int]string{
	DEBUG:     "debug",
	INFO:      "info",
	WARNING:   "warning",
	ERROR:     "error",
	PANIC:     "panic",
	FATAL:     "fatal",
	NEVER_LOG: "never_log",
}

var STR_TO_LEVEL = map[string]int{
	"debug":   DEBUG,
	"info":    INFO,
	"warning": WARNING,
	"warn":    WARNING,
	"error":   ERROR,
	"panic":   PANIC,
	"fatal":   FATAL,
}

var logger Logger

func init() {
	logger = newDefaultLogger()
}

func ParseLevel(level string) int {
	return STR_TO_LEVEL[level]
}
func LevelToString(level int) string {
	return LEVEL_TO_STR[level]
}

type Logger interface {
	Debug(...interface{})
	Info(...interface{})
	Warn(...interface{})
	Error(...interface{})
	Fatal(...interface{})
	Panic(...interface{})
	SetLevel(int)
	Debugf(string, ...interface{})
	Infof(string, ...interface{})
	Warnf(string, ...interface{})
	Errorf(string, ...interface{})
	Fatalf(string, ...interface{})
	Panicf(string, ...interface{})
}

type defaultLogger struct {
	logger   *log.Logger
	minLevel int
}

func newDefaultLogger() *defaultLogger {
	return &defaultLogger{
		logger:   log.New(os.Stdout, "", log.LstdFlags),
		minLevel: WARNING,
	}
}

func (d *defaultLogger) prepareMsg(level int, msg string) string {
	return fmt.Sprintf("level=%s  message=%s", LEVEL_TO_STR[level], msg)
}

func (d *defaultLogger) logf(level int, msg string, v ...interface{}) {
	if d.minLevel <= level {
		msg = d.prepareMsg(level, msg)
		d.logger.Printf(msg, v...)
	}
}

func (d *defaultLogger) log(level int, v ...interface{}) {
	if d.minLevel <= level {
		msg := d.prepareMsg(level, "")
		d.logger.Print(append([]interface{}{msg}, v...)...)
	}
}

func (d *defaultLogger) SetLevel(level int) {
	if level < DEBUG || level > NEVER_LOG {
		panic(fmt.Sprintf("Invalid Log Level: %d", level))
	}
	d.minLevel = level
}

func (d *defaultLogger) Debug(v ...interface{}) {
	d.log(DEBUG, v...)
}

func (d *defaultLogger) Info(v ...interface{}) {
	d.log(INFO, v...)
}
func (d *defaultLogger) Warn(v ...interface{}) {
	d.log(WARNING, v...)
}
func (d *defaultLogger) Error(v ...interface{}) {
	d.log(ERROR, v...)
}

func (d *defaultLogger) Panic(v ...interface{}) {
	if d.minLevel <= PANIC {
		msg := d.prepareMsg(PANIC, "")
		d.logger.Panic(append([]interface{}{msg}, v...)...)
	}
}

func (d *defaultLogger) Fatal(v ...interface{}) {
	if d.minLevel <= FATAL {
		msg := d.prepareMsg(FATAL, "")
		d.logger.Fatal(append([]interface{}{msg}, v...)...)
	}
}

func (d *defaultLogger) Debugf(msg string, v ...interface{}) {
	d.logf(DEBUG, msg, v...)
}

func (d *defaultLogger) Infof(msg string, v ...interface{}) {
	d.logf(INFO, msg, v...)
}
func (d *defaultLogger) Warnf(msg string, v ...interface{}) {
	d.logf(WARNING, msg, v...)
}
func (d *defaultLogger) Errorf(msg string, v ...interface{}) {
	d.logf(ERROR, msg, v...)
}
func (d *defaultLogger) Panicf(msg string, v ...interface{}) {
	if d.minLevel <= PANIC {
		msg = d.prepareMsg(PANIC, msg)
		d.logger.Panicf(msg, v...)
	}
}
func (d *defaultLogger) Fatalf(msg string, v ...interface{}) {
	if d.minLevel <= FATAL {
		msg = d.prepareMsg(FATAL, msg)
		d.logger.Panicf(msg, v...)
	}
}
