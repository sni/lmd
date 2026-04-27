package lmd

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kdar/factorlog"
)

type cmdFlags struct {
	flagRateLimit        string
	flagRateInput        string
	flagRateOutput       string
	flagVersion          bool
	flagVerbose          bool
	flagVeryVerbose      bool
	flagTraceVerbose     bool
	flagTraceVerboseFull bool
}

type Cmd struct {
	flags cmdFlags
}

func MainLQ(build string) {
	Build = build
	cmd := &Cmd{}
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: lq [arguments] unix socket|tcp connection string\n")
		fmt.Fprintf(flag.CommandLine.Output(), "\nDescription:\n\n  lq sends a livestatus query.\n\nArguments:\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "\nExample:\n\n echo -e 'GET services\\n\\n' | lq localhost:6557\n\n")
	}
	flag.StringVar(&cmd.flags.flagRateLimit, "r", "", "enable rate limit (input/output), ex.: 1K")
	flag.StringVar(&cmd.flags.flagRateInput, "rate-input", "", "enable rate limit for input only, ex.: 1K")
	flag.StringVar(&cmd.flags.flagRateOutput, "rate-output", "", "enable rate limit for output only, ex.: 1K")
	flag.BoolVar(&cmd.flags.flagVerbose, "v", false, "enable verbose output")
	flag.BoolVar(&cmd.flags.flagVerbose, "verbose", false, "enable verbose output")
	flag.BoolVar(&cmd.flags.flagVeryVerbose, "vv", false, "enable very verbose output")
	flag.BoolVar(&cmd.flags.flagTraceVerbose, "vvv", false, "enable trace output")
	flag.BoolVar(&cmd.flags.flagTraceVerboseFull, "vvvv", false, "enable trace output with full query response")
	flag.BoolVar(&cmd.flags.flagVersion, "V", false, "print version and exit")
	flag.BoolVar(&cmd.flags.flagVersion, "version", false, "print version and exit")
	flag.Parse()
	applyVerboseFlags(&cmd.flags)

	connectionStr := ""

	if len(flag.Args()) == 0 {
		if os.Getenv("OMD_ROOT") != "" {
			connectionStr = os.Getenv("OMD_ROOT") + "/tmp/run/live"
		} else {
			fmt.Fprintf(os.Stderr, "ERROR: must specify at least one connection. See -h/--help for all options.\n")
			os.Exit(1)
		}
	} else {
		connectionStr = flag.Args()[0]
	}

	// parse remaining flags
	if len(flag.Args()) > 1 {
		os.Args = flag.Args()
		flag.Parse()
	}

	if cmd.flags.flagVersion {
		fmt.Fprintf(os.Stdout, "lq - version %s\n", Version())
		os.Exit(0)
	}

	inputDelay, outputDelay := parseDelays(cmd.flags)
	readSize := int64(1)
	if outputDelay <= time.Microsecond {
		readSize *= 10000
		outputDelay *= 10000
	}
	if outputDelay <= time.Millisecond {
		readSize *= 100
		outputDelay *= 100
	}

	// open connection
	daemon := NewLMDInstance()
	daemon.Config = NewConfig([]string{})
	daemon.Config.ValidateConfig()
	log.Debugf("connecting to %s", connectionStr)
	peer := NewPeer(daemon, &Connection{Source: []string{connectionStr}, Name: "lq", ID: "lq"})
	conn, connType, err := peer.GetConnection(&Request{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: connection failed: %s\n", err.Error())
		os.Exit(1)
	}

	switch connType {
	case ConnTypeUnix, ConnTypeTCP:
	default:
		fmt.Fprintf(os.Stderr, "ERROR: connection type not supported: %s\n", connType)
		os.Exit(1)
	}

	// read query from stdin
	scanner := bufio.NewScanner(os.Stdin)
	query := ""
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		log.Tracef("stdin: %s", line)
		if line == "" {
			processQuery(query, conn, readSize, inputDelay, outputDelay, false)
			query = ""

			continue
		}

		query += line + "\n"
	}

	if query == "" {
		fmt.Fprintf(os.Stderr, "ERROR: empty query\n")
		os.Exit(1)
	}

	processQuery(query, conn, readSize, inputDelay, outputDelay, true)
}

func applyVerboseFlags(flags *cmdFlags) {
	log.SetFormatter(factorlog.NewStdFormatter(LogToolsOutput))
	log.SetOutput(os.Stderr)
	log.SetVerbosity(LogVerbosityDefault)
	log.SetMinMaxSeverity(factorlog.StringToSeverity("ERROR"), factorlog.StringToSeverity("PANIC"))

	if flags.flagVerbose {
		log.SetVerbosity(LogVerbosityDefault)
		log.SetMinMaxSeverity(factorlog.StringToSeverity("INFO"), factorlog.StringToSeverity("PANIC"))
	}

	if flags.flagVeryVerbose {
		log.SetVerbosity(LogVerbosityDebug)
		log.SetMinMaxSeverity(factorlog.StringToSeverity("VERBOSE"), factorlog.StringToSeverity("PANIC"))
		log.Debugf("log level debug initialized")
	}

	if flags.flagTraceVerbose || flags.flagTraceVerboseFull {
		log.SetMinMaxSeverity(factorlog.StringToSeverity("TRACE"), factorlog.StringToSeverity("PANIC"))
		log.SetVerbosity(LogVerbosityTrace)
		log.Debugf("log level trace initialized")
	}
}

func processQuery(query string, conn net.Conn, readSize int64, inputDelay, outputDelay time.Duration, finalQuery bool) {
	log.Tracef("sending query: %s", strings.ReplaceAll(query, "\n", "\\n")+"\\n")
	for _, c := range query {
		_, err := fmt.Fprintf(conn, "%c", c)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: failed to send query: %s\n", err.Error())
			os.Exit(1)
		}
		if inputDelay > 0 {
			time.Sleep(inputDelay)
		}
	}
	fmt.Fprintf(conn, "\n")

	if finalQuery {
		err := closeWrite(conn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: failed to send query: %s\n", err.Error())
			os.Exit(1)
		}
	}

	// read response with configured delay
	for {
		body := new(bytes.Buffer)
		log.Tracef("going to read %d bytes of response", readSize)
		numRes, err := io.CopyN(body, conn, readSize)
		log.Tracef("read %d bytes of response", numRes)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Fprintf(os.Stderr, "ERROR: failed to read response: %s\n", err.Error())
				os.Exit(1)
			}
			log.Tracef("read finished: %s", err.Error())
		}

		fmt.Fprintf(os.Stdout, "%s", body.String())
		if outputDelay > 0 {
			time.Sleep(outputDelay)
		}
		if err != nil {
			break
		}
	}
}

func parseRateDelay(rate string) time.Duration {
	rate = strings.ToLower(rate)
	multi := int64(1)

	switch {
	case strings.HasSuffix(rate, "k"):
		rate = strings.TrimSuffix(rate, "k")
		multi = 1e3
	case strings.HasSuffix(rate, "m"):
		rate = strings.TrimSuffix(rate, "m")
		multi = 1e6
	case strings.HasSuffix(rate, "g"):
		rate = strings.TrimSuffix(rate, "g")
		multi = 1e9
	}

	value, err := strconv.ParseInt(rate, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot parse rate: %s\n", err.Error())
		os.Exit(1)
	}

	value *= multi

	// value is the number of bytes per second
	return time.Second / time.Duration(value)
}

func parseDelays(flags cmdFlags) (inputDelay, outputDelay time.Duration) {
	if flags.flagRateLimit != "" {
		outputDelay = parseRateDelay(flags.flagRateLimit)
		inputDelay = outputDelay
	}
	if flags.flagRateInput != "" {
		inputDelay = parseRateDelay(flags.flagRateInput)
	}
	if flags.flagRateOutput != "" {
		outputDelay = parseRateDelay(flags.flagRateOutput)
	}

	return inputDelay, outputDelay
}
