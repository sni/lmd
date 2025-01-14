package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"pkg/lmd"
)

// Build contains the current git commit id
// compile passing -ldflags "-X main.Build <build sha1>" to set the id.
var Build string

type Cmd struct {
	flags struct {
		flagRateLimit string
		flagVersion   bool
		flagVerbose   bool
	}
}

func main() {
	cmd := &Cmd{}
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [arguments] unix socket|tcp connection string\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "\nDescription:\n\n  lq sends a livestatus query.\n\nArguments:\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "\nExample:\n\n echo -e 'GET services\\n\\n' lq localhost:6557\n\n")
	}
	flag.StringVar(&cmd.flags.flagRateLimit, "r", "", "enable rate limit, ex.: 1K")
	flag.StringVar(&cmd.flags.flagRateLimit, "limit-rate", "", "enable rate limit, ex.: 1K")
	flag.BoolVar(&cmd.flags.flagVerbose, "v", false, "enable verbose output")
	flag.BoolVar(&cmd.flags.flagVerbose, "verbose", false, "enable verbose output")
	flag.BoolVar(&cmd.flags.flagVersion, "V", false, "print version and exit")
	flag.BoolVar(&cmd.flags.flagVersion, "version", false, "print version and exit")
	flag.Parse()

	if cmd.flags.flagVersion {
		lmd.Build = Build
		fmt.Fprintf(os.Stdout, "lb - version %s\n", lmd.Version())
		os.Exit(0)
	}

	if len(flag.Args()) == 0 {
		fmt.Fprintf(os.Stderr, "ERROR: must specify at least one connection. See -h/--help for all options.\n")
		os.Exit(1)
	}

	// open connection
	daemon := lmd.NewLMDInstance()
	daemon.Config = lmd.NewConfig([]string{})
	daemon.Config.ValidateConfig()
	peer := lmd.NewPeer(daemon, &lmd.Connection{Source: flag.Args(), Name: "lq", ID: "lq"})
	conn, connType, err := peer.GetConnection(&lmd.Request{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: connection failed: %s\n", err.Error())
		os.Exit(1)
	}

	switch connType {
	case lmd.ConnTypeUnix, lmd.ConnTypeTCP:
	default:
		fmt.Fprintf(os.Stderr, "ERROR: connection type not supported: %s\n", connType)
		os.Exit(1)
	}

	// read query from stdin
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		query := strings.TrimSpace(scanner.Text())
		if query == "" {
			break
		}
		_, err := fmt.Fprintf(conn, "%s\n", query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: failed to send query: %s\n", err.Error())
			os.Exit(1)
		}
		fmt.Fprintf(conn, "\n")

		// read response with configured delay
		for {
			body := new(bytes.Buffer)
			_, err := io.CopyN(body, conn, 1)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					fmt.Fprintf(os.Stderr, "ERROR: failed to read response: %s\n", err.Error())
					os.Exit(1)
				}

				break
			}
			fmt.Fprintf(os.Stdout, "%s", body.String())
		}
	}
}
