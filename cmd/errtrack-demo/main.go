// errtrack-demo is a standalone harness that exercises the errtrack
// pipeline end-to-end without booting the full enclave. Useful to
// manually verify that:
//
//   - log entries flow into the store,
//   - the /errors endpoints serve them,
//   - secrets injected into messages and fields are scrubbed in the
//     served output.
//
// Not built into production images. Intended for local dev only.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/trackrecord/enclave/internal/errtrack"
	"github.com/trackrecord/enclave/internal/logredact"
	"github.com/trackrecord/enclave/internal/logstream"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	base, _ := zap.NewDevelopment()

	store := errtrack.NewStore(0, 0) // defaults: 1024 capacity, 50/sec
	server := logstream.NewServer(50052, "devkey", base)
	server.SetErrorStore(store)

	if err := server.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "log stream start: %v\n", err)
		os.Exit(1)
	}

	// Build the same chain as cmd/enclave/main.go:
	//   base -> redact -> errtrack
	logger := zap.New(
		errtrack.NewCore(
			logredact.NewRedactCore(base.Core()),
			store,
			zapcore.ErrorLevel,
		),
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)

	// Synthetic errors that exercise the redactor: an api_key in the
	// message, a sensitive field, and a benign duplicate to bump count.
	logger.Error("auth failed: api_key=AKIATESTLEAK0000",
		zap.String("user_id", "u-42"),
		zap.String("path", "/api/x"),
	)
	logger.Error("auth failed: api_key=AKIATESTLEAK0000",
		zap.String("user_id", "u-99"),
	)
	logger.Error("upstream timeout", zap.String("exchange", "binance"))

	fmt.Println()
	fmt.Println("errtrack demo running on https://localhost:50052")
	fmt.Println("test endpoints (note plaintext, no TLS in this harness):")
	fmt.Println(`  curl.exe -H "X-Api-Key: devkey" http://localhost:50052/errors/groups`)
	fmt.Println(`  curl.exe -H "X-Api-Key: devkey" http://localhost:50052/errors/stats`)
	fmt.Println()
	fmt.Println("Ctrl+C to stop.")

	// Wait for SIGINT so the user can curl the endpoints.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	_ = server.Stop()
	_ = logger.Sync()
}
