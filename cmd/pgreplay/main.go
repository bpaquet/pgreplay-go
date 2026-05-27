package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	stdlog "log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	kingpin "github.com/alecthomas/kingpin/v2"
	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gocardless/pgreplay-go/pkg/pgreplay"
	"github.com/pkg/errors"
)

var logger kitlog.Logger

var (
	app = kingpin.New("pgreplay", "Replay Postgres logs against database").Version(versionStanza())

	// Global flags applying to every command
	debug          = app.Flag("debug", "Enable debug logging").Default("false").Bool()
	startFlag      = app.Flag("start", "Play logs from this time onward (accepts '"+pgreplay.PostgresTimestampFormat+"' or RFC 3339 like 2026-05-26T09:00:00Z)").String()
	finishFlag     = app.Flag("finish", "Stop playing logs at this time (accepts '"+pgreplay.PostgresTimestampFormat+"' or RFC 3339 like 2026-05-26T09:00:00Z)").String()
	metricsAddress = app.Flag("metrics-address", "Address to bind HTTP metrics listener").Default("0.0.0.0").String()
	metricsPort    = app.Flag("metrics-port", "Port to bind HTTP metrics listener").Default("9445").Uint16()

	filter                  = app.Command("filter", "Process an errlog file into a pgreplay preprocessed JSON log")
	filterJsonInput         = filter.Flag("json-input", "JSON input file. Repeatable; accepts shell globs like 'foo-*.json'.").Strings()
	filterErrlogInput       = filter.Flag("errlog-input", "Postgres errlog input file. Repeatable; accepts shell globs.").Strings()
	filterAuroraErrlogInput = filter.Flag("aurora-errlog-input", "Aurora/RDS PostgreSQL errlog input file. Repeatable; accepts shell globs like 'postgresql.log.*'. log_line_prefix='%t:%r:%u@%d:[%p]:'").Strings()
	filterCsvLogInput       = filter.Flag("csvlog-input", "Postgres CSV log input file. Repeatable; accepts shell globs.").Strings()
	filterOutput            = filter.Flag("output", "JSON output file").String()
	filterNullOutput        = filter.Flag("null-output", "Don't output anything, for testing parsing only").Bool()
	filterOnlyUsers         = filter.Flag("only-users", "Keep only items for these users (comma-separated). If unset, all users are kept.").String()
	filterIgnoreQuery       = filter.Flag("ignore-query", "Drop items whose query matches any of these regexes (comma-separated). Example: '^SET ,^SHOW'").String()

	run                  = app.Command("run", "Replay from log files against a real database")
	runHost              = run.Flag("host", "PostgreSQL database host").Required().String()
	runPort              = run.Flag("port", "PostgreSQL database port").Default("5432").Uint16()
	runDatname           = run.Flag("database", "PostgreSQL root database").Default("postgres").String()
	runUser              = run.Flag("user", "PostgreSQL root user").Default("postgres").String()
	runPassword          = run.Flag("password", "PostgreSQl password user (the default value is obtained from the DB_PASSWORD env var)").Default(os.Getenv("DB_PASSWORD")).String()
	runReplayRate        = run.Flag("replay-rate", "Rate of playback, will execute queries at Nx speed").Default("1").Float()
	runErrlogInput       = run.Flag("errlog-input", "Path to PostgreSQL errlog").ExistingFile()
	runAuroraErrlogInput = run.Flag("aurora-errlog-input", "Path to Aurora/RDS PostgreSQL errlog (log_line_prefix='%t:%r:%u@%d:[%p]:')").ExistingFile()
	runCsvLogInput       = run.Flag("csvlog-input", "Path to PostgreSQL CSV log").ExistingFile()
	runJsonInput         = run.Flag("json-input", "Path to preprocessed pgreplay JSON log file").ExistingFile()
	runLogSlowdownAbove  = run.Flag("log-slowdown-above", "Log queries whose replay/original ratio is at or above this value. 0 disables.").Default("0").Float()
	runLogSlowdownMin    = run.Flag("log-slowdown-min", "Skip slowdown logging for queries whose original duration was below this.").Default("100ms").Duration()
	runTopN              = run.Flag("top-n", "Print this many slowest fingerprints at shutdown. 0 disables.").Default("20").Int()
	runIgnoreQuery       = run.Flag("ignore-query-for-statistics", "Exclude items whose query matches any of these regexes from statistics (comma-separated). Items are still replayed; only stats are skipped.").String()
	runMaxDuration       = run.Flag("max-duration", "Replay at most this much original log time (derived as --finish = first_item_timestamp + max-duration). 0 disables.").Default("3m").Duration()
	runStatsCsv          = run.Flag("stats-csv", "Write per-fingerprint stats (count, avg/p50/p95/p99 original+replay ms and ratios, sample query) to this CSV file at shutdown.").String()
	runRttCorrection     = run.Flag("rtt-correction", "Subtract a measured per-connection RTT baseline (min of 5 SELECT 1 round-trips) from each query's replay duration so the stat is closer to the backend-only timing logged on the source. Use --no-rtt-correction to disable.").Default("true").Bool()
)

func main() {
	command := kingpin.MustParse(app.Parse(os.Args[1:]))

	logger = kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	logger = kitlog.With(logger, "ts", kitlog.DefaultTimestampUTC, "caller", kitlog.DefaultCaller)
	stdlog.SetOutput(kitlog.NewStdlibAdapter(logger))

	if *debug {
		logger = level.NewFilter(logger, level.AllowDebug())
	} else {
		logger = level.NewFilter(logger, level.AllowInfo())
	}

	// Starting the Prometheus Server
	server := pgreplay.StartPrometheusServer(logger, *metricsAddress, *metricsPort)

	var err error
	var start, finish *time.Time

	if start, err = parseTimestamp(*startFlag); err != nil {
		kingpin.Fatalf("--start flag %s", err)
	}

	if finish, err = parseTimestamp(*finishFlag); err != nil {
		kingpin.Fatalf("--finish flag %s", err)
	}

	switch command {
	case filter.FullCommand():
		var items chan pgreplay.Item

		switch checkSingleFormatMulti(filterJsonInput, filterErrlogInput, filterAuroraErrlogInput, filterCsvLogInput) {
		case filterJsonInput:
			items = parseLogs(*filterJsonInput, pgreplay.ParseJSON)
		case filterErrlogInput:
			items = parseLogs(*filterErrlogInput, pgreplay.ParseErrlog)
		case filterAuroraErrlogInput:
			items = parseLogs(*filterAuroraErrlogInput, pgreplay.ParseAuroraErrlog)
		case filterCsvLogInput:
			items = parseLogs(*filterCsvLogInput, pgreplay.ParseCsvLog)
		default:
			logger.Log("event", "postgres.error", "error", "you must provide an input")
			os.Exit(255)
		}

		items = filterByUsers(items, *filterOnlyUsers)
		items = filterByIgnoreQuery(items, *filterIgnoreQuery)

		// Apply the start and end filters
		items = pgreplay.NewStreamer(start, finish, logger).Filter(items)

		if *filterNullOutput {
			logger.Log("event", "filter.null_output", "msg", "Null output enabled, logs won't be serialized")
			for range items {
				// no-op
			}

			return
		}

		if *filterOutput == "" {
			kingpin.Fatalf("must provide output file when no --null-output")
		}

		outputFile, err := os.Create(*filterOutput)
		if err != nil {
			kingpin.Fatalf("failed to create output file: %v", err)
		}

		// Buffer the writes by 32MB to enable much faster filtering
		buffer := bufio.NewWriterSize(outputFile, 32*1000*1000)

		for item := range items {
			bytes, err := pgreplay.ItemMarshalJSON(item)
			if err != nil {
				kingpin.Fatalf("failed to serialize item: %v", err)
			}

			if _, err := buffer.Write(append(bytes, byte('\n'))); err != nil {
				kingpin.Fatalf("failed to write to output file: %v", err)
			}
		}

		buffer.Flush()
		outputFile.Close()

	case run.FullCommand():
		ctx := context.Background()
		database, err := pgreplay.NewDatabase(
			ctx,
			pgreplay.DatabaseConnConfig{
				Host:     *runHost,
				Port:     *runPort,
				Database: *runDatname,
				User:     *runUser,
				Password: *runPassword,
			},
		)

		if err != nil {
			logger.Log("event", "postgres.error", "error", err)
			os.Exit(255)
		}

		database.Aggregator = pgreplay.NewAggregator(*runLogSlowdownAbove, *runLogSlowdownMin, compileIgnoreRegexes(*runIgnoreQuery), logger)
		database.RttCorrection = *runRttCorrection
		database.Logger = logger

		var items chan pgreplay.Item

		switch checkSingleFormat(runJsonInput, runErrlogInput, runAuroraErrlogInput, runCsvLogInput) {
		case runJsonInput:
			items = parseLog(*runJsonInput, pgreplay.ParseJSON)
		case runErrlogInput:
			items = parseLog(*runErrlogInput, pgreplay.ParseErrlog)
		case runAuroraErrlogInput:
			items = parseLog(*runAuroraErrlogInput, pgreplay.ParseAuroraErrlog)
		case runCsvLogInput:
			items = parseLog(*runCsvLogInput, pgreplay.ParseCsvLog)
		default:
			logger.Log("event", "postgres.error", "error", "you must provide an input")
			os.Exit(255)
		}

		if *runMaxDuration > 0 {
			items, finish = applyMaxDuration(items, start, finish, *runMaxDuration)
		}

		replay_started := time.Now()
		stream, err := pgreplay.NewStreamer(start, finish, logger).Stream(items, *runReplayRate)
		if err != nil {
			kingpin.Fatalf("failed to start streamer: %s", err)
		}

		errs, done := database.Consume(ctx, stream)

		var status int

		for {
			select {
			case err := <-errs:
				if err != nil {
					logger.Log("event", "consume.error", "error", err)
				}
			case err := <-done:
				if err != nil {
					status = 255
				}

				logger.Log("event", "consume.finished", "error", err, "status", status)
				logger.Log("event", "time.elapsed", "total", buildTimeElapsed(replay_started))
				itemErrs, connErrs := pgreplay.ErrorCounts()
				logger.Log("event", "errors.summary", "item_errors", itemErrs, "connection_errors", connErrs)
				if database.Aggregator != nil && *runStatsCsv != "" {
					if err := writeStatsCsv(*runStatsCsv, database.Aggregator.TopByP95Ratio(0)); err != nil {
						logger.Log("event", "stats_csv.error", "error", err)
					} else {
						logger.Log("event", "stats_csv.written", "path", *runStatsCsv)
					}
				}
				if database.Aggregator != nil && *runTopN > 0 {
					for i, row := range database.Aggregator.TopByP95Ratio(*runTopN) {
						logger.Log(
							"event", "slow_fingerprint",
							"rank", i+1,
							"fingerprint", row.Fingerprint,
							"count", row.Count,
							"avg_ratio", fmt.Sprintf("%.2f", row.AvgRatio),
							"p50_ratio", fmt.Sprintf("%.2f", row.P50Ratio),
							"p95_ratio", fmt.Sprintf("%.2f", row.P95Ratio),
							"p99_ratio", fmt.Sprintf("%.2f", row.P99Ratio),
							"p50_original_ms", fmt.Sprintf("%.3f", row.P50OriginalMs),
							"p50_replay_ms", fmt.Sprintf("%.3f", row.P50ReplayMs),
							"p95_original_ms", fmt.Sprintf("%.3f", row.P95OriginalMs),
							"p95_replay_ms", fmt.Sprintf("%.3f", row.P95ReplayMs),
							"p99_original_ms", fmt.Sprintf("%.3f", row.P99OriginalMs),
							"p99_replay_ms", fmt.Sprintf("%.3f", row.P99ReplayMs),
						)
					}
				}
				logger.Log("event", "server.status", "message", "shutting down the server!")
				err = pgreplay.ShutdownServer(ctx, server)
				if err != nil {
					logger.Log("error", "server.shutdown", "message", err.Error())
				}

				os.Exit(status)
			}
		}
	}
}

// Set by goreleaser
var (
	Version   = "dev"
	Commit    = "none"
	Date      = "unknown"
	GoVersion = runtime.Version()
)

func versionStanza() string {
	return fmt.Sprintf(
		"pgreplay Version: %v\nGit SHA: %v\nGo Version: %v\nGo OS/Arch: %v/%v\nBuilt at: %v",
		Version, Commit, GoVersion, runtime.GOOS, runtime.GOARCH, Date,
	)
}

func checkSingleFormat(formats ...*string) (result *string) {
	var supplied = 0
	for _, format := range formats {
		if *format != "" {
			result = format
			supplied++
		}
	}

	if supplied != 1 {
		kingpin.Fatalf("must provide exactly one input format")
	}

	return result // which becomes the one that isn't empty
}

var (
	csvCommentRegex    = regexp.MustCompile(`(?s)/\*.*?\*/`)
	csvWhitespaceRegex = regexp.MustCompile(`\s+`)
)

// cleanSampleQuery makes a SQL string fit nicely in a CSV column: strip /* ... */
// comments (Datadog/SQL Commenter tags add a lot of noise without signal), collapse
// whitespace to single spaces, and truncate.
func cleanSampleQuery(q string) string {
	q = csvCommentRegex.ReplaceAllString(q, " ")
	q = csvWhitespaceRegex.ReplaceAllString(q, " ")
	q = strings.TrimSpace(q)
	const max = 200
	if len(q) > max {
		q = q[:max] + "…"
	}
	return q
}

// writeStatsCsv writes the full per-fingerprint summary to path. One row per fingerprint,
// sorted by descending p95 ratio.
func writeStatsCsv(path string, rows []pgreplay.FingerprintSummary) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{
		"fingerprint", "count",
		"avg_original_ms", "avg_replay_ms", "avg_ratio",
		"p50_original_ms", "p50_replay_ms", "p50_ratio",
		"p95_original_ms", "p95_replay_ms", "p95_ratio",
		"p99_original_ms", "p99_replay_ms", "p99_ratio",
		"sample_query",
	}); err != nil {
		return err
	}

	for _, r := range rows {
		if err := w.Write([]string{
			r.Fingerprint,
			strconv.FormatInt(r.Count, 10),
			f3(r.AvgOriginalMs), f3(r.AvgReplayMs), f2(r.AvgRatio),
			f3(r.P50OriginalMs), f3(r.P50ReplayMs), f2(r.P50Ratio),
			f3(r.P95OriginalMs), f3(r.P95ReplayMs), f2(r.P95Ratio),
			f3(r.P99OriginalMs), f3(r.P99ReplayMs), f2(r.P99Ratio),
			cleanSampleQuery(r.SampleQuery),
		}); err != nil {
			return err
		}
	}
	return w.Error()
}

func f2(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }
func f3(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }

// applyMaxDuration sets finish = base + max, where base is *start when --start was
// provided, otherwise the timestamp of the first log item (peeked from the channel). If
// finish was already set, the earlier of the two wins.
func applyMaxDuration(items chan pgreplay.Item, start, finish *time.Time, max time.Duration) (chan pgreplay.Item, *time.Time) {
	if start != nil {
		derived := start.Add(max)
		if finish == nil || derived.Before(*finish) {
			finish = &derived
		}
		return items, finish
	}

	var first pgreplay.Item
	for item := range items {
		if item != nil {
			first = item
			break
		}
	}
	if first == nil {
		return items, finish
	}

	derived := first.GetTimestamp().Add(max)
	if finish == nil || derived.Before(*finish) {
		finish = &derived
	}

	prepended := make(chan pgreplay.Item, pgreplay.ItemBufferSize)
	go func() {
		defer close(prepended)
		prepended <- first
		for item := range items {
			prepended <- item
		}
	}()
	return prepended, finish
}

// compileIgnoreRegexes parses a comma-separated list of regex patterns. Empty entries are
// skipped. On parse failure it kills the process via kingpin.Fatalf.
func compileIgnoreRegexes(csv string) []*regexp.Regexp {
	if csv == "" {
		return nil
	}
	var patterns []*regexp.Regexp
	for _, p := range strings.Split(csv, ",") {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		re, err := regexp.Compile(p)
		if err != nil {
			kingpin.Fatalf("invalid regex %q: %s", p, err)
		}
		patterns = append(patterns, re)
	}
	return patterns
}

// filterByIgnoreQuery drops items whose query matches any of the provided regexes
// (comma-separated). Items with an empty query (Connect, Disconnect) are passed through.
func filterByIgnoreQuery(items chan pgreplay.Item, csv string) chan pgreplay.Item {
	patterns := compileIgnoreRegexes(csv)
	if len(patterns) == 0 {
		return items
	}

	out := make(chan pgreplay.Item, pgreplay.ItemBufferSize)
	go func() {
		defer close(out)
		for item := range items {
			if item == nil {
				continue
			}
			q := item.GetQuery()
			if q != "" {
				drop := false
				for _, re := range patterns {
					if re.MatchString(q) {
						drop = true
						break
					}
				}
				if drop {
					continue
				}
			}
			out <- item
		}
	}()
	return out
}

// filterByUsers returns a channel that only yields items whose user is in the allowlist
// (a comma-separated string). If empty, the original channel is returned unchanged.
func filterByUsers(items chan pgreplay.Item, csv string) chan pgreplay.Item {
	if csv == "" {
		return items
	}

	allowed := make(map[string]bool)
	for _, u := range strings.Split(csv, ",") {
		if u = strings.TrimSpace(u); u != "" {
			allowed[u] = true
		}
	}
	if len(allowed) == 0 {
		return items
	}

	out := make(chan pgreplay.Item, pgreplay.ItemBufferSize)
	go func() {
		defer close(out)
		for item := range items {
			if item != nil && allowed[item.GetUser()] {
				out <- item
			}
		}
	}()
	return out
}

// expandGlobs resolves shell-style wildcards (* ? [...]), leading ~, and $VAR/${VAR}
// references. Literal paths pass through unchanged (after var/tilde expansion). A glob
// that matches nothing is a fatal error to avoid silent typos. Matches are sorted
// lexicographically so log-rotation filenames stream in chronological order.
func expandGlobs(patterns []string) []string {
	var out []string
	for _, p := range patterns {
		p = expandPath(p)
		if !strings.ContainsAny(p, "*?[") {
			out = append(out, p)
			continue
		}
		matches, err := filepath.Glob(p)
		if err != nil {
			kingpin.Fatalf("invalid glob pattern %q: %s", p, err)
		}
		if len(matches) == 0 {
			kingpin.Fatalf("glob pattern %q matched no files", p)
		}
		sort.Strings(matches)
		out = append(out, matches...)
	}
	return out
}

// expandPath expands $VAR / ${VAR} and a leading ~/ in the input path.
func expandPath(p string) string {
	p = os.ExpandEnv(p)
	if strings.HasPrefix(p, "~/") || p == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			if p == "~" {
				p = home
			} else {
				p = filepath.Join(home, p[2:])
			}
		}
	}
	return p
}

// checkSingleFormatMulti is the *[]string analogue of checkSingleFormat — used by the
// filter command where each input flag is repeatable.
func checkSingleFormatMulti(formats ...*[]string) *[]string {
	var result *[]string
	supplied := 0
	for _, f := range formats {
		if len(*f) > 0 {
			result = f
			supplied++
		}
	}
	if supplied != 1 {
		kingpin.Fatalf("must provide exactly one input format")
	}
	return result
}

// parseLogs concatenates the given files (after glob expansion) via io.MultiReader and
// feeds them through the parser as a single stream. Glob patterns are sorted
// lexicographically so log rotations with date-stamped names play back in order.
func parseLogs(paths []string, parser pgreplay.ParserFunc) chan pgreplay.Item {
	expanded := expandGlobs(paths)
	logger.Log("event", "parse.inputs", "count", len(expanded), "files", strings.Join(expanded, ","))

	readers := make([]io.Reader, 0, len(expanded))
	for _, p := range expanded {
		f, err := os.Open(p)
		if err != nil {
			kingpin.Fatalf("failed to open logfile %s: %s", p, err)
		}
		readers = append(readers, f)
	}

	items, logerrs, done := parser(io.MultiReader(readers...))

	go func() {
		logger.Log("event", "parse.finished", "error", <-done)
	}()

	go func() {
		for err := range logerrs {
			level.Debug(logger).Log("event", "parse.error", "error", err)
		}
	}()

	return items
}

func parseLog(path string, parser pgreplay.ParserFunc) chan pgreplay.Item {
	file, err := os.Open(path)
	if err != nil {
		kingpin.Fatalf("failed to open logfile: %s", err)
	}

	items, logerrs, done := parser(file)

	go func() {
		logger.Log("event", "parse.finished", "error", <-done)
	}()

	go func() {
		for err := range logerrs {
			level.Debug(logger).Log("event", "parse.error", "error", err)
		}
	}()

	return items
}

// parseTimestamp accepts either Postgres-flavored timestamps or RFC 3339
// (e.g. 2026-05-26T09:00:00Z, 2026-05-26T11:00:00+02:00).
func parseTimestamp(in string) (*time.Time, error) {
	if in == "" {
		return nil, nil
	}

	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		pgreplay.PostgresTimestampFormat,
	} {
		if t, err := time.Parse(layout, in); err == nil {
			return &t, nil
		}
	}

	return nil, errors.Errorf("must be a valid timestamp (RFC 3339 or '%s')", pgreplay.PostgresTimestampFormat)
}

func buildTimeElapsed(start time.Time) string {
	const day = time.Minute * 60 * 24

	duration := time.Since(start)

	if duration < 0 {
		duration *= -1
	}

	if duration < day {
		return duration.String()
	}

	n := duration / day
	duration -= n * day

	if duration == 0 {
		return fmt.Sprintf("%dd", n)
	}

	return fmt.Sprintf("%dd%s", n, duration)
}
