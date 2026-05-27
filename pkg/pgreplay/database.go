package pgreplay

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/eapache/channels"
	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	pgx "github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	dto "github.com/prometheus/client_model/go"
)

var (
	connectionsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "pgreplay_connections_active",
			Help: "Number of connections currently open against Postgres",
		},
	)
	connectionsEstablishedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pgreplay_connections_established_total",
			Help: "Number of connections established against Postgres",
		},
	)
	itemsProcessedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pgreplay_items_processed_total",
			Help: "Total count of replay items that have been sent to the database",
		},
	)
	itemsMostRecentTimestamp = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "pgreplay_items_most_recent_timestamp",
			Help: "Most recent timestamp of processed items",
		},
	)
	itemsErrorTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pgreplay_items_error_total",
			Help: "Total count of replay items whose Handle returned an error",
		},
	)
	connectionsErrorTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pgreplay_connections_error_total",
			Help: "Total count of failed connection attempts",
		},
	)
)

// ErrorCounts returns the cumulative item-handle and connection error counts since
// process start. Useful for printing a one-line shutdown summary without scraping
// /metrics.
func ErrorCounts() (itemErrors, connectionErrors float64) {
	return counterValue(itemsErrorTotal), counterValue(connectionsErrorTotal)
}

func counterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		return 0
	}
	return m.GetCounter().GetValue()
}

func NewDatabase(ctx context.Context, cfg DatabaseConnConfig) (*Database, error) {
	cfg.Password = passwordForUser(cfg.User, cfg.Password)
	connConfig, err := pgx.ParseConfig(ParseConnData(cfg))
	if err != nil {
		return nil, err
	}

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	return &Database{cfg: connConfig, conns: map[SessionID]*Conn{}}, conn.Close(ctx)
}

func ParseConnData(cfg DatabaseConnConfig) string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.User, cfg.Password),
		Host:   fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Path:   "/" + cfg.Database,
	}
	return u.String()
}

type Database struct {
	cfg           *pgx.ConnConfig
	conns         map[SessionID]*Conn
	Aggregator    *Aggregator   // optional; if nil, replay durations are not recorded
	RttCorrection bool          // when true, subtract the per-connection measured RTT from each query's replay duration
	Logger        kitlog.Logger // used to log the measured RTT per connection; if nil, RTT is measured silently
}

// Consume iterates through all the items in the given channel and attempts to process
// them against the item's session connection. Consume returns two error channels, the
// first for per item errors that should be used for diagnostics only, and the second to
// indicate unrecoverable failures.
//
// Once all items have finished processing, both channels will be closed.
func (d *Database) Consume(ctx context.Context, items chan Item) (chan error, chan error) {
	var wg sync.WaitGroup

	errs, done := make(chan error, 10), make(chan error)

	go func() {
		for item := range items {
			var err error
			conn, ok := d.conns[item.GetSessionID()]

			// Connection did not exist, so create a new one
			if !ok {
				if conn, err = d.Connect(ctx, item); err != nil {
					connectionsErrorTotal.Inc()
					errs <- fmt.Errorf("connect failed for user=%s database=%s: %w", item.GetUser(), item.GetDatabase(), err)
					continue
				}

				d.conns[item.GetSessionID()] = conn

				wg.Add(1)
				connectionsEstablishedTotal.Inc()
				connectionsActive.Inc()

				go func(conn *Conn) {
					defer wg.Done()
					defer connectionsActive.Dec()

					if err := conn.Start(ctx, errs, d.Aggregator, d.Logger, d.RttCorrection); err != nil {
						errs <- err
					}
				}(conn)
			}

			conn.In() <- item
		}

		for _, conn := range d.conns {
			conn.Close()
		}

		// Wait for every connection to terminate
		wg.Wait()

		close(errs)
		close(done)
	}()

	return errs, done
}

// Connect establishes a new connection to the database, reusing the ConnInfo that was
// generated when the Database was constructed. The wg is incremented whenever we
// establish a new connection and decremented when we disconnect.
func (d *Database) Connect(ctx context.Context, item Item) (*Conn, error) {
	cfg := d.cfg.Copy()
	cfg.Database, cfg.User = item.GetDatabase(), item.GetUser()
	cfg.Password = passwordForUser(cfg.User, cfg.Password)

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return &Conn{conn, channels.NewInfiniteChannel(), sync.Once{}}, nil
}

// measureRTT estimates the round-trip latency of a connection by running n "SELECT 1"
// statements and returning the smallest observed duration. Min (rather than median) is
// chosen so we under-correct rather than over-correct — subtracting too much would zero
// out the duration of genuinely-slow queries. Returns 0 on context cancellation or if all
// probe queries fail; callers should treat that as "no correction applied".
func measureRTT(ctx context.Context, conn *pgx.Conn, n int) time.Duration {
	var best time.Duration
	for i := 0; i < n; i++ {
		if ctx.Err() != nil {
			break
		}
		start := time.Now()
		if _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
			continue
		}
		d := time.Since(start)
		if best == 0 || d < best {
			best = d
		}
	}
	return best
}

// passwordForUser returns DB_PASSWORD_<USER> if set, otherwise fallback. The user name is
// uppercased and '-' is replaced with '_' (e.g. metricstore-www-ro → DB_PASSWORD_METRICSTORE_WWW_RO).
func passwordForUser(user, fallback string) string {
	envName := "DB_PASSWORD_" + strings.ToUpper(strings.ReplaceAll(user, "-", "_"))
	if pw, ok := os.LookupEnv(envName); ok {
		return pw
	}
	return fallback
}

// Conn represents a single database connection handling a stream of work Items
type Conn struct {
	*pgx.Conn
	channels.Channel
	sync.Once
}

func (c *Conn) Close() {
	c.Once.Do(c.Channel.Close)
}

// Start begins to process the items that are placed into the Conn's channel. We'll finish
// once the connection has died or we run out of items to process. Per-item errors are
// surfaced via errs (non-blocking — dropped if the buffer is full, but counted). If agg
// is non-nil, each item's replay duration is recorded for performance comparison.
//
// When rttCorrection is true, a per-connection RTT baseline is measured at the start of
// the session (min of 5 SELECT 1) and subtracted from each query's replay duration before
// recording. This pushes the metric closer to the backend-only duration logged on the
// source (which excludes network/client overhead).
func (c *Conn) Start(ctx context.Context, errs chan<- error, agg *Aggregator, logger kitlog.Logger, rttCorrection bool) error {
	items := make(chan Item)
	channels.Unwrap(c.Channel, items)
	defer c.Close()

	var rtt time.Duration
	if rttCorrection {
		rtt = measureRTT(ctx, c.Conn, 5)
		if logger != nil {
			cfg := c.Conn.Config()
			level.Info(logger).Log(
				"event", "rtt.measured",
				"user", cfg.User,
				"database", cfg.Database,
				"rtt_ms", fmt.Sprintf("%.3f", float64(rtt.Microseconds())/1000.0),
			)
		}
	}

	for item := range items {
		if item == nil {
			continue
		}

		itemsProcessedTotal.Inc()
		itemsMostRecentTimestamp.Set(float64(item.GetTimestamp().Unix()))

		started := time.Now()
		err := item.Handle(ctx, c.Conn)
		elapsed := time.Since(started)

		adjusted := elapsed - rtt
		if adjusted < 0 {
			adjusted = 0
		}

		if agg != nil {
			agg.Record(item, adjusted)
		}

		if err != nil {
			itemsErrorTotal.Inc()
			select {
			case errs <- fmt.Errorf("query failed (user=%s session=%s): %w", item.GetUser(), item.GetSessionID(), err):
			default:
			}
		}

		// If we're no longer alive, then we know we can no longer process items
		if c.IsClosed() {
			return err
		}
	}

	// If we're still alive after consuming all our items, assume that we finished
	// processing our logs before we saw this connection be disconnected. We should
	// terminate ourselves by handling our own disconnect, so we can know when all our
	// connection are done.
	if !c.IsClosed() {
		Disconnect{}.Handle(ctx, c.Conn)
	}

	return nil
}
