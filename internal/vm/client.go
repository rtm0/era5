package vm

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rtm0/era5/internal/era5"
)

// Client is a Victoria Metrics client capable of inserting ERA5 metrics via
// various protocols.
type Client struct {
	logger     *slog.Logger
	httpCli    *http.Client
	insertURL  string
	recsToText recsToTextFunc
}

// NewClient creates a new VM client.
func NewClient(logger *slog.Logger, insertURL string, maxConns int) (*Client, error) {
	url, err := url.Parse(insertURL)
	if err != nil {
		return nil, err
	}
	recsToText := supportedAPIs[url.Path]
	if recsToText == nil {
		return nil, fmt.Errorf("inserting into %q is not supported", insertURL)
	}

	return &Client{
		logger: logger,
		httpCli: &http.Client{
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:        maxConns,
				IdleConnTimeout:     30 * time.Second,
				MaxIdleConnsPerHost: maxConns,
				MaxConnsPerHost:     maxConns,
			},
		},
		insertURL:  insertURL,
		recsToText: recsToText,
	}, nil
}

// Insert inserts ERA5 records into Victoria Metrics.
func (c *Client) Insert(recs []era5.Record) {
	res, err := c.httpCli.Post(c.insertURL, "text/plain", c.recsToText(recs))
	if err != nil {
		c.logger.Error("Could not post data", "err", err)
		return
	}
	if res.StatusCode != http.StatusNoContent {
		c.logger.Error("Unexpected status", "code", res.StatusCode)
	}
	if _, err := io.Copy(io.Discard, res.Body); err != nil {
		c.logger.Error("Failed to drain response body", "err", err)
	}
	res.Body.Close()
}

type recsToTextFunc func([]era5.Record) io.Reader

var supportedAPIs = map[string]recsToTextFunc{
	"/influx/write":        recsToInfluxDB,
	"/influx/api/v2/write": recsToInfluxDB,
	"/write":               recsToInfluxDB,
	"/api/v2/write":        recsToInfluxDB,
}

var influxDBFmt = "era5,la=%.2f,lo=%.2f u10=%d,v10=%d,t2m=%d,sf=%d,tcc=%d,tp=%d %d\n"

// recsToInfluxDB converts ERA5 records into InfluxDB line protocol v2.
func recsToInfluxDB(recs []era5.Record) io.Reader {
	var sb strings.Builder
	for _, r := range recs {
		sb.WriteString(fmt.Sprintf(influxDBFmt, []any{
			r.Latitude,
			r.Longitude,
			r.ZonalWind10M,
			r.MeridionalWind10M,
			r.Temperature2M,
			r.Snowfall,
			r.TotalCloudCover,
			r.TotalPrecipitation,
			r.Timestamp,
		}...))
	}
	return strings.NewReader(sb.String())
}
