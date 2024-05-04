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
	logger    *slog.Logger
	httpCli   *http.Client
	insertURL string
	recToText recToTextFunc
}

// NewClient creates a new VM client.
func NewClient(logger *slog.Logger, insertURL string, maxConns int) (*Client, error) {
	url, err := url.Parse(insertURL)
	if err != nil {
		return nil, err
	}
	recToText := supportedAPIs[url.Path]
	if recToText == nil {
		return nil, fmt.Errorf("inserting into %q is not supported", insertURL)
	}

	q := url.Query()
	for name, value := range apiParams[url.Path] {
		q.Add(name, value)
	}
	url.RawQuery = q.Encode()

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
		insertURL: url.String(),
		recToText: recToText,
	}, nil
}

// Insert inserts ERA5 records into Victoria Metrics.
func (c *Client) Insert(recs []era5.Record) {
	res, err := c.httpCli.Post(c.insertURL, "text/plain", recsToText(recs, c.recToText))
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

type recToTextFunc func(*strings.Builder, *era5.Record)

// recsToText converts multiple ERA5 records to text.
func recsToText(recs []era5.Record, recToText recToTextFunc) io.Reader {
	var sb strings.Builder
	for _, r := range recs {
		recToText(&sb, &r)
		sb.WriteString("\n")
	}
	return strings.NewReader(sb.String())
}

var supportedAPIs = map[string]recToTextFunc{
	"/influx/write":        recToInfluxDB,
	"/influx/api/v2/write": recToInfluxDB,
	"/write":               recToInfluxDB,
	"/api/v2/write":        recToInfluxDB,
	"/api/v1/import/csv":   recToCSV,
}

var apiParams = map[string]map[string]string{
	"/api/v1/import/csv": map[string]string{
		"format": "" +
			"1:time:unix_ms," +
			"2:label:la," +
			"3:label:lo," +
			"4:metric:era5_u10," +
			"5:metric:era5_v10," +
			"6:metric:era5_t2m," +
			"7:metric:era5_sf," +
			"8:metric:era5_tcc," +
			"9:metric:era5_tp",
	},
}

var influxDBFmt = "era5,la=%.2f,lo=%.2f u10=%d,v10=%d,t2m=%d,sf=%d,tcc=%d,tp=%d %d"

// recToInfluxDB converts a ERA5 record into InfluxDB line protocol v2 and
// appends it to the string builder.
func recToInfluxDB(sb *strings.Builder, r *era5.Record) {
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

var csvFmt = "%d,%.2f,%.2f,%d,%d,%d,%d,%d,%d"

// recToCSV converts an ERA5 record into a CSV record and appends it to the
// string builder.
func recToCSV(sb *strings.Builder, r *era5.Record) {
	sb.WriteString(fmt.Sprintf(csvFmt, []any{
		r.Timestamp,
		r.Latitude,
		r.Longitude,
		r.ZonalWind10M,
		r.MeridionalWind10M,
		r.Temperature2M,
		r.Snowfall,
		r.TotalCloudCover,
		r.TotalPrecipitation,
	}...))
}
