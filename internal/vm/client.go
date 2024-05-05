package vm

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/rtm0/era5/internal/era5"
)

// Client is a Victoria Metrics client capable of inserting ERA5 metrics via
// various protocols.
type Client struct {
	logger       *slog.Logger
	httpCli      *http.Client
	insertURL    string
	metricPrefix string
	recToText    recToTextFunc
}

const metricPrefixRE = "^[a-zA-Z0-9]+$"

// NewClient creates a new VM client.
func NewClient(logger *slog.Logger, insertURL string, maxConns int, metricPrefix string) (*Client, error) {
	url, err := url.Parse(insertURL)
	if err != nil {
		return nil, err
	}

	matches, err := regexp.Match(metricPrefixRE, []byte(metricPrefix))
	if err != nil {
		return nil, err
	}
	if !matches {
		return nil, fmt.Errorf("metric prefix %q does not match %q regular expression", metricPrefix, metricPrefixRE)
	}

	apiParams := apiParamsFuncs[url.Path]
	if apiParams == nil {
		return nil, fmt.Errorf("inserting into %q is not supported", insertURL)
	}
	q := url.Query()
	for name, value := range apiParams(metricPrefix) {
		q.Add(name, value)
	}
	url.RawQuery = q.Encode()

	recToText := recToTextFuncs[url.Path]
	if recToText == nil {
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
		insertURL:    url.String(),
		metricPrefix: metricPrefix,
		recToText:    recToText,
	}, nil
}

// Insert inserts ERA5 records into Victoria Metrics.
func (c *Client) Insert(recs []era5.Record) {
	res, err := c.httpCli.Post(c.insertURL, "text/plain", recsToText(recs, c.metricPrefix, c.recToText))
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

type apiParamsFunc func(string) map[string]string

var apiParamsFuncs = map[string]apiParamsFunc{
	"/influx/write":        influxDBAPIParams,
	"/influx/api/v2/write": influxDBAPIParams,
	"/write":               influxDBAPIParams,
	"/api/v2/write":        influxDBAPIParams,
	"/api/v1/import/csv":   csvAPIParams,
}

func influxDBAPIParams(metricPrefix string) map[string]string {
	return nil
}

func csvAPIParams(metricPrefix string) map[string]string {
	return map[string]string{
		"format": fmt.Sprintf(""+
			"1:time:unix_ms,"+
			"2:label:la,"+
			"3:label:lo,"+
			"4:metric:%[1]s_u10,"+
			"5:metric:%[1]s_v10,"+
			"6:metric:%[1]s_t2m,"+
			"7:metric:%[1]s_sf,"+
			"8:metric:%[1]s_tcc,"+
			"9:metric:%[1]s_tp", metricPrefix),
	}
}

type recToTextFunc func(*strings.Builder, *era5.Record, string)

// recsToText converts multiple ERA5 records to text.
func recsToText(recs []era5.Record, metricPrefix string, recToText recToTextFunc) io.Reader {
	var sb strings.Builder
	for _, r := range recs {
		recToText(&sb, &r, metricPrefix)
		sb.WriteString("\n")
	}
	return strings.NewReader(sb.String())
}

var recToTextFuncs = map[string]recToTextFunc{
	"/influx/write":        recToInfluxDB,
	"/influx/api/v2/write": recToInfluxDB,
	"/write":               recToInfluxDB,
	"/api/v2/write":        recToInfluxDB,
	"/api/v1/import/csv":   recToCSV,
}

var influxDBFmt = "%s,la=%.2f,lo=%.2f u10=%d,v10=%d,t2m=%d,sf=%d,tcc=%d,tp=%d %d"

// recToInfluxDB converts a ERA5 record into InfluxDB line protocol v2 and
// appends it to the string builder.
func recToInfluxDB(sb *strings.Builder, r *era5.Record, metricPrefix string) {
	sb.WriteString(fmt.Sprintf(influxDBFmt, []any{
		metricPrefix,
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
func recToCSV(sb *strings.Builder, r *era5.Record, _ string) {
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
