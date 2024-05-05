package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/rtm0/era5/internal/era5"
	"github.com/rtm0/era5/internal/vm"
)

var (
	file          = flag.String("file", "", "path to an ERA5 file in NetCDF format")
	concurrency   = flag.Int("concurrency", runtime.NumCPU(), "number of concurrent requests to Victoria Metrics")
	recsPerInsert = flag.Int("recsPerInsert", 500, "number of records sent to VM in one batch")
	vmInsertURL   = flag.String("vmInsertUrl", "http://localhost:8428/write", "Victoria Metrics insert API URL. Default: InfluxDB line protocol v2")
	metricPrefix  = flag.String("metricPrefix", "era5", "a prefix that will be added to the metric names (cannot be empty)")
	limitHours    = flag.Int("limitHours", 0, "export only this many hours of data. Default: 0 (no limit)")
)

func main() {
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	vmCli, err := vm.NewClient(logger, *vmInsertURL, *concurrency, *metricPrefix)
	if err != nil {
		logger.Error("Could not create new VM client", "err", err)
		os.Exit(1)
	}

	s, err := era5.NewScanner(*file, *limitHours)
	if err != nil {
		logger.Error("Could not create an ERA5 scanner", "err", err)
		os.Exit(1)
	}
	defer s.Close()
	logger.Info("ERA5 summary", s.Summary()...)
	extracted := make(chan []era5.Record)
	go func() {
		for s.Scan() {
			extracted <- s.Records()
		}
		if s.Error() != nil {
			logger.Error("could not read ERA5 records", "err", s.Error())
		}
		close(extracted)
	}()

	loaded := make(chan int)
	var loaders sync.WaitGroup
	for _ = range *concurrency {
		loaders.Add(1)
		go func() {
			for recs := range extracted {
				n := len(recs)
				for i := 0; i < n; i += *recsPerInsert {
					begin := i
					limit := begin + *recsPerInsert
					if limit > n {
						limit = n
					}
					vmCli.Insert(recs[begin:limit])
				}
				loaded <- n
			}
			loaders.Done()
		}()
	}
	done := make(chan bool)
	go func() {
		var inserted, total float64
		total = float64(s.TotalRecCount())
		start := time.Now()
		for n := range loaded {
			inserted += float64(n)
			percent := fmt.Sprintf("%.2f%%", 100*inserted/total)
			duration := time.Since(start).Round(1 * time.Second)
			logger.Info("inserted", "rows", percent, "in", duration)
		}
		done <- true
	}()

	loaders.Wait()
	close(loaded)
	<-done
	close(done)
}
