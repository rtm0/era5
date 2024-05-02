package era5

import (
	"github.com/batchatco/go-native-netcdf/netcdf"
	"github.com/batchatco/go-native-netcdf/netcdf/api"
)

// TZ=UTC date --date="1900-01-01 00:00:00" +%s
const unixSecs1900 = -2208988800

// Scanner retrieves metric value from a file one timestamp at a time.
type Scanner struct {
	nc   api.Group
	la   []float32
	lo   []float32
	ts   []int64
	u10  api.VarGetter
	v10  api.VarGetter
	t2m  api.VarGetter
	sf   api.VarGetter
	tcc  api.VarGetter
	tp   api.VarGetter
	pos  int
	recs []Record
	err  error
}

// NewScanner creates a new ERA5 file scanner.
func NewScanner(filePath string) (*Scanner, error) {
	nc, err := netcdf.Open(filePath)
	if err != nil {
		return nil, err
	}
	s := &Scanner{nc: nc}
	s.la, err = dimValues[float32](nc, "latitude")
	if err != nil {
		return nil, err
	}
	s.lo, err = dimValues[float32](nc, "longitude")
	if err != nil {
		return nil, err
	}
	hours, err := dimValues[int32](nc, "time")
	if err != nil {
		return nil, err
	}
	s.ts = make([]int64, len(hours))
	for i, h := range hours {
		s.ts[i] = (int64(h)*3600 + unixSecs1900) * 1000
	}
	s.u10, err = nc.GetVarGetter("u10")
	if err != nil {
		return nil, err
	}
	s.v10, err = nc.GetVarGetter("v10")
	if err != nil {
		return nil, err
	}
	s.t2m, err = nc.GetVarGetter("t2m")
	if err != nil {
		return nil, err
	}
	s.sf, err = nc.GetVarGetter("sf")
	if err != nil {
		return nil, err
	}
	s.tcc, err = nc.GetVarGetter("tcc")
	if err != nil {
		return nil, err
	}
	s.tp, err = nc.GetVarGetter("tp")
	if err != nil {
		return nil, err
	}
	return s, nil
}

func dimValues[T int32 | float32](nc api.Group, dimName string) ([]T, error) {
	dim, err := nc.GetVarGetter(dimName)
	if err != nil {
		return nil, err
	}
	v, err := dim.Values()
	if err != nil {
		return nil, err
	}
	return v.([]T), nil
}

// Close closes the scanner.
func (s *Scanner) Close() {
	s.nc.Close()
}

// Summary returns the summary information about the dataset suitable for
// logging.
func (s *Scanner) Summary() []any {
	return []any{
		"dims", []string{"ts", "lo", "la"},
		"metrics", []string{"u10", "v10", "t2m", "sf", "tcc", "tp"},
		"tsCnt", len(s.ts),
		"laCnt", len(s.la),
		"loCnt", len(s.lo),
		"totalRecCnt", s.TotalRecCount(),
	}
}

// TotalRecCount returns the total number of records within the dataset.
func (s *Scanner) TotalRecCount() int {
	// 6 is the number of metrics.
	return len(s.ts) * len(s.la) * len(s.lo) * 6
}

// Scan reads all records for the next timescamp.
func (s *Scanner) Scan() bool {
	if s.pos >= len(s.ts) {
		return false
	}

	u10, ok := s.scan(s.u10)
	if !ok {
		return false
	}
	v10, ok := s.scan(s.v10)
	if !ok {
		return false
	}
	t2m, ok := s.scan(s.t2m)
	if !ok {
		return false
	}
	sf, ok := s.scan(s.sf)
	if !ok {
		return false
	}
	tcc, ok := s.scan(s.tcc)
	if !ok {
		return false
	}
	tp, ok := s.scan(s.tp)
	if !ok {
		return false
	}

	s.recs = make([]Record, len(s.la)*len(s.lo))
	k := 0
	for i, la := range s.la {
		for j, lo := range s.lo {
			s.recs[k].Timestamp = s.ts[s.pos]
			s.recs[k].Latitude = la
			s.recs[k].Longitude = lo
			s.recs[k].ZonalWind10M = u10[i][j]
			s.recs[k].MeridionalWind10M = v10[i][j]
			s.recs[k].Temperature2M = t2m[i][j]
			s.recs[k].Snowfall = sf[i][j]
			s.recs[k].TotalCloudCover = tcc[i][j]
			s.recs[k].TotalPrecipitation = tp[i][j]
			k++
		}
	}
	s.pos++
	return true
}

func (s *Scanner) scan(vg api.VarGetter) ([][]int16, bool) {
	begin := int64(s.pos)
	limit := begin + 1
	v, err := vg.GetSlice(begin, limit)
	if err != nil {
		s.err = err
		return nil, false
	}
	return v.([][][]int16)[0], true
}

// Records returns the records that have been read by the last Scan() operation.
// The function transfers ownership of records to the caller and the subsequent
// calls to this function without prior invocation of Scan() will return nil.
func (s *Scanner) Records() []Record {
	recs := s.recs
	s.recs = nil
	return recs
}
