package era5

// Record is a collection of readings taken at a given geo location at a given
// time.
type Record struct {
	// Dimensions
	Timestamp int64
	Latitude  float32
	Longitude float32

	// Metrics
	ZonalWind10M       int16 // u
	MeridionalWind10M  int16 // v
	Temperature2M      int16
	Snowfall           int16
	TotalCloudCover    int16
	TotalPrecipitation int16
}
