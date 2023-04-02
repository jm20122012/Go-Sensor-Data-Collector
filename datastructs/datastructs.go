package datastructs

type AvtechResponseData struct {
	Sensor []SensorData `json:"sensor"`
}

type SensorData struct {
	Label string `json:"label"`
	TempF string `json:"tempf"`
	TempC string `json:"tempc"`
	HighF string `json:"highf"`
	HighC string `json:"highc"`
	LowF  string `json:"lowf"`
	LowC  string `json:"lowc"`
}

type WeatherStationResponseData []struct {
	MacAddress string `json:"macAddress"`
	LastData   struct {
		DateUTC                 int     `json:"dateutc"`
		InsideTempF             float64 `json:"tempinf"`
		InsideHumidity          int     `json:"humidityin"`
		BarometricPressureRelIn float64 `json:"baromrelin"`
		BarometricPressureAbsIn float64 `json:"baromabsin"`
		OutsideTempF            float64 `json:"tempf"`
		OutsideBattStatus       int     `json:"battout"`
		OutsideHumidity         int     `json:"humidity"`
		WindDirection           int     `json:"winddir"`
		WindSpeedMPH            float64 `json:"windspeedmph"`
		WindGustMPH             float64 `json:"windgustmph"`
		MaxDailyGust            float64 `json:"maxdailygust"`
		HourlyRainIn            float64 `json:"hourlyrainin"`
		EventRainIn             float64 `json:"eventrainin"`
		DailyRainIn             float64 `json:"dailyrainin"`
		WeeklyRainIn            float64 `json:"weeklyrainin"`
		MonthlyRainIn           float64 `json:"monthlyrainin"`
		TotalRainIn             float64 `json:"totalrainin"`
		SolarRadiation          float64 `json:"solarradiation"`
		UVIndex                 float64 `json:"uv"`
		BattCO2                 int     `json:"batt_co2"`
		FeelsLikeOutside        float64 `json:"feelsLike"`
		DewPointOutside         float64 `json:"dewPoint"`
		FeelsLikeInside         float64 `json:"feelsLikein"`
		DewPointInside          float64 `json:"dewPointin"`
		LastRain                string  `json:"lastRain"`
		TZ                      string  `json:"tz"`
		Date                    string  `json:"date"`
	} `json:"lastData"`
	Info struct {
		Name string `json:"name"`
	} `json:"info"`
}
