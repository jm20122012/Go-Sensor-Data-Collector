package main

import (
	"context"
	"encoding/json"
	"example/sensor-data-collection-service/databaseutils"
	"example/sensor-data-collection-service/datastructs"
	"example/sensor-data-collection-service/mqttutils"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/joho/godotenv"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

func getAvtechData(avtechUrl string) (*datastructs.AvtechResponseData, error) {
	resp, err := http.Get(avtechUrl)
	if err != nil {
		return nil, err
	} else {
		// log.Println("Avtech API call successful")
	}

	defer resp.Body.Close()

	var responseData datastructs.AvtechResponseData

	err = json.NewDecoder(resp.Body).Decode(&responseData)
	if err != nil {
		return nil, err
	}

	return &responseData, nil
}

func getWeatherStationData(apiUrl string) (datastructs.WeatherStationResponseData, error) {
	resp, err := http.Get(apiUrl)
	if err != nil {
		log.Println("Error getting Ambient Weather Station API info: ", err)
		return nil, err
	}
	// log.Println("Ambient Weather Station API call successful")

	defer resp.Body.Close()

	var responseData datastructs.WeatherStationResponseData

	err = json.NewDecoder(resp.Body).Decode(&responseData)
	if err != nil {
		return nil, err
	}

	return responseData, nil
}

func avtechWorker(wg *sync.WaitGroup) {
	defer wg.Done()
	// -------------------- Assign ENV Vars -------------------- //
	avtechUrl := os.Getenv("AVTECH_URL")

	influxClient := databaseutils.CreateInfluxDBClient()
	defer influxClient.InfluxClient.Close()

	// Use blocking write client for writes to desired bucket
	writeAPI := influxClient.InfluxClient.WriteAPIBlocking(influxClient.InfluxOrg, influxClient.InfluxBucket)

	for {
		data, err := getAvtechData(avtechUrl)
		if err != nil {
			log.Println("Error getting Avtech data: ", err)
		}

		// log.Println("All Data: ", data)
		// log.Println("First Index: ", data.Sensor[0])
		// log.Println("Second Index: ", data.Sensor[1])
		// log.Println("First Index Temp F: ", data.Sensor[0].TempF)

		tempf, _ := strconv.ParseFloat(data.Sensor[0].TempF, 8)
		tempc, _ := strconv.ParseFloat(data.Sensor[0].TempC, 8)

		p := influxdb2.NewPointWithMeasurement("temp_sensor_data").
			AddTag("sensor_location", "basement_rack").
			AddField("temperature_f", tempf).
			AddField("temperature_c", tempc).
			SetTime(time.Now())

		err = writeAPI.WritePoint(context.Background(), p)
		if err != nil {
			panic(err)
		}

		time.Sleep(1 * time.Minute)
	}
}

func ambientWeatherStationWorker(wg *sync.WaitGroup) {
	defer wg.Done()
	// -------------------- Assign ENV Vars -------------------- //
	ambientUrl := os.Getenv("AMBIENT_FULL_URL")

	influxClient := databaseutils.CreateInfluxDBClient()
	defer influxClient.InfluxClient.Close()

	// Use blocking write client for writes to desired bucket
	writeAPI := influxClient.InfluxClient.WriteAPIBlocking(influxClient.InfluxOrg, influxClient.InfluxBucket)

	for {
		data, err := getWeatherStationData(ambientUrl)
		if err != nil {
			log.Println("Error getting data from Ambient Weather Station: ", err)
		}

		// log.Println("Ambient Weather Data: ", data)
		// log.Printf("Temp F Val: %v - Type: %T", data[0].LastData.OutsideTempF, data[0].LastData.OutsideTempF)
		p := influxdb2.NewPointWithMeasurement("weather_sensor_data").
			AddTag("sensor_location", "outside_weather_station").
			AddField("outside_temperature_f", data[0].LastData.OutsideTempF).
			AddField("inside_temperature_f", data[0].LastData.InsideTempF).
			AddField("barometric_pressure_absolute", data[0].LastData.BarometricPressureAbsIn).
			AddField("barometric_pressure_relative", data[0].LastData.BarometricPressureRelIn).
			AddField("humidity", float64(data[0].LastData.OutsideHumidity)).
			AddField("wind_direction", float64(data[0].LastData.WindDirection)).
			AddField("wind_speed_mph", data[0].LastData.WindSpeedMPH).
			AddField("wind_gust_mph", data[0].LastData.WindGustMPH).
			AddField("max_daily_gust", data[0].LastData.MaxDailyGust).
			AddField("hourly_rain_inches", data[0].LastData.HourlyRainIn).
			AddField("event_rain_inches", data[0].LastData.EventRainIn).
			AddField("daily_rain_inches", data[0].LastData.DailyRainIn).
			AddField("weekly_rain_inches", data[0].LastData.WeeklyRainIn).
			AddField("total_rain_inches", data[0].LastData.TotalRainIn).
			AddField("solar_radiation", data[0].LastData.SolarRadiation).
			AddField("uv_index", data[0].LastData.UVIndex).
			AddField("feels_like_outside", data[0].LastData.FeelsLikeOutside).
			AddField("feels_like_inside", data[0].LastData.FeelsLikeInside).
			AddField("dew_point_outside", data[0].LastData.DewPointOutside).
			AddField("dew_point_inside", data[0].LastData.DewPointInside).
			AddField("outside_battery_status", data[0].LastData.OutsideBattStatus).
			AddField("co2_battery_status", data[0].LastData.BattCO2).
			SetTime(time.Now())

		// Write data to DB
		err = writeAPI.WritePoint(context.Background(), p)
		if err != nil {
			panic(err)
		}

		time.Sleep(1 * time.Minute)
	}
}

func main() {
	// Load environment variables
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	mqttClient := mqttutils.CreateMqttClient()
	mqttutils.MqttSubscribe(mqttClient)

	var wg sync.WaitGroup
	wg.Add(2)

	go avtechWorker(&wg)
	go ambientWeatherStationWorker(&wg)

	wg.Wait()

}
