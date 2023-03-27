package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/joho/godotenv"
)

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
		DateUTC        int     `json:"dateutc"`
		TempInf        float64 `json:"tempinf"`
		HumidityIn     int     `json:"humidityin"`
		BaromRelIn     float64 `json:"baromrelin"`
		BaromAbsIn     float64 `json:"baromabsin"`
		TempF          float64 `json:"tempf"`
		BattOut        int     `json:"battout"`
		Humidity       int     `json:"humidity"`
		WindDir        int     `json:"winddir"`
		WindSpeedMPH   float64 `json:"windspeedmph"`
		WindGustMPH    float64 `json:"windgustmph"`
		MaxDailyGust   float64 `json:"maxdailygust"`
		HourlyRainIn   float64 `json:"hourlyrainin"`
		EventRainIn    float64 `json:"eventrainin"`
		DailyRainIn    float64 `json:"dailyrainin"`
		WeeklyRainIn   float64 `json:"weeklyrainin"`
		MonthlyRainIn  float64 `json:"monthlyrainin"`
		TotalRainIn    float64 `json:"totalrainin"`
		SolarRadiation float64 `json:"solarradiation"`
		UV             float64 `json:"uv"`
		BattCO2        int     `json:"batt_co2"`
		FeelsLike      float64 `json:"feelsLike"`
		DewPoint       float64 `json:"dewPoint"`
		FeelsLikeIn    float64 `json:"feelsLikein"`
		DewPointIn     float64 `json:"dewPointin"`
		LastRain       string  `json:"lastRain"`
		TZ             string  `json:"tz"`
		Date           string  `json:"date"`
	} `json:"lastData"`
	Info struct {
		Name string `json:"name"`
	} `json:"info"`
}

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	log.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	log.Println("Connected")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	log.Printf("Connect lost: %v", err)
}

func publish(client mqtt.Client) {
	token := client.Publish("test", 0, false, time.Now().String())
	token.Wait()
}

func mqttSubscribe(client mqtt.Client) {
	topic := os.Getenv("MQTT_SUB_TOPIC")
	token := client.Subscribe(topic, 1, nil)
	token.Wait()
	log.Println("MQTT Subscribed to topic: ", topic)
}

func getAvtechData(avtechUrl string) (*AvtechResponseData, error) {
	resp, err := http.Get(avtechUrl)
	if err != nil {
		return nil, err
	} else {
		log.Println("Avtech API call successful")
	}

	defer resp.Body.Close()

	var responseData AvtechResponseData

	err = json.NewDecoder(resp.Body).Decode(&responseData)
	if err != nil {
		return nil, err
	}

	return &responseData, nil
}

func getWeatherStationData(apiUrl string) (*WeatherStationResponseData, error) {
	resp, err := http.Get(apiUrl)
	if err != nil {
		log.Println("Error getting Ambient Weather Station API info: ", err)
		return nil, err
	}
	log.Println("Ambient Weather Station API call successful")

	defer resp.Body.Close()

	var responseData WeatherStationResponseData

	err = json.NewDecoder(resp.Body).Decode(&responseData)
	if err != nil {
		return nil, err
	}

	return &responseData, nil
}

func avtechWorker(wg *sync.WaitGroup) {
	defer wg.Done()
	// -------------------- Assign ENV Vars -------------------- //
	avtechUrl := os.Getenv("AVTECH_URL")
	influxUrl := os.Getenv("INFLUXDB_URL")
	influxToken := os.Getenv("INFLUXDB_API_TOKEN")
	influxBucket := os.Getenv("INFLUXDB_BUCKET")
	influxOrg := os.Getenv("INFLUXDB_ORG")

	// -------------------- InfluxDB Setup -------------------- //
	// Create a new client using an InfluxDB server base URL and an authentication token

	// This is the default client instantiation.  However, due to server using
	// a self-signed TLS certificate without using SANs, this throws an error
	// client := influxdb2.NewClient(influxUrl, influxToken)

	// To skip certificate verification, create the client like this:

	// Create a new TLS configuration with certificate verification disabled
	// This is not recommended though.  Only use for testing until a new
	// TLS certificate can be created with SANs
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	influxClient := influxdb2.NewClientWithOptions(influxUrl, influxToken, influxdb2.DefaultOptions().SetTLSConfig(tlsConfig))

	// Use blocking write client for writes to desired bucket
	writeAPI := influxClient.WriteAPIBlocking(influxOrg, influxBucket)

	for true {
		data, err := getAvtechData(avtechUrl)
		if err != nil {
			log.Println("Error getting Avtech data: ", err)
		}

		log.Println("All Data: ", data)
		log.Println("First Index: ", data.Sensor[0])
		log.Println("Second Index: ", data.Sensor[1])
		log.Println("First Index Temp F: ", data.Sensor[0].TempF)

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
	// influxUrl := os.Getenv("INFLUXDB_URL")
	// influxToken := os.Getenv("INFLUXDB_API_TOKEN")
	// influxBucket := os.Getenv("INFLUXDB_BUCKET")
	// influxOrg := os.Getenv("INFLUXDB_ORG")

	// // -------------------- InfluxDB Setup -------------------- //
	// // Create a new client using an InfluxDB server base URL and an authentication token

	// // This is the default client instantiation.  However, due to server using
	// // a self-signed TLS certificate without using SANs, this throws an error
	// // client := influxdb2.NewClient(influxUrl, influxToken)

	// // To skip certificate verification, create the client like this:

	// // Create a new TLS configuration with certificate verification disabled
	// // This is not recommended though.  Only use for testing until a new
	// // TLS certificate can be created with SANs
	// tlsConfig := &tls.Config{InsecureSkipVerify: true}
	// influxClient := influxdb2.NewClientWithOptions(influxUrl, influxToken, influxdb2.DefaultOptions().SetTLSConfig(tlsConfig))

	// // Use blocking write client for writes to desired bucket
	// writeAPI := influxClient.WriteAPIBlocking(influxOrg, influxBucket)

	data, err := getWeatherStationData(ambientUrl)
	if err != nil {
		log.Println("Error getting data from Ambient Weather Station: ", err)
	}

	log.Println("Ambient Weather Data: ", data)

}

func main() {
	// Load environment variables
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// mqttBroker := os.Getenv("MQTT_BROKER_IP")
	// mqttPort, err := strconv.Atoi(os.Getenv("MQTT_BROKER_PORT"))
	// if err != nil {
	// 	log.Println("Error converting MQTT port to INT")
	// }

	// // -------------------- MQTT Setup -------------------- //
	// mqttListenerOpts := mqtt.NewClientOptions()
	// mqttListenerOpts.AddBroker(fmt.Sprintf("mqtt://%s:%d", mqttBroker, mqttPort))
	// mqttListenerOpts.SetClientID("goSensorDataCollector")
	// mqttListenerOpts.SetOrderMatters(false)
	// mqttListenerOpts.SetDefaultPublishHandler(messagePubHandler)
	// mqttListenerOpts.OnConnect = connectHandler
	// mqttListenerOpts.OnConnectionLost = connectLostHandler

	// client := mqtt.NewClient(mqttListenerOpts)
	// if token := client.Connect(); token.Wait() && token.Error() != nil {
	// 	panic(token.Error())
	// }

	// mqttSubscribe(client)

	// go avtechWorker(&wg)
	go ambientWeatherStationWorker((&wg))

	wg.Wait()

}
