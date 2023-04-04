package mqttutils

import (
	"context"
	"encoding/json"
	"example/sensor-data-collection-service/databaseutils"
	"example/sensor-data-collection-service/datastructs"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	log.Printf("Received message \" %s \" from topic: %s\n", msg.Payload(), msg.Topic())

	var sensorData datastructs.RpiSensorData

	// Convert MQTT json string to struct
	err := json.Unmarshal(msg.Payload(), &sensorData)
	if err != nil {
		log.Println("Error unmarshalling MQTT message: ", err)
	}

	log.Println("Sensor data: ", sensorData)

	// Setup InfluxDB client for writing data
	influxClient := databaseutils.CreateInfluxDBClient()
	defer influxClient.InfluxClient.Close()

	// Use blocking write client for writes to desired bucket
	writeAPI := influxClient.InfluxClient.WriteAPIBlocking(influxClient.InfluxOrg, influxClient.InfluxBucket)

	p := influxdb2.NewPointWithMeasurement("temp_sensor_data").
		AddTag("sensor_location", sensorData.SensorLocation).
		AddField("temperature_f", sensorData.Temp_F).
		AddField("temperature_c", sensorData.Temp_C).
		AddField("humidity", sensorData.Humidity).
		SetTime(time.Now())

	err = writeAPI.WritePoint(context.Background(), p)
	if err != nil {
		panic(err)
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	log.Println("Connected")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	log.Printf("Connect lost: %v", err)
}

func Publish(client mqtt.Client) {
	token := client.Publish("test", 0, false, time.Now().String())
	token.Wait()
}

func MqttSubscribe(client mqtt.Client) {
	topic := os.Getenv("MQTT_SUB_TOPIC")
	token := client.Subscribe(topic, 1, nil)
	token.Wait()
	log.Println("MQTT Subscribed to topic: ", topic)
}

func CreateMqttClient() mqtt.Client {
	mqttBroker := os.Getenv("MQTT_BROKER_IP")
	mqttPort, err := strconv.Atoi(os.Getenv("MQTT_BROKER_PORT"))
	if err != nil {
		log.Println("Error converting MQTT port to INT")
	}

	// // -------------------- MQTT Setup -------------------- //
	mqttListenerOpts := mqtt.NewClientOptions()
	mqttListenerOpts.AddBroker(fmt.Sprintf("mqtt://%s:%d", mqttBroker, mqttPort))
	mqttListenerOpts.SetClientID("goSensorDataCollector")
	mqttListenerOpts.SetOrderMatters(false)
	mqttListenerOpts.SetDefaultPublishHandler(messagePubHandler)
	mqttListenerOpts.OnConnect = connectHandler
	mqttListenerOpts.OnConnectionLost = connectLostHandler

	newClient := mqtt.NewClient(mqttListenerOpts)
	if token := newClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	return newClient
}
