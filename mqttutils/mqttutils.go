package databaseutils

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	log.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
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

func CreateMqttClient(deviceIP string) mqtt.Client {
	err := godotenv.Load("./.env")
	if err != nil {
		log.Fatal("Error - could not load ENV file", err)
	}

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
