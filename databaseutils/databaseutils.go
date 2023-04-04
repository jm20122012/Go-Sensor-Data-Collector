package databaseutils

import (
	"crypto/tls"
	"os"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

type InfluxClient struct {
	InfluxClient influxdb2.Client
	InfluxURL    string
	InfluxToken  string
	InfluxBucket string
	InfluxOrg    string
}

func CreateInfluxDBClient() *InfluxClient {
	var newClient InfluxClient

	newClient.InfluxURL = os.Getenv("INFLUXDB_URL")
	newClient.InfluxToken = os.Getenv("INFLUXDB_API_TOKEN")
	newClient.InfluxBucket = os.Getenv("INFLUXDB_BUCKET")
	newClient.InfluxOrg = os.Getenv("INFLUXDB_ORG")

	// // -------------------- InfluxDB Setup -------------------- //
	// // Create a new client using an InfluxDB server base URL and an authentication token

	// // This is the default client instantiation.  However, due to server using
	// // a self-signed TLS certificate without using SANs, this throws an error
	// // client := influxdb2.NewClient(influxUrl, influxToken)

	// // To skip certificate verification, create the client like this:

	// // Create a new TLS configuration with certificate verification disabled
	// // This is not recommended though.  Only use for testing until a new
	// // TLS certificate can be created with SANs
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	influxClient := influxdb2.NewClientWithOptions(newClient.InfluxURL, newClient.InfluxToken, influxdb2.DefaultOptions().SetTLSConfig(tlsConfig))

	newClient.InfluxClient = influxClient

	return &newClient
}
