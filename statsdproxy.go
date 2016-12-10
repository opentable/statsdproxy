package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"os"
	"time"
)

// StatsdServer represents a backend instance
type StatsdServer struct {
	IP       string
	UDPPort  string
	MgmtPort string
}

// Configuration stores the core configuration of the proxy
type Configuration struct {
	Workers       uint
	CheckInterval uint
	Servers       []StatsdServer
}

// UDPAddress returns the string representation of the statsd udp endpoint
func (s *StatsdServer) UDPAddress() string {
	return net.JoinHostPort(s.IP, s.UDPPort)
}

// MgmtAddress returns the string representation of the statsd management
// address
func (s *StatsdServer) MgmtAddress() string {
	return net.JoinHostPort(s.IP, s.MgmtPort)
}

// CheckStatsdHealth checks the health of a single instance
func (s *StatsdServer) CheckStatsdHealth() (up bool, err error) {
	addr := s.MgmtAddress()
	conn, err := net.Dial("tcp", addr)

	if err != nil {
		return false, fmt.Errorf("Failed to connect to %s: %s", addr, err)
	}

	defer conn.Close()

	count, err := conn.Write([]byte("health\n"))
	if count != 7 || err != nil {
		return false, fmt.Errorf("Failed to get health on %s: %s", addr, err)
	}

	buffer := make([]byte, 100)
	count, err = conn.Read(buffer)
	if count != 11 || err != nil {
		return false, fmt.Errorf("Unable to read health response from %s: %s", addr, err)
	}

	if bytes.Equal(buffer[0:count], []byte("health: up\n")) {
		conn.Write([]byte("quit\n"))
		return true, error(nil)
	}
	return false, fmt.Errorf("Health check to %s failed: Response %s", addr, string(buffer))
}

// CheckBackend checks each backend server in turn
func CheckBackend(servers []StatsdServer, statusChan chan<- []StatsdServer, quit <-chan bool) {
	for {
		select {
		case <-quit:
			return
		default:
			var lineServers []StatsdServer
			for _, server := range servers {
				up, err := server.CheckStatsdHealth()
				if up && err == nil {
					lineServers = append(lineServers, server)
				} else {
					log.Printf("Removing server %s: %s", server.UDPAddress(), err)
				}
			}
			statusChan <- lineServers
		}

		time.Sleep(10 * time.Second)
	}
}

// HandleMetric handles an individual metric string by hashing and passing to a
// backend
func HandleMetric(servers []StatsdServer, metric []byte) {
	h := fnv.New32a()
	metric = bytes.TrimSpace(metric)
	metricName := bytes.SplitN(metric, []byte(":"), 2)[0]
	h.Write(metricName)
	if len(servers) > 0 {
		destIndex := h.Sum32() % uint32(len(servers))
		LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
		if err != nil {
			log.Printf("Failed to resolve local udp address: %s", err)
		}

		RemoteAddr, err := net.ResolveUDPAddr("udp", servers[destIndex].UDPAddress())
		if err != nil {
			log.Printf("Failed to resolve remote address (%s): %s", servers[destIndex].UDPAddress(), err)
		}
		Conn, err := net.DialUDP("udp", LocalAddr, RemoteAddr)
		defer Conn.Close()

		if err != nil {
			log.Printf("Failed to write metric to %s: %s",
				servers[destIndex].UDPAddress(),
				err,
			)
		} else {
			_, err := Conn.Write(metric)
			if err != nil {
				log.Printf("Failed to write metric to %s: %s", servers[destIndex].UDPAddress(), err)
			}
		}
	}
}

// LoadConfig loads the config from the config file
func LoadConfig(filename string) Configuration {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to read config %s: %s", filename, err)
	}

	configuration := Configuration{}
	err = json.NewDecoder(file).Decode(&configuration)
	if err != nil {
		log.Fatalf("Failed to decode config %s: %s", filename, err)
	}
	return configuration
}

// ListenStatsD listens for stats on the default port
func ListenStatsD(port int, quit <-chan bool, metricChan chan<- []byte) {
	ServerAddr, err := net.ResolveUDPAddr("udp", ":8125")
	if err != nil {
		log.Printf("Failed to resolve listening address: %s", err)
	}

	ServerConn, err := net.ListenUDP("udp", ServerAddr)
	if err != nil {
		log.Printf("Failed to listen at listening port: %s", err)
	}

	defer ServerConn.Close()

	for {
		buf := make([]byte, 1024, 1024)
		count, _, err := ServerConn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("Failed to read from socket: %s", err)
		} else {
			metricChan <- buf[0:count]
		}
	}
}

func main() {
	var configFile = flag.String("config", "/etc/statsdproxy.json", "Config file to load")
	flag.Parse()
	config := LoadConfig(*configFile)
	lineServers := config.Servers

	statusChan := make(chan []StatsdServer)
	quitBackend := make(chan bool)
	quitListen := make(chan bool)
	metricChan := make(chan []byte)

	go CheckBackend(config.Servers, statusChan, quitBackend)
	go ListenStatsD(8125, quitListen, metricChan)

	for {
		select {
		case lineServers = <-statusChan:
			log.Printf("Got a new server list")
			if len(lineServers) == 0 {
				log.Printf("No live servers to send metrics to. Dropping packets")
			}
		case metric := <-metricChan:
			go HandleMetric(lineServers, metric)
		}
	}
}
