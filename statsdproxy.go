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

type StatsdServer struct {
	Ip       string
	UdpPort  string
	MgmtPort string
}

type Configuration struct {
	Workers       uint
	CheckInterval uint
	Servers       []StatsdServer
}

func (s *StatsdServer) UdpAddress() string {
	return net.JoinHostPort(s.Ip, s.UdpPort)
}

func (s *StatsdServer) MgmtAddress() string {
	return net.JoinHostPort(s.Ip, s.MgmtPort)
}

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
		return false, fmt.Errorf("Unable to read health response from %s: %s\n", addr, err)
	}

	if bytes.Equal(buffer[0:count], []byte("health: up\n")) {
		conn.Write([]byte("quit\n"))
		return true, error(nil)
	} else {
		return false, fmt.Errorf("Health check to %s failed: Response %s\n", addr, string(buffer))
	}
}

func CheckBackend(servers []StatsdServer, status_chan chan<- []StatsdServer, quit <-chan bool) {
	for {
		select {
		case <-quit:
			return
		default:
			var live_servers []StatsdServer
			for _, server := range servers {
				up, err := server.CheckStatsdHealth()
				if up && err == nil {
					live_servers = append(live_servers, server)
				} else {
					log.Printf("Removing server %s: %s", server.UdpAddress(), err)
				}
			}
			status_chan <- live_servers
		}

		time.Sleep(10 * time.Second)
	}
}

func HandleMetric(servers []StatsdServer, metric []byte) {
	h := fnv.New32a()
	metric = bytes.TrimSpace(metric)
	metric_name := bytes.SplitN(metric, []byte(":"), 2)[0]
	h.Write(metric_name)
	if len(servers) > 0 {
		dest_index := h.Sum32() % uint32(len(servers))
		LocalAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
		RemoteAddr, _ := net.ResolveUDPAddr("udp", servers[dest_index].UdpAddress())
		Conn, err := net.DialUDP("udp", LocalAddr, RemoteAddr)
		defer Conn.Close()

		if err != nil {
			log.Printf("Failed to write metric to %s: %s",
				servers[dest_index].UdpAddress(),
				err,
			)
		} else {
			Conn.Write(metric)
		}
	}
}

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

func ListenStatsD(port int, quit <-chan bool, metric_chan chan<- []byte) {
	ServerAddr, _ := net.ResolveUDPAddr("udp", ":8125")
	ServerConn, _ := net.ListenUDP("udp", ServerAddr)
	defer ServerConn.Close()

	for {
		buf := make([]byte, 1024, 1024)
		count, _, err := ServerConn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("Failed to read from socket: %s", err)
		} else {
			fmt.Println(buf[0:count])
			metric_chan <- buf[0:count]
		}
	}
}

func main() {
	var config_file = flag.String("config", "/etc/statsproxy.json", "Config file to load")
	flag.Parse()
	config := LoadConfig(*config_file)
	live_servers := config.Servers

	status_chan := make(chan []StatsdServer)
	quit_backend := make(chan bool)
	quit_listen := make(chan bool)
	metric_chan := make(chan []byte)

	go CheckBackend(config.Servers, status_chan, quit_backend)
	go ListenStatsD(8125, quit_listen, metric_chan)

	for {
		select {
		case live_servers = <-status_chan:
			log.Printf("Got a new server list")
			if len(live_servers) == 0 {
				log.Printf("No live servers to send metrics to. Dropping packets")
			}
		case metric := <-metric_chan:
			HandleMetric(live_servers, metric)
		}
	}
}
