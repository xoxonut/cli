package ipconfig

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type DB int

const (
	KAFKA DB = iota
	COUCHDB
)

type Config struct {
	Kafka_ip   string `json:"kafka_ip"`
	CouchDB_ip string `json:"couchDB_ip"`
}

func GetIPs() (Config, error) {
	//open config.json
	file, err := os.Open("config.json")
	if err != nil {
		fmt.Println("open file error:", err)
		return Config{}, err
	}
	defer file.Close()

	//read all file
	data, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("read file error:", err)
		return Config{}, err
	}

	//json deserialize
	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		fmt.Println("json unmarshal error:", err)
		return Config{}, err
	}

	return config, nil
}

func SetIP(db DB, new string) error {
	conifg, err := GetIPs()
	if err != nil {
		return err
	}
	switch db {
	case KAFKA:
		conifg.Kafka_ip = new
	case COUCHDB:
		conifg.CouchDB_ip = new
	}
	newData, err := json.Marshal(conifg)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile("config.json", newData, 0644)
	if err != nil {
		return err
	}
	return nil
}
