package configuration

import (
	"log"

	"github.com/jinzhu/configor"
)

type Service struct {
	ServiceName string `yaml:"service_name" toml:"ServiceName"`
	Host        string `yaml:"host" toml:"Host"`
	Port        int    `yaml:"port" toml:"Port"`
}

type Config struct {
	Handler Service `yaml:"handler" toml:"Handler"`
	Checker Service `yaml:"checker" toml:"Checker"`
}

// GetConfig creates a new config from a given environment
func GetConfig(configFile string) Config {
	conf := Config{}
	if configFile == "" {
		configFile = "config.yml"
	}
	err := configor.Load(&conf, configFile)
	if err != nil {
		log.Fatal(err)
	}
	return conf
}
