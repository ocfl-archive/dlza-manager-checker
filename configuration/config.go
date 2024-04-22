package configuration

import (
	"emperror.dev/errors"
	"encoding/json"
	"github.com/je4/filesystem/v2/pkg/vfsrw"
	"log"
	"maps"

	"github.com/jinzhu/configor"
)

type Service struct {
	ServiceName string `yaml:"service_name" toml:"ServiceName"`
	Host        string `yaml:"host" toml:"Host"`
	Port        int    `yaml:"port" toml:"Port"`
}

type Logging struct {
	LogLevel string
	LogFile  string
}

type Config struct {
	Handler Service `yaml:"handler" toml:"Handler"`
	Checker Service `yaml:"checker" toml:"Checker"`
	Logging Logging `yaml:"logging" toml:"Logging"`
}

type Connection struct {
	Folder string
	VFS    vfsrw.Config
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

func LoadVfsConfig(connectionString string) (vfsrw.Config, error) {
	vfsMap := make(map[string]*vfsrw.VFS)
	connection := Connection{}
	err := json.Unmarshal([]byte(connectionString), &connection)
	if err != nil {
		return nil, errors.Wrapf(err, "error mapping json for storage location connection field")
	}
	maps.Copy(vfsMap, connection.VFS)
	return vfsMap, nil
}
