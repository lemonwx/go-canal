/**
 *  author: lim
 *  data  : 18-7-25 下午9:36
 */

package config

import (
	"io/ioutil"

	"github.com/go-yaml/yaml"
	"github.com/juju/errors"
)

type bind struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type master struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type sync struct {
	SyncFlag  bool `yaml:"sync"`
	SyncTime  int  `yaml:"synctime"`
	SyncCount int  `yaml:"syncount"`
}

type Config struct {
	Bind   bind   `yaml:"bind"`
	Master master `yaml:"master"`
	Sync   sync   `yaml:"sync"`
}

func ReadConfig() (*Config, error) {
	cfg := &Config{}

	cfgEncode, err := ioutil.ReadFile("t.yaml")
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = yaml.Unmarshal(cfgEncode, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cfg, nil
}
