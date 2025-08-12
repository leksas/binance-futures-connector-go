package binance_futures_connector

import (
	gojson "github.com/goccy/go-json"
)

func Marshal(v interface{}) ([]byte, error) {
	return gojson.Marshal(v)
}

func Unmarshal(data []byte, v interface{}) error {
	return gojson.Unmarshal(data, v)
}

func MarshalIndent(v interface{}, prefix, indent string) ([]byte, error) {
	return gojson.MarshalIndent(v, prefix, indent)
}
