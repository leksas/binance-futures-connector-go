package main

import (
	"context"
	"fmt"

	binance_futures_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	Ping()
}

func Ping() {
	baseURL := "https://fapi.binance.com"

	client := binance_futures_connector.NewClient("", "", baseURL)

	// ExchangeInfo
	err := client.NewPingService().Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Print("Ping ... ok")
}
