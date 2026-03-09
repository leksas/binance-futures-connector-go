package main

import (
	"context"
	"fmt"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	Ping()
}

func Ping() {
	baseURL := "https://fapi.binance.com"

	client := bf.NewClient("", "", baseURL)

	// ExchangeInfo
	err := client.NewPingService().Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Print("Ping ... ok")
}
