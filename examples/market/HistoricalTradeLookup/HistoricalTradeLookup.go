package main

import (
	"context"
	"fmt"

	binance_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	HistoricalTradeLookup()
}

func HistoricalTradeLookup() {
	apiKey := "your api key"
	baseURL := "https://fapi.binance.com"

	client := binance_connector.NewClient(apiKey, "", baseURL)

	historicalTradeLookup, err := client.NewHistoricalTradeLookupService().
		Symbol("BTCUSDT").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(binance_connector.PrettyPrint(historicalTradeLookup))
}
