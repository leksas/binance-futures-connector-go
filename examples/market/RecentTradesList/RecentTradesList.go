package main

import (
	"context"
	"fmt"

	binance_futures_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	RecentTrades()
}

func RecentTrades() {
	apiKey := "your api key"
	baseURL := "https://fapi.binance.com"

	client := binance_futures_connector.NewClient(apiKey, "", baseURL)

	recentTrades, err := client.NewRecentTradesListService().
		Symbol("BTCUSDT").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(binance_futures_connector.PrettyPrint(recentTrades))
}
