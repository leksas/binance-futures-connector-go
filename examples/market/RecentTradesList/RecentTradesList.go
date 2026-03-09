package main

import (
	"context"
	"fmt"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	RecentTrades()
}

func RecentTrades() {
	apiKey := "your api key"
	baseURL := "https://fapi.binance.com"

	client := bf.NewClient(apiKey, "", baseURL)

	recentTrades, err := client.NewRecentTradesListService().
		Symbol("BTCUSDT").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(bf.PrettyPrint(recentTrades))
}
