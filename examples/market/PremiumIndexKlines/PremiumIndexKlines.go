package main

import (
	"context"
	"fmt"

	binance_futures_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	PremiumIndexKlines()
}

func PremiumIndexKlines() {
	baseURL := "https://fapi.binance.com"

	client := binance_futures_connector.NewClient("", "", baseURL)

	// Klines
	klines, err := client.NewPremiumIndexKlinesService().
		Symbol("BTCUSDT").Interval("1m").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(binance_futures_connector.PrettyPrint(klines))
}
