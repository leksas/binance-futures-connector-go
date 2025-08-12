package main

import (
	"context"
	"fmt"

	binance_futures_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	PremiumIndex()
}

func PremiumIndex() {
	baseURL := "https://fapi.binance.com"

	client := binance_futures_connector.NewClient("", "", baseURL)

	// Klines
	klines, err := client.NewPremiumIndexService().
		Symbol("BTCUSDT").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(binance_futures_connector.PrettyPrint(klines))
}
