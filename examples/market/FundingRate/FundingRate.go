package main

import (
	"context"
	"fmt"

	binance_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	FundingRate()
}

func FundingRate() {
	baseURL := "https://fapi.binance.com"

	client := binance_connector.NewClient("", "", baseURL)

	// Klines
	klines, err := client.NewFundingRateService().
		Symbol("BTCUSDT").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(binance_connector.PrettyPrint(klines))
}
