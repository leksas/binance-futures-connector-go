package main

import (
	"context"
	"fmt"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	TickerPrice()
}

func TickerPrice() {
	baseURL := "https://fapi.binance.com"

	client := bf.NewClient("", "", baseURL)

	// Klines
	klines, err := client.NewTickerPriceService().Symbol("BTCUSDT").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(bf.PrettyPrint(klines))
}
