package main

import (
	"context"
	"fmt"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	DeliveryPrice()
}

func DeliveryPrice() {
	baseURL := "https://fapi.binance.com"

	client := bf.NewClient("", "", baseURL)

	// Klines
	klines, err := client.NewDeliveryPriceService().Pair("BTCUSDT").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(bf.PrettyPrint(klines))
}
