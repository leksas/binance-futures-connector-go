package main

import (
	"context"
	"fmt"

	binance_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	ExchangeInfo()
}

func ExchangeInfo() {
	baseURL := "https://fapi.binance.com"

	client := binance_connector.NewClient("", "", baseURL)

	// ExchangeInfo
	exchangeInfo, err := client.NewExchangeInfoService().Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(binance_connector.PrettyPrint(exchangeInfo))
}
