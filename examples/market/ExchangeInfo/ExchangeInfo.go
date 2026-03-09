package main

import (
	"context"
	"fmt"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	ExchangeInfo()
}

func ExchangeInfo() {
	baseURL := "https://fapi.binance.com"

	client := bf.NewClient("", "", baseURL)

	// ExchangeInfo
	exchangeInfo, err := client.NewExchangeInfoService().Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(bf.PrettyPrint(exchangeInfo))
}
