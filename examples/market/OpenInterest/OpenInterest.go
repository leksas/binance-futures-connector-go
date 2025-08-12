package main

import (
	"context"
	"fmt"

	binance_futures_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	OpenInterest()
}

func OpenInterest() {
	baseURL := "https://fapi.binance.com"

	client := binance_futures_connector.NewClient("", "", baseURL)

	//
	res, err := client.NewOpenInterestService().Symbol("BTCUSDT").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(binance_futures_connector.PrettyPrint(res))
}
