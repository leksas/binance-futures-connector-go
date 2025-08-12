package main

import (
	"context"
	"fmt"

	binance_futures_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	TopPositionRatio()
}

func TopPositionRatio() {
	baseURL := "https://fapi.binance.com"

	client := binance_futures_connector.NewClient("", "", baseURL)

	res, err := client.NewPositionRatioService().
		Symbol("BTCUSDT").Period("5m").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(binance_futures_connector.PrettyPrint(res))
}
