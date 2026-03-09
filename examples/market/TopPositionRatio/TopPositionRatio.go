package main

import (
	"context"
	"fmt"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	TopPositionRatio()
}

func TopPositionRatio() {
	baseURL := "https://fapi.binance.com"

	client := bf.NewClient("", "", baseURL)

	res, err := client.NewPositionRatioService().
		Symbol("BTCUSDT").Period("5m").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(bf.PrettyPrint(res))
}
