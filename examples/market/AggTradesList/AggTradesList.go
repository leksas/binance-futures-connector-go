package main

import (
	"context"
	"fmt"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	AggTradesList()
}

func AggTradesList() {
	baseURL := "https://fapi.binance.com"

	client := bf.NewClient("", "", baseURL)

	// AggTradesList
	aggTradesList, err := client.NewAggTradesListService().
		Symbol("BTCUSDT").Limit(20).Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(bf.PrettyPrint(aggTradesList))
}
