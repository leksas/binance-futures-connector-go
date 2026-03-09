package main

import (
	"context"
	"fmt"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	OpenInterest()
}

func OpenInterest() {
	baseURL := "https://fapi.binance.com"

	client := bf.NewClient("", "", baseURL)

	//
	res, err := client.NewOpenInterestService().Symbol("BTCUSDT").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(bf.PrettyPrint(res))
}
