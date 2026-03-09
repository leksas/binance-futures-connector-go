package main

import (
	"context"
	"fmt"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	OrderBook()
}

func OrderBook() {
	baseURL := "https://fapi.binance.com"

	client := bf.NewClient("", "", baseURL)

	// OrderBook
	orderBook, err := client.NewOrderBookService().
		Symbol("BTCUSDT").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(bf.PrettyPrint(orderBook))
}
