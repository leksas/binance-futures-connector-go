package main

import (
	"context"
	"fmt"
	"log"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	CancelAllOpenOrders()
}

func CancelAllOpenOrders() {
	client := bf.NewClient("api_key", "secret_key", "https://fapi.binance.com")

	response, err := client.NewCancelAllOpenOrdersService().Symbol("BTCUSDT").Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Println(bf.PrettyPrint(response))
}
