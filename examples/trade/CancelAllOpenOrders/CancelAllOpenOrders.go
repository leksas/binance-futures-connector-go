package main

import (
	"context"
	"fmt"
	"log"

	binance_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	CancelAllOpenOrders()
}

func CancelAllOpenOrders() {
	client := binance_connector.NewClient("api_key", "secret_key", "https://fapi.binance.com")

	response, err := client.NewCancelAllOpenOrdersService().Symbol("BTCUSDT").Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Println(binance_connector.PrettyPrint(response))
}
