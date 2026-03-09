package main

import (
	"context"
	"fmt"
	"log"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	AccountPositionExample()
}

func AccountPositionExample() {
	client := bf.NewWebsocketAPIClient("api_key", "secret_key", "wss://ws-fapi.binance.com/ws-fapi/v1")
	err := client.Connect()
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}
	defer client.Close()

	response, err := client.NewPlaceNewOrderService().Symbol("BTCUSDT").Side("BUY").OrderType("MARKET").Quantity(0.01).
		Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Println(bf.PrettyPrint(response))

	client.WaitForCloseSignal()
}
