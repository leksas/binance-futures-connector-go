package main

import (
	"context"
	"fmt"
	"log"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	QueryOrderExample()
}

func QueryOrderExample() {
	client := bf.NewWebsocketAPIClient("api_key", "secret_key", "wss://ws-api.testnet.binance.vision/ws-api/v3")
	err := client.Connect()
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}
	defer client.Close()

	response, err := client.NewQueryOrderService().Symbol("BTCUSDT").OrderId(123123123).Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Println(bf.PrettyPrint(response))

	client.WaitForCloseSignal()
}
