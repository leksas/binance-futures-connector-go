package main

import (
	"context"
	"fmt"
	"log"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	PlaceNewOrderExample()
}

func PlaceNewOrderExample() {
	client, _ := bf.NewEdWebsocketAPIClient("", "")
	err := client.Connect()
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}
	defer client.Close()

	response, err := client.NewPlaceNewOrderService().
		Symbol("LTCUSDT").
		Side(bf.Buy).
		OrderType(bf.Limit).
		Price(50).
		Quantity(1).
		TimeInForce(bf.GTC).
		Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Println(bf.PrettyPrint(response))

	client.WaitForCloseSignal()
}
