package main

import (
	"context"
	"fmt"
	"log"

	binance_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	ModifyOrderExample()
}

func ModifyOrderExample() {
	client := binance_connector.NewWebsocketAPIClient("api_key", "secret_key", "wss://ws-fapi.binance.com/ws-fapi/v1")
	err := client.Connect()
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}
	defer client.Close()

	orderId := int64(746089296278)
	response, err := client.NewModifyOrderService().OrderId(orderId).Symbol("BTCUSDT").Side(binance_connector.Buy).
		Price(115000).Quantity(0.001).Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Println(binance_connector.PrettyPrint(response))

	client.WaitForCloseSignal()
}
