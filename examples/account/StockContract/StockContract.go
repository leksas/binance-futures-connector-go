package main

import (
	"context"
	"fmt"
	"log"

	binance_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	StockContractExample()
}

func StockContractExample() {
	var (
		apiKey     = ""
		privateKey = ""
	)

	client, _ := binance_connector.NewEdClient(apiKey, privateKey)
	err := client.NewStockContractService().Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Println("Stock contract signed successfully")
}
