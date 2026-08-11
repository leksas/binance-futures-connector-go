package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	symbol := flag.String("symbol", "BTCUSDT", "Symbol")
	price := flag.Float64("price", 0.5, "Price")
	quantity := flag.Float64("quantity", 10.0, "Quantity")
	flag.Parse()

	CreateLimitOrder(*symbol, *price, *quantity)
}

func CreateLimitOrder(symbol string, price float64, quantity float64) {
	client, err := bf.NewEdClient(bf.API_KEY, bf.PRIVATE_KEY)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	response, err := client.NewCreateOrderService().
		Symbol(symbol).
		Side(bf.Buy).
		OrderType(bf.Limit).
		Price(price).
		Quantity(quantity).
		TimeInForce(bf.GTC).
		Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Println(bf.PrettyPrint(response))
}
