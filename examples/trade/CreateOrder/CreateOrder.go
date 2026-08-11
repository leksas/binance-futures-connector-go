package main

import (
	"context"
	"fmt"
	"log"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	CreateOrder()
}

func CreateOrder() {
	client, err := bf.NewEdClient(bf.API_KEY, bf.PRIVATE_KEY)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	var (
		symbol          = "PLAYUSDT"
		side            = bf.Buy
		posSide         = bf.Long
		ordType         = bf.Market
		qty     float64 = 60
	)
	response, err := client.NewCreateOrderService().
		Symbol(symbol).
		Side(side).
		PositionSide(posSide).
		OrderType(ordType).
		Quantity(qty).
		Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Println(bf.PrettyPrint(response))
}
