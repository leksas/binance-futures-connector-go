package main

import (
	"context"
	"log"
	"time"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	CancelOrder()

}

func CancelOrder() {
	client, err := bf.NewEdWebsocketAPIClient(bf.API_KEY, bf.PRIVATE_KEY)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	if err := client.Connect(); err != nil {
		log.Printf("Error: %v", err)
		return
	}

	var (
		symbol        = bf.SYMBOL
		orderID int64 = 123123123
	)
	for {
		rsp, err := client.NewQueryOrderService().
			Symbol(symbol).
			OrderId(orderID).
			Do(context.Background())
		if err != nil {
			log.Printf("Error: %v", err)
		}
		time.Sleep(time.Millisecond * 10)

		log.Printf("Response: %v", rsp)
	}
}
