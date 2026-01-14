package main

import (
	"context"
	"fmt"
	"log"

	binance_connector "github.com/leksas/binance-futures-connector-go"
)

var (
	APIKey     = ""
	PrivateKey = ""
	BrokerID   = ""
)

func main() {
	IfNewUserExample()
}

func IfNewUserExample() {
	client, _ := binance_connector.NewEdClient(APIKey, PrivateKey, "https://fapi.binance.com")

	response, err := client.NewIfNewUserService().BrokerID(BrokerID).Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Println(binance_connector.PrettyPrint(response))
}
