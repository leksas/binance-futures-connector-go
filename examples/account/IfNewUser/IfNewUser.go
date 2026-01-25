package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	binance_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	IfNewUserExample()
}

func IfNewUserExample() {
	var (
		apiKey     = flag.String("apiKey", "", "ApiKey")
		privateKey = flag.String("privateKey", "", "PrivateKey")
		code       = flag.String("code", "", "Code")
	)

	flag.Parse()

	client, _ := binance_connector.NewEdClient(*apiKey, *privateKey, "https://fapi.binance.com")

	response, err := client.NewIfNewUserService().BrokerID(*code).Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Println(binance_connector.PrettyPrint(response))
}
