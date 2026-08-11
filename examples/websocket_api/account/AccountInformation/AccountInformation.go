package main

import (
	"context"
	"fmt"
	"log"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	AccountInformationExample()
}

func AccountInformationExample() {
	client, err := bf.NewEdWebsocketAPIClient(bf.API_KEY, bf.PRIVATE_KEY)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	client.SetBindIP("1.2.1.3")
	err = client.Connect()
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}
	defer client.Close()

	response, err := client.NewAccountInformationService().Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Println(bf.PrettyPrint(response))

	client.WaitForCloseSignal()
}
