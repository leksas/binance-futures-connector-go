package main

import (
	"context"
	"fmt"
	"log"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	SetPositionSide()
}

func SetPositionSide() {
	client, err := bf.NewEdClient(bf.API_KEY, bf.PRIVATE_KEY)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	response, err := client.NewSetPositionSideDualService().
		DualSidePosition(false).
		Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Println(bf.PrettyPrint(response))
}
