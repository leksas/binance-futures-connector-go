package main

import (
	"context"
	"fmt"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	ServerTime()
}

func ServerTime() {

	client := bf.NewClient("", "")

	// set to debug mode
	client.Debug = true

	// NewServerTimeService
	serverTime, err := client.NewServerTimeService().Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(bf.PrettyPrint(serverTime))
}
