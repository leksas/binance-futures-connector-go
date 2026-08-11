package main

import (
	"context"
	"fmt"
	"log"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	GetAPITradingStatus()

}

func GetAPITradingStatus() {
	client := bf.NewClient(bf.API_KEY, bf.SECRET_KEY)

	rsp, err := client.NewGetAPITradingStatusService().
		Do(context.Background())
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Println(rsp)
		log.Printf("Response: %v", bf.PrettyPrint(rsp))
	}
}
