package main

import (
	"context"
	"fmt"

	binance_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	Basis()
}

func Basis() {
	client := binance_connector.NewClient("", "")

	rsp, err := client.NewBasisService().Pair("BTCUSDT").ContractType(binance_connector.PERPETUAL).Period("1h").Limit(100).Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(binance_connector.PrettyPrint(rsp))
}
