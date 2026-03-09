package main

import (
	"context"
	"fmt"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	Basis()
}

func Basis() {
	client := bf.NewClient("", "")

	rsp, err := client.NewBasisService().Pair("BTCUSDT").ContractType(bf.PERPETUAL).Period("1h").Limit(100).Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(bf.PrettyPrint(rsp))
}
