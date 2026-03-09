package main

import (
	"context"
	"fmt"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	GlobalAccountRatio()
}

func GlobalAccountRatio() {
	client := bf.NewClient("", "")

	rsp, err := client.NewGlobalAccountRatioService().Symbol("BTCUSDT").Period("1h").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(bf.PrettyPrint(rsp))
}
