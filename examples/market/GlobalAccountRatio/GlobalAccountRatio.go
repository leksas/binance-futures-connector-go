package main

import (
	"context"
	"fmt"

	binance_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	GlobalAccountRatio()
}

func GlobalAccountRatio() {
	client := binance_connector.NewClient("", "")

	rsp, err := client.NewGlobalAccountRatioService().Symbol("BTCUSDT").Period("1h").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(binance_connector.PrettyPrint(rsp))
}
