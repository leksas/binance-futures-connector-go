package main

import (
	"context"
	"fmt"

	binance_futures_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	TakerRatio()
}

func TakerRatio() {
	client := binance_futures_connector.NewClient("", "")

	rsp, err := client.NewTakerRatioService().Symbol("BTCUSDT").Period("1h").Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(binance_futures_connector.PrettyPrint(rsp))
}
