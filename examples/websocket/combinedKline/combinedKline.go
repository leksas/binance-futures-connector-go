package main

import (
	"fmt"
	"time"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	WsCombineKlineExample()
}

func WsCombineKlineExample() {
	websocketStreamClient := bf.NewWebsocketStreamClient(true)
	wsKlineHandler := func(event *bf.WsKlineEvent) {
		fmt.Println(bf.PrettyPrint(event))
	}
	errHandler := func(err error) {
		fmt.Println(err)
	}

	symbolIntervalPair := map[string]string{
		"BTCUSDT": "1m",
		"ETHUSDT": "1m",
	}
	doneCh, stopCh, err := websocketStreamClient.WsCombinedKlineServe(symbolIntervalPair, wsKlineHandler, errHandler)
	if err != nil {
		fmt.Println(err)
		return
	}
	// use stopCh to exit
	go func() {
		time.Sleep(10 * time.Second)
		stopCh <- struct{}{}
	}()
	<-doneCh
}
