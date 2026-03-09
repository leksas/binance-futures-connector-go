package main

import (
	"fmt"
	"time"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	WsAggTradeExample()
}

func WsAggTradeExample() {
	websocketStreamClient := bf.NewWebsocketStreamClient(true)
	wsAggTradeHandler := func(event *bf.WsAggTradeEvent) {
		fmt.Println(bf.PrettyPrint(event))
	}
	errHandler := func(err error) {
		fmt.Println(err)
	}
	symbols := []string{"BTCUSDT"}
	doneCh, stopCh, err := websocketStreamClient.WsCombinedAggTradeServe(symbols, wsAggTradeHandler, errHandler)
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
