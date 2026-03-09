package main

import (
	"fmt"
	"time"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	WsTradeExample()
}

func WsTradeExample() {
	websocketStreamClient := bf.NewWebsocketStreamClient(true)
	wsTradeHandler := func(event *bf.WsTradeEvent) {
		fmt.Println(bf.PrettyPrint(event))
	}
	errHandler := func(err error) {
		fmt.Println(err)
	}
	symbols := []string{"BTCUSDT"}
	doneCh, stopCh, err := websocketStreamClient.WsCombinedTradeServe(symbols, wsTradeHandler, errHandler)
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
