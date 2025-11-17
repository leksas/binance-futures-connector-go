package main

import (
	"fmt"
	"time"

	binance_futures_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	WsTradeExample()
}

func WsTradeExample() {
	websocketStreamClient := binance_futures_connector.NewWebsocketStreamClient(true)
	wsTradeHandler := func(event *binance_futures_connector.WsTradeEvent) {
		fmt.Println(binance_futures_connector.PrettyPrint(event))
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
