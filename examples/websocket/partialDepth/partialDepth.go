package main

import (
	"fmt"
	"time"

	binance_futures_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	WsBookTickerExample()
}

func WsBookTickerExample() {
	websocketStreamClient := binance_futures_connector.NewWebsocketStreamClient(true)
	wsPartialDepthHandler := func(event *binance_futures_connector.WsPartialDepthEvent) {
		fmt.Println(binance_futures_connector.PrettyPrint(event))
	}
	errHandler := func(err error) {
		fmt.Println(err)
	}
	symbols := map[string]string{"BTCUSDT": "20"}
	doneCh, stopCh, err := websocketStreamClient.WsCombinedPartialDepthServe(symbols, wsPartialDepthHandler, errHandler)
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
