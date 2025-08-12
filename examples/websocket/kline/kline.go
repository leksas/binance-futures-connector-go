package main

import (
	"fmt"
	"time"

	binance_futures_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	WsKlineExample()
}

func WsKlineExample() {
	websocketStreamClient := binance_futures_connector.NewWebsocketStreamClient(false)
	wsKlineHandler := func(event *binance_futures_connector.WsKlineEvent) {
		fmt.Println(binance_futures_connector.PrettyPrint(event))
	}
	errHandler := func(err error) {
		fmt.Println(err)
	}
	doneCh, stopCh, err := websocketStreamClient.WsKlineServe("BTCUSDT", "1m", wsKlineHandler, errHandler)
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
