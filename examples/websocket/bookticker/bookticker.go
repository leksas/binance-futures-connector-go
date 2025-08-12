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
	wsBookTickerHandler := func(event *binance_futures_connector.WsBookTickerEvent) {
		fmt.Println(binance_futures_connector.PrettyPrint(event))
	}
	errHandler := func(err error) {
		fmt.Println(err)
	}
	symbols := []string{"BTCUSDT"}
	doneCh, stopCh, err := websocketStreamClient.WsCombinedBookTickerServe(symbols, wsBookTickerHandler, errHandler)
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
