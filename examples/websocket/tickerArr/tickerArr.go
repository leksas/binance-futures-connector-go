package main

import (
	"fmt"
	"time"

	binance_futures_connector "github.com/leksas/binance-futures-connector-go"
)

func main() {
	WsTickerArrExample()
}

func WsTickerArrExample() {
	websocketStreamClient := binance_futures_connector.NewWebsocketStreamClient(true)
	wsTickerArrHandler := func(event []*binance_futures_connector.WsTickerArrEvent) {
		fmt.Println(binance_futures_connector.PrettyPrint(event))
	}
	errHandler := func(err error) {
		fmt.Println(err)
	}
	doneCh, stopCh, err := websocketStreamClient.WsCombinedTickerArrServe(wsTickerArrHandler, errHandler)
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
