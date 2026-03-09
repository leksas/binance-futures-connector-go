package main

import (
	"fmt"
	"time"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	WsBookTickerExample()
}

func WsBookTickerExample() {
	websocketStreamClient := bf.NewWebsocketStreamClient(true, bf.PublicEndPoint)
	wsPartialDepthHandler := func(event *bf.WsPartialDepthEvent) {
		fmt.Println(bf.PrettyPrint(event))
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
