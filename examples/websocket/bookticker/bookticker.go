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
	wsBookTickerHandler := func(event *bf.WsBookTickerEvent) {
		fmt.Println(bf.PrettyPrint(event))
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
