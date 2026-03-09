package main

import (
	"fmt"
	"time"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	WsTickerArrExample()
}

func WsTickerArrExample() {
	websocketStreamClient := bf.NewWebsocketStreamClient(true)
	wsTickerArrHandler := func(event []*bf.WsTickerArrEvent) {
		fmt.Println(bf.PrettyPrint(event))
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
