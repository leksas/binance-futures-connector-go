package main

import (
	"fmt"
	"time"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	WsKlineExample()
}

func WsKlineExample() {
	websocketStreamClient := bf.NewWebsocketStreamClient(false)
	wsKlineHandler := func(event *bf.WsKlineEvent) {
		fmt.Println(bf.PrettyPrint(event))
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
