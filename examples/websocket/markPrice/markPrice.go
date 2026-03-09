package main

import (
	"fmt"
	"time"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	WsMarkPriceExample()
}

func WsMarkPriceExample() {
	websocketStreamClient := bf.NewWebsocketStreamClient(true)
	wsMarkPriceHandler := func(event *bf.WsMarkPriceEvent) {
		fmt.Println(bf.PrettyPrint(event))
	}
	errHandler := func(err error) {
		fmt.Println(err)
	}
	symbols := []string{"BTCUSDT"}
	doneCh, stopCh, err := websocketStreamClient.WsCombinedMarkPriceServe(symbols, true, wsMarkPriceHandler, errHandler)
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
