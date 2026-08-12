package binance_futures_connector

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

type PriceLevel struct {
	Price    string `json:"p"`
	Quantity string `json:"q"`
}

const (
	UserDataEventTypeOutboundAccountPosition UserDataEventType = "outboundAccountPosition"
	UserDataEventTypeBalanceUpdate           UserDataEventType = "balanceUpdate"
	UserDataEventTypeExecutionReport         UserDataEventType = "executionReport"
	UserDataEventTypeListStatus              UserDataEventType = "listStatus"
	UserDataEventTypeTerminated              UserDataEventType = "terminated"
)

// Parse parses this PriceLevel's Price and Quantity and
// returns them both.  It also returns an error if either
// fails to parse.
func (p *PriceLevel) Parse() (float64, float64, error) {
	price, err := strconv.ParseFloat(p.Price, 64)
	if err != nil {
		return 0, 0, err
	}
	quantity, err := strconv.ParseFloat(p.Quantity, 64)
	if err != nil {
		return price, 0, err
	}
	return price, quantity, nil
}

// Ask is a type alias for PriceLevel.
type Ask = PriceLevel

// Bid is a type alias for PriceLevel.
type Bid = PriceLevel

var (
	// WebsocketTimeout is an interval for sending ping/pong messages if WebsocketKeepalive is enabled
	WebsocketTimeout = time.Second * 60
	// WebsocketKeepalive enables sending ping/pong messages to check the connection stability
	WebsocketKeepalive = true
)

type Depth struct {
	Price    float64
	Quantity float64
}

// WsPartialDepthHandler handle websocket partial depth event
type WsPartialDepthHandler func(event *WsPartialDepthEvent)

// WsPartialDepthServe serve websocket partial depth handler with a symbol, using 1sec updates
func (c *WebsocketStreamClient) WsPartialDepthServe(symbol string, levels string, handler WsPartialDepthHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@depth%s", c.Endpoint, strings.ToLower(symbol), levels)
	return wsPartialDepthServe(endpoint, handler, errHandler)
}

// WsPartialDepthServe100Ms serve websocket partial depth handler with a symbol, using 100msec updates
func (c *WebsocketStreamClient) WsPartialDepthServe100Ms(symbol string, levels string, handler WsPartialDepthHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@depth%s@100ms", c.Endpoint, strings.ToLower(symbol), levels)
	return wsPartialDepthServe(endpoint, handler, errHandler)
}

// WsPartialDepthServe serve websocket partial depth handler with a symbol
func wsPartialDepthServe(endpoint string, handler WsPartialDepthHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsPartialDepthEvent)
		err = Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

type WsPartialDepthEvent struct {
	Event         string     `json:"e"`  // 事件类型，这里是 "depthUpdate"
	EventTime     int64      `json:"E"`  // 事件发生的时间戳（毫秒）
	TradeTime     int64      `json:"T"`  // 交易时间戳（毫秒）
	Symbol        string     `json:"s"`  // 交易对，如 "BNBUSDT"
	FirstUpdateID int64      `json:"U"`  // 本次更新的第一个ID
	LastUpdateID  int64      `json:"u"`  // 本次更新的最后一个ID
	PrevLastID    int64      `json:"pu"` // 上一次更新的最后一个ID
	Bids          [][]string `json:"b"`  // 买方挂单列表，格式为 [价格, 数量]
	Asks          [][]string `json:"a"`  // 卖方挂单列表，格式为 [价格, 数量]
}

type WsCombinedPartialDepthEvent struct {
	Data   *WsPartialDepthEvent `json:"data"`
	Stream string               `json:"stream"`
}

// WsCombinedPartialDepthServe is similar to WsPartialDepthServe, but it for multiple symbols
func (c *WebsocketStreamClient) WsCombinedPartialDepthServe(symbolLevels map[string]string, handler WsPartialDepthHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := c.Endpoint
	for s, l := range symbolLevels {
		endpoint += fmt.Sprintf("%s@depth%s", strings.ToLower(s), l) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsCombinedPartialDepthEvent)
		err = Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event.Data)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsDepthHandler handle websocket depth event
type WsDepthHandler func(event *WsDepthEvent)

// WsDepthServe serve websocket depth handler with a symbol, using 1sec updates
func (c *WebsocketStreamClient) WsDepthServe(symbol string, handler WsDepthHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@depth", c.Endpoint, strings.ToLower(symbol))
	return wsDepthServe(endpoint, handler, errHandler)
}

// WsDepthServe100Ms serve websocket depth handler with a symbol, using 100msec updates
func (c *WebsocketStreamClient) WsDepthServe100Ms(symbol string, handler WsDepthHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@depth@100ms", c.Endpoint, strings.ToLower(symbol))
	return wsDepthServe(endpoint, handler, errHandler)
}

// WsDepthServe serve websocket depth handler with an arbitrary endpoint address
func wsDepthServe(endpoint string, handler WsDepthHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	cfg := newWsConfig(endpoint)

	var parser fastjson.Parser
	wsHandler := func(message []byte) {
		j, err := parser.ParseBytes(message)
		if err != nil {
			errHandler(err)
			return
		}
		event := new(WsDepthEvent)

		event.Event = string(j.GetStringBytes("e"))
		event.Time = j.GetInt64("E")
		event.Symbol = string(j.GetStringBytes("s"))
		event.FirstUpdateID = j.GetInt64("U")
		event.LastUpdateID = j.GetInt64("u")

		bids := j.GetArray("bids")
		bidsLen := len(bids)
		event.Bids = make([]Bid, bidsLen)

		for i := 0; i < bidsLen; i++ {
			item, _ := bids[i].Array()
			event.Bids[i] = Bid{
				Price:    string(item[0].GetStringBytes()),
				Quantity: string(item[1].GetStringBytes()),
			}
		}

		asks := j.GetArray("asks")
		asksLen := len(asks)
		event.Asks = make([]Ask, asksLen)
		for i := 0; i < asksLen; i++ {
			item, _ := asks[i].Array()
			event.Asks[i] = Ask{
				Price:    string(item[0].GetStringBytes()),
				Quantity: string(item[1].GetStringBytes()),
			}
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsDepthEvent define websocket depth event
type WsDepthEvent struct {
	Event         string `json:"e"`
	Time          int64  `json:"E"`
	Symbol        string `json:"s"`
	FirstUpdateID int64  `json:"U"`
	LastUpdateID  int64  `json:"u"`
	Bids          []Bid  `json:"b"`
	Asks          []Ask  `json:"a"`
}

// WsCombinedDepthServe is similar to WsDepthServe, but it for multiple symbols
func (c *WebsocketStreamClient) WsCombinedDepthServe(symbols []string, handler WsDepthHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := c.Endpoint
	for _, s := range symbols {
		endpoint += fmt.Sprintf("%s@depth", strings.ToLower(s)) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	return wsCombinedDepthServe(endpoint, handler, errHandler)
}

func (c *WebsocketStreamClient) WsCombinedDepthServe100Ms(symbols []string, handler WsDepthHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := c.Endpoint
	for _, s := range symbols {
		endpoint += fmt.Sprintf("%s@depth@100ms", strings.ToLower(s)) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	return wsCombinedDepthServe(endpoint, handler, errHandler)
}

func wsCombinedDepthServe(endpoint string, handler WsDepthHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	cfg := newWsConfig(endpoint)

	var parser fastjson.Parser
	wsHandler := func(message []byte) {
		j, err := parser.ParseBytes(message)
		if err != nil {
			errHandler(err)
			return
		}
		event := new(WsDepthEvent)
		stream := string(j.GetStringBytes("stream"))
		symbol := strings.Split(stream, "@")[0]
		event.Symbol = strings.ToUpper(symbol)

		data := j.Get("data")
		event.Time = data.GetInt64("E")
		event.LastUpdateID = data.GetInt64("u")
		event.FirstUpdateID = data.GetInt64("U")

		bids := data.GetArray("b")
		bidsLen := len(bids)
		event.Bids = make([]Bid, bidsLen)
		for i := 0; i < bidsLen; i++ {
			item, _ := bids[i].Array()
			event.Bids[i] = Bid{
				Price:    item[0].String(),
				Quantity: item[1].String(),
			}
		}

		asks := data.GetArray("a")
		asksLen := len(asks)
		event.Asks = make([]Ask, asksLen)
		for i := 0; i < asksLen; i++ {
			item, _ := asks[i].Array()
			event.Asks[i] = Ask{
				Price:    item[0].String(),
				Quantity: item[1].String(),
			}
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsBookTickerEvent define websocket best book ticker event.
type WsBookTickerEvent struct {
	Event           string `json:"e"`
	UpdateID        int64  `json:"u"`
	EventTime       int64  `json:"E"`
	TransactionTime int64  `json:"T"`
	Symbol          string `json:"s"`
	BestBidPrice    string `json:"b"`
	BestBidQty      string `json:"B"`
	BestAskPrice    string `json:"a"`
	BestAskQty      string `json:"A"`
}

type WsCombinedBookTickerEvent struct {
	Data   *WsBookTickerEvent `json:"data"`
	Stream string             `json:"stream"`
}

// WsBookTickerHandler handle websocket that pushes updates to the best bid or ask price or quantity in real-time for a specified symbol.
type WsBookTickerHandler func(event *WsBookTickerEvent)

// WsBookTickerServe serve websocket that pushes updates to the best bid or ask price or quantity in real-time for a specified symbol.
func (c *WebsocketStreamClient) WsBookTickerServe(symbol string, handler WsBookTickerHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@bookTicker", c.Endpoint, strings.ToLower(symbol))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsBookTickerEvent)
		err := Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsCombinedBookTickerServe is similar to WsBookTickerServe, but it is for multiple symbols
func (c *WebsocketStreamClient) WsCombinedBookTickerServe(symbols []string, handler WsBookTickerHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := c.Endpoint
	for _, s := range symbols {
		endpoint += fmt.Sprintf("%s@bookTicker", strings.ToLower(s)) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint, c.BindIP)
	wsHandler := func(message []byte) {
		event := new(WsCombinedBookTickerEvent)
		err = Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event.Data)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

type WsCombinedKlineEvent struct {
	Data   *WsKlineEvent `json:"data"`
	Stream string        `json:"stream"`
}

// WsKlineHandler handle websocket kline event
type WsKlineHandler func(event *WsKlineEvent)

// WsCombinedKlineServe is similar to WsKlineServe, but it handles multiple symbols with it interval
func (c *WebsocketStreamClient) WsCombinedKlineServe(symbolIntervalPair map[string]string, handler WsKlineHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := c.Endpoint
	for symbol, interval := range symbolIntervalPair {
		endpoint += fmt.Sprintf("%s@kline_%s", strings.ToLower(symbol), interval) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)

	wsHandler := func(message []byte) {
		event := new(WsCombinedKlineEvent)
		err = Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event.Data)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsKlineServe serve websocket kline handler with a symbol and interval like 15m, 30s
func (c *WebsocketStreamClient) WsKlineServe(symbol string, interval string, handler WsKlineHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@kline_%s", c.Endpoint, strings.ToLower(symbol), interval)
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsKlineEvent)
		err := Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsKlineEvent define websocket kline event
type WsKlineEvent struct {
	Event  string  `json:"e"`
	Time   int64   `json:"E"`
	Symbol string  `json:"s"`
	Kline  WsKline `json:"k"`
}

// WsKline define websocket kline
type WsKline struct {
	StartTime            int64  `json:"t"`
	EndTime              int64  `json:"T"`
	Symbol               string `json:"s"`
	Interval             string `json:"i"`
	FirstTradeID         int64  `json:"f"`
	LastTradeID          int64  `json:"L"`
	Open                 string `json:"o"`
	Close                string `json:"c"`
	High                 string `json:"h"`
	Low                  string `json:"l"`
	Volume               string `json:"v"`
	TradeNum             int64  `json:"n"`
	IsFinal              bool   `json:"x"`
	QuoteVolume          string `json:"q"`
	ActiveBuyVolume      string `json:"V"`
	ActiveBuyQuoteVolume string `json:"Q"`
}

type WsCombinedContinuousKlineEvent struct {
	Data   *WsContinuousKlineEvent `json:"data"`
	Stream string                  `json:"stream"`
}

// WsContinuousKlineHandler handle websocket continuous kline event
type WsContinuousKlineHandler func(event *WsContinuousKlineEvent)

// WsCombinedContinuousKlineServe is similar to WsContinuousKlineServe, but it handles multiple symbols with it contract type and interval
func (c *WebsocketStreamClient) WsCombinedContinuousKlineServe(
	symbolIntervalPair map[string]string,
	contractType string,
	handler WsContinuousKlineHandler,
	errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {

	endpoint := c.Endpoint
	for symbol, interval := range symbolIntervalPair {
		endpoint += fmt.Sprintf("%s_%s@continuousKline_%s", strings.ToLower(symbol), strings.ToLower(contractType), interval) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)

	wsHandler := func(message []byte) {
		event := new(WsCombinedContinuousKlineEvent)
		err = Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event.Data)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsContinuousKlineServe serve websocket continuous kline handler with a symbol, contract type and interval like 15m, 30s
func (c *WebsocketStreamClient) WsContinuousKlineServe(symbol, contractType, interval string, handler WsContinuousKlineHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s_%s@continuousKline_%s", c.Endpoint, strings.ToLower(symbol), strings.ToLower(contractType), interval)
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsContinuousKlineEvent)
		err := Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsContinuousKlineEvent define websocket continuous kline event
type WsContinuousKlineEvent struct {
	Event        string  `json:"e"`
	Time         int64   `json:"E"`
	Symbol       string  `json:"ps"`
	ContractType string  `json:"ct"`
	Kline        WsKline `json:"k"`
}

// WsTradeHandler handle websocket trade event
type WsTradeHandler func(event *WsTradeEvent)

// WsTradeServe serve websocket handler with a symbol
func (c *WebsocketStreamClient) WsTradeServe(symbol string, handler WsTradeHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@trade", c.Endpoint, strings.ToLower(symbol))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsTradeEvent)
		err := Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

type WsCombinedTradeEvent struct {
	Data   *WsTradeEvent `json:"data"`
	Stream string        `json:"stream"`
}

// WsTradeEvent define websocket trade event
type WsTradeEvent struct {
	Event        string `json:"e"`
	Time         int64  `json:"E"`
	Symbol       string `json:"s"`
	TradeID      int64  `json:"t"`
	Price        string `json:"p"` // sometime is 0, need filter
	Quantity     string `json:"q"`
	TradeTime    int64  `json:"T"`
	X            string `json:"X"` // MARKET, NA
	IsBuyerMaker bool   `json:"m"`
}

// WsCombinedTradeServe is similar to WsTradeServe, but it handles multiple symbol
func (c *WebsocketStreamClient) WsCombinedTradeServe(symbols []string, handler WsTradeHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := c.Endpoint
	for s := range symbols {
		endpoint += fmt.Sprintf("%s@trade", strings.ToLower(symbols[s])) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsCombinedTradeEvent)
		err = Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		// skip 0 price
		if event.Data.Price == "0" || event.Data.X == "NA" {
			return
		}
		handler(event.Data)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsAggTradeHandler handle websocket aggregate trade event
type WsAggTradeHandler func(event *WsAggTradeEvent)

// WsAggTradeServe serve websocket aggregate handler with a symbol
func (c *WebsocketStreamClient) WsAggTradeServe(symbol string, handler WsAggTradeHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@aggTrade", c.Endpoint, strings.ToLower(symbol))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsAggTradeEvent)
		err := Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

type WsCombinedAggTradeEvent struct {
	Data   *WsAggTradeEvent `json:"data"`
	Stream string           `json:"stream"`
}

// WsAggTradeEvent define websocket aggregate trade event
type WsAggTradeEvent struct {
	Event                 string `json:"e"`
	Time                  int64  `json:"E"`
	Symbol                string `json:"s"`
	AggTradeID            int64  `json:"a"`
	Price                 string `json:"p"`
	Quantity              string `json:"q"`
	FirstBreakdownTradeID int64  `json:"f"`
	LastBreakdownTradeID  int64  `json:"l"`
	TradeTime             int64  `json:"T"`
	IsBuyerMaker          bool   `json:"m"`
}

// WsCombinedAggTradeServe is similar to WsAggTradeServe, but it handles multiple symbol
func (c *WebsocketStreamClient) WsCombinedAggTradeServe(symbols []string, handler WsAggTradeHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := c.Endpoint
	for s := range symbols {
		endpoint += fmt.Sprintf("%s@aggTrade", strings.ToLower(symbols[s])) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsCombinedAggTradeEvent)
		err = Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event.Data)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

type WsCombinedMarkPriceEvent struct {
	Data   *WsMarkPriceEvent `json:"data"`
	Stream string            `json:"stream"`
}

// WsMarkPriceEvent define websocket mark price event
type WsMarkPriceEvent struct {
	Event                string `json:"e"` // 事件类型
	Time                 int64  `json:"E"` // 事件时间
	Symbol               string `json:"s"` // 交易对
	MarkPrice            string `json:"p"` // 标记价格
	IndexPrice           string `json:"i"` // 现货指数价格
	EstimatedSettlePrice string `json:"P"` // 预估结算价,仅在结算前最后一小时有参考价值
	LastFundingRate      string `json:"r"` // 资金费率
	NextFundingTime      int64  `json:"T"` // 下次资金费率时间
}

// WsMarkPriceHandler handle websocket mark price event
type WsMarkPriceHandler func(event *WsMarkPriceEvent)

// WsMarkPriceServe serve websocket mark price handler with a symbol
func (c *WebsocketStreamClient) WsMarkPriceServe(symbol string, everySecond bool, handler WsMarkPriceHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@markPrice", c.Endpoint, strings.ToLower(symbol))
	if everySecond {
		endpoint = fmt.Sprintf("%s@markPrice@1s", strings.ToLower(symbol))
	}

	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsMarkPriceEvent)
		err := Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsCombinedMarkPriceServe is similar to WsMarkPriceServe, but it handles multiple symbol
func (c *WebsocketStreamClient) WsCombinedMarkPriceServe(symbols []string, everySecond bool, handler WsMarkPriceHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := c.Endpoint
	for s := range symbols {
		if everySecond {
			endpoint += fmt.Sprintf("%s@markPrice@1s", strings.ToLower(symbols[s])) + "/"
		} else {
			endpoint += fmt.Sprintf("%s@markPrice", strings.ToLower(symbols[s])) + "/"
		}
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsCombinedMarkPriceEvent)
		err = Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event.Data)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

type WsMinTickerEvent struct {
	Event       string `json:"e"`
	EventTime   int64  `json:"E"`
	Symbol      string `json:"s"`
	Close       string `json:"c"`
	Open        string `json:"o"`
	High        string `json:"h"`
	Low         string `json:"l"`
	TotalVolume string `json:"v"`
	TotalAmount string `json:"q"`
}

type WsCombinedMiniTickerEvent struct {
	Data   *WsMinTickerEvent `json:"data"`
	Stream string            `json:"stream"`
}

// WsMarkTickerHandler handle websocket mark ticker event
type WsMiniTickerHandler func(event *WsMinTickerEvent)

// WsMiniTickerServe serve websocket mini ticker handler with a symbol
func (c *WebsocketStreamClient) WsMiniTickerServe(symbol string, handler WsMiniTickerHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@miniTicker", c.Endpoint, strings.ToLower(symbol))

	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsMinTickerEvent)
		err := Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsCombinedMiniTickerServe is similar to WsMiniTickerServe, but it handles multiple symbol
func (c *WebsocketStreamClient) WsCombinedMiniTickerServe(symbols []string, everySecond bool, handler WsMiniTickerHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := c.Endpoint
	for s := range symbols {
		endpoint += fmt.Sprintf("%s@miniPrice", strings.ToLower(symbols[s])) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsCombinedMiniTickerEvent)
		err = Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event.Data)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

type WsTickerArrEvent struct {
	Event              string `json:"e"`
	EventTime          int64  `json:"E"`
	Symbol             string `json:"s"`
	PriceChange        string `json:"p"`
	PriceChangePercent string `json:"P"`
	WeightedAvgPrice   string `json:"w"`
	Close              string `json:"c"`
	Quantity           string `json:"Q"`
	Open               string `json:"o"`
	High               string `json:"h"`
	Low                string `json:"l"`
	TotalVolume        string `json:"v"`
	TotalAmount        string `json:"q"`
	OpenTime           int64  `json:"O"`
	CloseTime          int64  `json:"C"`
	FirstTradeID       int64  `json:"F"`
	LastTradeID        int64  `json:"L"`
	TradeCount         int64  `json:"n"`
}

type WsCombinedTickerArrEvent struct {
	Data   []*WsTickerArrEvent `json:"data"`
	Stream string              `json:"stream"`
}

// WsMarkTickerHandler handle websocket mark ticker event
type WsTickerArrHandler func(event []*WsTickerArrEvent)

// WsTickerArrServe serve websocket ticker arr handler with all symbol
func (c *WebsocketStreamClient) WsCombinedTickerArrServe(handler WsTickerArrHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/!ticker@arr", c.Endpoint)

	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsCombinedTickerArrEvent)
		err := Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event.Data)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

type WsForceOrderEvent struct {
	Event      string       `json:"e"`
	EventTime  int64        `json:"E"`
	Order      WsForceOrder `json:"o"`
	Symbol     string       `json:"ps"` // (迁移后新增) 标的交易对
	SymbolType int          `json:"st"` // (迁移后新增) Symbol 类型:1 = UM,2 = CM
}

type WsForceOrder struct {
	Symbol      string      `json:"s"`
	Side        Side        `json:"S"`
	OrderType   OrderType   `json:"o"`
	TimeInForce TimeInForce `json:"f"`
	Quantity    string      `json:"q"`
	Price       string      `json:"p"`
	AvgPrice    string      `json:"ap"`
	Status      OrderStatus `json:"X"`
	LastQty     string      `json:"l"`
	FilledQty   string      `json:"z"`
	TradeTime   int64       `json:"T"`
}

type WsCombinedForceOrderEvent struct {
	Data   *WsForceOrderEvent `json:"data"`
	Stream string             `json:"stream"`
}

// WsForceOrderHandler handle websocket force order event
type WsForceOrderHandler func(event *WsForceOrderEvent)

func (c *WebsocketStreamClient) WsCombinedForceOrderServe(symbols []string, handler WsForceOrderHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := c.Endpoint
	for s := range symbols {
		endpoint += fmt.Sprintf("%s@forceOrder", strings.ToLower(symbols[s])) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsCombinedForceOrderEvent)
		err = Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event.Data)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

func (c *WebsocketStreamClient) WsCombinedForceOrderArrServe(handler WsForceOrderHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/forceOrder@arr", c.Endpoint)
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsCombinedForceOrderEvent)
		err = Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event.Data)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsUserDataHandler handle WsUserDataEvent
type WsUserDataHandler func(event *WsUserDataEvent)

// WsUserDataServe serve user data handler with listen key
func (c *WebsocketStreamClient) WsUserDataServe(listenKey string, handler WsUserDataHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s", c.Endpoint, listenKey)
	cfg := newWsConfig(endpoint, c.BindIP)
	wsHandler := func(message []byte) {
		recvTime := time.Now()
		j, err := newJSONv2(message)
		if err != nil {
			errHandler(err)
			return
		}

		var event = new(WsUserDataEvent)
		event.Event = UserDataType(j.GetStringBytes("e"))
		event.RecvTime = recvTime
		switch event.Event {
		case ListenKeyExpired:
			err = Unmarshal(message, &event.ListenKeyExpire)
		case AccountDataUpdate:
			err = Unmarshal(message, &event.AccountData)
		case MarginCall:
			err = Unmarshal(message, &event.MarginCall)
		case OrderTradeUpdate:
			event.OrderTrade = WsOrderTradeEvent{
				Event:           UserDataEventType(OrderTradeUpdate),
				EventTime:       j.GetInt64("E"),
				TransactionTime: j.GetInt64("T"),
				Order:           parseOrderTrade(j.Get("o")),
			}
		case TradeLite:
			err = Unmarshal(message, &event.TradeLite)
		case AccountConfigUpdate:
			err = Unmarshal(message, &event.AccountConfig)
		case StrategyUpdate:
			err = Unmarshal(message, &event.StrategyUpdate)
		case GridUpdate:
			err = Unmarshal(message, &event.GridUpdate)
		case ConditionalOrderTriggerReject:
			err = Unmarshal(message, &event.OrderReject)
		case AlgoUpdate:
			err = Unmarshal(message, &event.AlgoUpdate)
		}
		if err != nil {
			errHandler(err)
			return
		}

		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

func parseOrderTrade(data *fastjson.Value) WsOrder {
	ordStatus := string(data.GetStringBytes("X"))
	switch ordStatus {
	case PARTIALLY_FILLED, FILLED:
		return WsOrder{
			Symbol:         string(data.GetStringBytes("s")),
			ClientOrderId:  string(data.GetStringBytes("c")),
			Side:           Side(data.GetStringBytes("S")),
			PositionSide:   PositionSide(data.GetStringBytes("ps")),
			OrderType:      OrderType(data.GetStringBytes("o")),
			OrigType:       OrderType(data.GetStringBytes("ot")),
			TimeInForce:    TimeInForce(data.GetStringBytes("f")),
			Price:          string(data.GetStringBytes("p")),
			Quantity:       string(data.GetStringBytes("q")),
			StopPrice:      string(data.GetStringBytes("sp")),
			AvgPrice:       string(data.GetStringBytes("ap")),
			ExecType:       string(data.GetStringBytes("x")),
			Status:         OrderStatus(data.GetStringBytes("X")),
			OrderId:        int64(data.GetInt64("i")),
			LastVolume:     string(data.GetStringBytes("l")),
			FilledVolume:   string(data.GetStringBytes("z")),
			LastPrice:      string(data.GetStringBytes("L")),
			FeeAsset:       string(data.GetStringBytes("N")),
			FeeCost:        string(data.GetStringBytes("n")),
			TradeTime:      int64(data.GetInt64("T")),
			TradeId:        int64(data.GetInt64("t")),
			BidValue:       string(data.GetStringBytes("b")),
			AskValue:       string(data.GetStringBytes("a")),
			IsMaker:        bool(data.GetBool("m")),
			ReduceOnly:     bool(data.GetBool("rp")),
			WorkingType:    WorkingType(data.GetStringBytes("wt")),
			IsActive:       bool(data.GetBool("cp")),
			ActivePrice:    string(data.GetStringBytes("AP")),
			PriceRate:      string(data.GetStringBytes("cr")),
			PriceProtect:   bool(data.GetBool("pP")),
			SI:             data.GetInt("si"),
			SS:             data.GetInt("ss"),
			RealizedProfit: string(data.GetStringBytes("rp")),
			StpMode:        STPMode(data.GetStringBytes("V")),
			PriceMatch:     string(data.GetStringBytes("pm")),
			GoodTillDate:   data.GetInt64("gtd"),
		}
	default:
		return WsOrder{
			Symbol:        string(data.GetStringBytes("s")),
			ClientOrderId: string(data.GetStringBytes("c")),
			Side:          Side(data.GetStringBytes("S")),
			PositionSide:  PositionSide(data.GetStringBytes("ps")),
			OrderType:     OrderType(data.GetStringBytes("o")),
			OrigType:      OrderType(data.GetStringBytes("ot")),
			TimeInForce:   TimeInForce(data.GetStringBytes("f")),
			Price:         string(data.GetStringBytes("p")),
			Quantity:      string(data.GetStringBytes("q")),
			Status:        OrderStatus(data.GetStringBytes("X")),
			StopPrice:     string(data.GetStringBytes("sp")),
			TradeTime:     int64(data.GetInt64("T")),
			StpMode:       STPMode(data.GetStringBytes("V")),
		}
	}
}

// WsUserDataEvent define user data event
type WsUserDataEvent struct {
	Event    UserDataType
	RecvTime time.Time

	ListenKeyExpire WsListenKeyExpiredEvent     `json:"listenKeyExpire,omitempty"`
	AccountData     WsAccountUpdateEvent        `json:"accountData,omitempty"`
	MarginCall      WsMarginCallEvent           `json:"marginCall,omitempty"`
	OrderTrade      WsOrderTradeEvent           `json:"orderTrade,omitempty"`
	TradeLite       WsTradeLiteEvent            `json:"tradeLite,omitempty"`
	AccountConfig   WsAccountConfigEvent        `json:"accountConfig,omitempty"`
	StrategyUpdate  WsStrategyEvent             `json:"strategyUpdate,omitempty"`
	GridUpdate      WsGridEvent                 `json:"gridUpdate,omitempty"`
	OrderReject     WsConditionOrderRejectEvent `json:"orderReject,omitempty"`
	AlgoUpdate      WsAlgoUpdateEvent           `json:"algoUpdate,omitempty"`
}

type (
	// listenKey过期推送 listenKeyExpired
	// 当前连接使用的有效listenKey过期时，user data stream 将会推送此事件
	WsListenKeyExpiredEvent struct {
		Event     UserDataEventType `json:"e"` // 事件类型
		EventTime string            `json:"E"` // 事件时间
		ListenKey string            `json:"listenKey"`
	}

	// Balance 和 Position 更新推送 ACCOUNT_UPDATE
	WsAccountUpdateEvent struct {
		Event           UserDataEventType `json:"e"` // 事件类型
		EventTime       int64             `json:"E"` // 事件时间
		TransactionTime int64             `json:"T"` // 撮合时间
		AccountUpdate   WsAccountUpdate   `json:"a"` // 账户更新事件
	}

	WsAccountUpdate struct {
		Reason    string             `json:"m"` // 事件推出原因
		Balances  []WsBalanceUpdate  `json:"B"` // 余额信息
		Positions []WsPositionUpdate `json:"P"`
	}

	WsBalanceUpdate struct {
		Asset              string `json:"a"`  // 资产名称
		WalletBalance      string `json:"wb"` // 钱包余额
		CrossWalletBalance string `json:"cw"` // 除去逐仓仓位保证金的钱包余额
		BalanceChange      string `json:"bc"` // 除去盈亏与交易手续费以外的钱包余额改变量
	}

	WsPositionUpdate struct {
		Symbol                  string `json:"s"`   // 交易对
		PositionSide            string `json:"ps"`  // 持仓方向
		PositionAmt             string `json:"pa"`  // 仓位
		EntryPrice              string `json:"ep"`  // 入仓价格
		BreakEvenPrice          string `json:"bep"` // 盈亏平衡价
		CumulatedRealizedProfit string `json:"cr"`  // (费前)累计实现损益
		UnrealizedProfit        string `json:"up"`  // 持仓未实现盈亏
		MarginType              string `json:"mt"`  // 保证金模式
		IsoMargin               string `json:"iw"`  // 若为逐仓，仓位保证金
	}

	// 追加保证金通知 MARGIN_CALL
	WsMarginCallEvent struct {
		Event              UserDataEventType  `json:"e"`  // 事件类型
		EventTime          int64              `json:"E"`  // 事件时间
		CrossWalletBalance string             `json:"cw"` //除去逐仓仓位保证金的钱包余额, 仅在全仓 margin call 情况下推送此字段
		Position           []WsMarginPosition `json:"p"`  // 涉及持仓
	}

	WsMarginPosition struct {
		Symbol            string `json:"s"`  // symbol
		PositionSide      string `json:"ps"` // 持仓方向
		PositionAmt       string `json:"pa"` // 仓位
		MarginType        string `json:"mt"` // 保证金模式
		IsoMargin         string `json:"iw"` // 若为逐仓，仓位保证金
		MarkPrice         string `json:"mp"` // 标记价格
		UnrealizedProfit  string `json:"up"` // 未实现盈亏
		MaintenanceMargin string `json:"mm"` // 持仓需要的维持保证金
	}

	// 订单交易更新推送 ORDER_TRADE_UPDATE
	WsOrderTradeEvent struct {
		Event           UserDataEventType `json:"e"` // 事件类型
		EventTime       int64             `json:"E"` // 事件时间
		TransactionTime int64             `json:"T"` // 撮合时间
		Order           WsOrder           `json:"o"`
	}

	WsOrder struct {
		Symbol        string `json:"s"` // 交易对
		ClientOrderId string `json:"c"` // 客户端自定订单ID
		// 特殊的自定义订单ID:
		// "autoclose-"开头的字符串: 系统强平订单
		// "adl_autoclose": ADL自动减仓订单
		// "settlement_autoclose-": 下架或交割的结算订单
		Side           Side         `json:"S"`   // 订单方向
		OrderType      OrderType    `json:"o"`   // 订单类型
		TimeInForce    TimeInForce  `json:"f"`   // 有效方式
		Quantity       string       `json:"q"`   // 订单原始数量
		Price          string       `json:"p"`   // 订单原始价格
		AvgPrice       string       `json:"ap"`  // 订单平均价格
		StopPrice      string       `json:"sp"`  // 条件订单触发价格，对追踪止损单无效
		ExecType       string       `json:"x"`   // 本次事件的具体执行类型
		Status         OrderStatus  `json:"X"`   // 订单的当前状态
		OrderId        int64        `json:"i"`   // 订单ID
		LastVolume     string       `json:"l"`   // 订单末次成交量
		FilledVolume   string       `json:"z"`   // 订单累计已成交量
		LastPrice      string       `json:"L"`   // 订单末次成交价格
		FeeAsset       string       `json:"N"`   // 手续费资产类型
		FeeCost        string       `json:"n"`   // 手续费数量
		TradeTime      int64        `json:"T"`   // 成交时间
		TradeId        int64        `json:"t"`   // 成交ID
		BidValue       string       `json:"b"`   // 买单净值
		AskValue       string       `json:"a"`   // 卖单净值
		IsMaker        bool         `json:"m"`   // 该成交是作为挂单成交吗？
		ReduceOnly     bool         `json:"R"`   // 是否是只减仓单
		WorkingType    WorkingType  `json:"wt"`  // 触发价类型
		OrigType       string       `json:"ot"`  // 原始订单类型
		PositionSide   PositionSide `json:"ps"`  // 持仓方向
		IsActive       bool         `json:"cp"`  // 是否为触发平仓单; 仅在条件订单情况下会推送此字段
		ActivePrice    string       `json:"AP"`  // 追踪止损激活价格, 仅在追踪止损单时会推送此字段
		PriceRate      string       `json:"cr"`  // 追踪止损回调比例, 仅在追踪止损单时会推送此字段
		PriceProtect   bool         `json:"pP"`  // 是否开启条件单触发保护
		SI             int          `json:"si"`  // 忽略
		SS             int          `json:"ss"`  // 忽略
		RealizedProfit string       `json:"rp"`  // 该交易实现盈亏
		StpMode        STPMode      `json:"V"`   // 自成交防止模式
		PriceMatch     string       `json:"pm"`  // 价格匹配模式
		GoodTillDate   int64        `json:"gtd"` // TIF为GTD的订单自动取消时间
	}

	// 精简交易推送 TRADE_LITE
	WsTradeLiteEvent struct {
		Event           UserDataEventType `json:"e"` // 事件类型
		EventTime       int64             `json:"E"` // 事件时间
		TransactionTime int64             `json:"T"` // 交易时间
		Symbol          string            `json:"s"` // 交易对
		Quantity        string            `json:"q"` // 订单原始数量
		Price           string            `json:"p"` // 订单原始价格
		IsMaker         bool              `json:"m"` // 该成交是作为挂单成交吗？
		ClientOrderId   string            `json:"c"` // 客户端自定订单ID
		// 特殊的自定义订单ID:
		// "autoclose-"开头的字符串: 系统强平订单
		// "adl_autoclose": ADL自动减仓订单
		// "settlement_autoclose-": 下架或交割的结算订单
		Side       Side   `json:"S"` // 订单方向
		LastPrice  string `json:"L"` // 订单末次成交价格
		LastVolume string `json:"l"` // 订单末次成交量
		TradeId    int64  `json:"t"` // 成交ID
		OrderId    int64  `json:"i"` // 订单ID
	}

	// 杠杆倍数等账户配置 更新推送 ACCOUNT_CONFIG_UPDATE
	WsAccountConfigEvent struct {
		Event           UserDataEventType `json:"e"` // 事件类型
		EventTime       int64             `json:"E"` // 事件时间
		TransactionTime int64             `json:"T"` // 撮合时间
		LeverageConfig  struct {
			Symbol   string `json:"s"` // 交易对
			Leverage int    `json:"l"` // 杠杆倍数
		} `json:"ac,omitempty"`
		MarginConfig struct { // 用户账户配置
			MultiAssetMargin bool `json:"j"` // 联合保证金状态
		} `json:"ai,omitempty"`
	}

	// 策略交易更新推送 STRATEGY_UPDATE
	// 在策略交易创建、取消、失效等等时候更新
	WsStrategyEvent struct {
		Event           UserDataEventType `json:"e"`  // 事件类型
		EventTime       int64             `json:"E"`  // 事件时间
		TransactionTime int64             `json:"T"`  // 撮合时间
		StrategyEvent   WsStrategyUpdate  `json:"su"` // 策略事件
	}

	WsStrategyUpdate struct {
		StrategyId     int64          `json:"si"` // 策略ID
		StrategyType   string         `json:"st"` // 策略类型
		StrategyStatus StrategyStatus `json:"ss"` // 策略状态
		Symbol         string         `json:"s"`  // 交易对
		UpdateTime     int64          `json:"ut"` // 更新时间
		OpCode         OpCode         `json:"c"`  // opCode
	}

	// 网格更新推送 GRID_UPDATE
	// 在网格子订单有部份或是完全成交时更新
	WsGridEvent struct {
		Event           UserDataEventType `json:"e"` // 事件类型
		EventTime       int64             `json:"E"` // 事件时间
		TransactionTime int64             `json:"T"` // 撮合时间
		GridUpdate      WsGridUpdate      `json:"gu"`
	}

	WsGridUpdate struct {
		StrategyId        int64          `json:"si"` // 策略ID
		StrategyType      string         `json:"st"` // 策略类型
		StrategyStatus    StrategyStatus `json:"ss"` // 策略状态
		Symbol            string         `json:"s"`  // 交易对
		RealizedProfit    string         `json:"r"`  // 已实现 PNL
		UnmatchedPrice    float64        `json:"up"` // 未匹配价格
		UnmatchedQuantity string         `json:"uq"` // 未匹配数量
		UnmatchedFee      string         `json:"uf"` // 未匹配手续费
		MatchProfit       string         `json:"mp"` // 已配对 PNL
		UpdateTime        int64          `json:"ut"` // 更新时间
	}

	// 条件订单(TP/SL)触发后拒绝更新推送 CONDITIONAL_ORDER_TRIGGER_REJECT
	// 在止盈止损单触发后被拒绝时推送
	WsConditionOrderRejectEvent struct {
		Event           UserDataEventType `json:"e"` // 事件类型
		EventTime       int64             `json:"E"` // 事件时间
		TransactionTime int64             `json:"T"` // 撮合时间
		OrderReject     WsOrderReject     `json:"or"`
	}

	WsOrderReject struct {
		Symbol  string `json:"s"` // 交易对
		OrderId int64  `json:"i"` // 订单号
		Reason  string `json:"r"`
	}

	// 条件订单交易更新推送 ALGO_UPDATE
	WsAlgoUpdateEvent struct {
		Event           UserDataEventType `json:"e"` // 事件类型
		EventTime       int64             `json:"E"` // 事件时间
		TransactionTime int64             `json:"T"` // 撮合时间
		Order           WsAlgoWsOrder     `json:"o"`
	}

	WsAlgoWsOrder struct {
		ClientAlgoID  string       `json:"caid"` // 客户端自定条件订单ID
		AlgoID        int64        `json:"aid"`  // 条件单 Id
		AlgoType      string       `json:"at"`   // 条件单类型
		OrderType     OrderType    `json:"o"`    // 订单类型
		Symbol        string       `json:"s"`    // 交易对
		Side          Side         `json:"S"`    // 订单方向
		PositionSide  PositionSide `json:"ps"`   // 持仓方向
		TimeInForce   TimeInForce  `json:"f"`    // 有效方式
		Quantity      string       `json:"q"`    // 订单数量
		Status        OrderStatus  `json:"X"`    // 条件单状态
		OrderID       string       `json:"ai"`   // 触发后普通订单 id
		AvgPrice      string       `json:"ap"`   // 触发后在撮合引擎中实际订单的平均成交价格，仅在订单被触发并进入撮合引擎时显示
		FilledVolume  string       `json:"aq"`   // 触发后在撮合引擎中实际订单已成交数量，仅当订单被触发并进入撮合引擎时显示
		ActualType    string       `json:"act"`  // 触发后在撮合引擎中实际的订单类型，仅当订单被触发并进入撮合引擎时显示
		TriggerPrice  string       `json:"tp"`   // 条件单触发价格
		Price         string       `json:"p"`    // 订单价格
		STPMode       STPMode      `json:"V"`    // 自成交防止模式
		WorkingType   WorkingType  `json:"wt"`   // 触发价类型
		PriceMatch    PriceMatch   `json:"pm"`   // 价格匹配模式
		ClosePosition bool         `json:"cp"`   // 是否平仓
		PriceProtect  bool         `json:"pP"`   // 是否开启条件单触发保护
		ReduceOnly    bool         `json:"r"`    // 是否是只减仓单
		TriggerTime   int64        `json:"tt"`   // 触发时间
		GoodTillDate  int64        `json:"gtd"`  // TIF为GTD的订单自动取消时间
		RejectReason  string       `json:"rm"`   // 条件单失败原因
	}
)
