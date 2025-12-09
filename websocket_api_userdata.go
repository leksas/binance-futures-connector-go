package binance_futures_connector

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

const (
	SBE = "SBE"
	WS  = "WS"
)

type StartUserDataStreamService struct {
	websocketAPI *WebsocketAPIClient
}

func (s *StartUserDataStreamService) Do(ctx context.Context) (*StartUserDataStreamResponse, error) {
	parameters := map[string]interface{}{
		"apiKey": s.websocketAPI.APIKey,
	}

	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "userDataStream.start",
		"params": parameters,
	}

	messageCh := make(chan []byte)
	s.websocketAPI.ReqResponseMap.Store(id, messageCh)

	err := s.websocketAPI.SendMessage(payload)
	if err != nil {
		return nil, err
	}

	defer s.websocketAPI.ReqResponseMap.Delete(id)

	select {
	case response := <-messageCh:
		var startResponse StartUserDataStreamResponse
		err = Unmarshal(response, &startResponse)
		if err != nil {
			return nil, err
		}
		return &startResponse, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type StartUserDataStreamResponse struct {
	ID     string              `json:"id"`
	Status int                 `json:"status"`
	Error  *WsAPIErrorResponse `json:"error,omitempty"`
	Result struct {
		ListenKey string `json:"listenKey,omitempty"`
	} `json:"result,omitempty"`
	RateLimits []*WsAPIRateLimit `json:"rateLimits,omitempty"`
}

type PingUserDataStreamService struct {
	websocketAPI *WebsocketAPIClient
	listenKey    string
}

func (s *PingUserDataStreamService) ListenKey(listenKey string) *PingUserDataStreamService {
	s.listenKey = listenKey
	return s
}

func (s *PingUserDataStreamService) Do(ctx context.Context) (*PingUserDataStreamResponse, error) {
	parameters := map[string]interface{}{
		"listenKey": s.listenKey,
	}

	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "userDataStream.ping",
		"params": parameters,
	}

	messageCh := make(chan []byte)
	s.websocketAPI.ReqResponseMap.Store(id, messageCh)

	err := s.websocketAPI.SendMessage(payload)
	if err != nil {
		return nil, err
	}

	defer s.websocketAPI.ReqResponseMap.Delete(id)

	select {
	case response := <-messageCh:
		var pingResponse PingUserDataStreamResponse
		err = Unmarshal(response, &pingResponse)
		if err != nil {
			return nil, err
		}
		return &pingResponse, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type PingUserDataStreamResponse struct {
	ID         string              `json:"id"`
	Status     int                 `json:"status"`
	Error      *WsAPIErrorResponse `json:"error,omitempty"`
	Response   struct{}            `json:"result,omitempty"`
	RateLimits []*WsAPIRateLimit   `json:"rateLimits,omitempty"`
}

type StopUserDataStreamService struct {
	websocketAPI *WebsocketAPIClient
	listenKey    string
}

func (s *StopUserDataStreamService) ListenKey(listenKey string) *StopUserDataStreamService {
	s.listenKey = listenKey
	return s
}

func (s *StopUserDataStreamService) Do(ctx context.Context) (*StopUserDataStreamResponse, error) {
	parameters := map[string]interface{}{
		"apiKey": s.websocketAPI.APIKey,
	}

	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "userDataStream.stop",
		"params": parameters,
	}

	messageCh := make(chan []byte)
	s.websocketAPI.ReqResponseMap.Store(id, messageCh)

	err := s.websocketAPI.SendMessage(payload)
	if err != nil {
		return nil, err
	}

	defer s.websocketAPI.ReqResponseMap.Delete(id)

	select {
	case response := <-messageCh:
		var stopResponse StopUserDataStreamResponse
		err = Unmarshal(response, &stopResponse)
		if err != nil {
			return nil, err
		}
		return &stopResponse, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type StopUserDataStreamResponse struct {
	ID         string              `json:"id"`
	Status     int                 `json:"status"`
	Error      *WsAPIErrorResponse `json:"error,omitempty"`
	Response   struct{}            `json:"result,omitempty"`
	RateLimits []*WsAPIRateLimit   `json:"rateLimits,omitempty"`
}

type SubscribeUserDataStreamService struct {
	websocketAPI *WebsocketAPIClient
}

type UserDataResponse struct {
	Event WsAPIUserDataEvent `json:"event"`
}

type WsAPIUserDataEvent struct {
	RecvTime          time.Time
	Event             UserDataEventType    `json:"e"`
	Time              int64                `json:"E"`
	AccountUpdateTime int64                `json:"u"`
	TransactionTime   int64                `json:"T"`
	AccountUpdates    []WsAPIAccountUpdate `json:"B"`
	BalanceUpdate
}

// WsAccountUpdate define account update
type WsAPIAccountUpdate struct {
	Asset  string `json:"a"`
	Free   string `json:"f"`
	Locked string `json:"l"`
}

type ExecutionReportResponse struct {
	Event struct {
		Event                   UserDataEventType `json:"e"`
		Time                    int64             `json:"E"`
		TransactionTime         int64             `json:"T"`
		Symbol                  string            `json:"s"`
		ClientOrderId           string            `json:"c"`
		Side                    string            `json:"S"`
		Type                    string            `json:"o"`
		TimeInForce             TimeInForceType   `json:"f"`
		Volume                  string            `json:"q"`
		Price                   string            `json:"p"`
		StopPrice               string            `json:"P"`
		TrailingDeltaBips       int               `json:"d"`
		IceBergVolume           string            `json:"F"`
		OrderListId             int64             `json:"g"` // for OCO
		OrigCustomOrderId       string            `json:"C"` // customized order ID for the original order
		ExecutionType           string            `json:"x"` // execution type for this event NEW/TRADE...
		Status                  string            `json:"X"` // order status
		RejectReason            string            `json:"r"`
		Id                      int64             `json:"i"` // order id
		LatestVolume            string            `json:"l"` // quantity for the latest trade
		FilledVolume            string            `json:"z"`
		LatestPrice             string            `json:"L"` // price for the latest trade
		FeeAsset                string            `json:"N"`
		FeeCost                 string            `json:"n"`
		TradeId                 int64             `json:"t"`
		IsInOrderBook           bool              `json:"w"` // is the order in the order book?
		IsMaker                 bool              `json:"m"` // is this order maker?
		IsReduceOnly            bool              `json:"M"` // is this order reduce only?
		CreateTime              int64             `json:"O"`
		FilledQuoteVolume       string            `json:"Z"` // the quote volume that already filled
		LatestQuoteVolume       string            `json:"Y"` // the quote volume for the latest trade
		QuoteVolume             string            `json:"Q"`
		WorkingTime             int64             `json:"W"` // Working Time
		SelfTradePreventionMode string            `json:"V"`
	} `json:"event"`
}

type BalanceUpdate struct {
	Asset  string `json:"a"`
	Change string `json:"d"`
}

// UnmarshalError 自定义错误类型，用于表示 JSON 反序列化时的错误
type UnmarshalError struct {
	InnerError error
	Message    []byte
}

// Error 实现 error 接口的方法，用于返回错误信息
func (e *UnmarshalError) Error() string {
	return fmt.Sprintf("Error unmarshaling: %v, Message: %s", e.InnerError, string(e.Message))
}

// WsUserDataHandler handle WsUserDataEvent
type WsAPIUserDataHandler func(event *WsAPIUserDataEvent)

// WsUserDataHandler handle WsUserDataEvent
type UserDataHandler func(event *UserDataEvent)

func (s *SubscribeUserDataStreamService) Do(handler UserDataHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "userDataStream.subscribe",
	}

	messageCh := make(chan []byte, 100)
	s.websocketAPI.ReqResponseMap.Store(id, messageCh)

	err = s.websocketAPI.SendMessage(payload)
	if err != nil {
		return
	}

	doneCh = make(chan struct{})
	stopCh = make(chan struct{})

	go func() {
		defer s.websocketAPI.ReqResponseMap.Delete(id)
		defer close(doneCh)

		silent := false
		go func() {
			for message := range messageCh {
				recvTime := time.Now()

				var origin = WS
				event, err := parseUserDataJsonRsp(message)
				if err != nil {
					log.Println("Error unmarshaling JSON:", err, "Message:", string(message))
					if !silent {
						errHandler(&UnmarshalError{
							InnerError: err,
							Message:    message,
						})
					}
					return
				}
				event.RecvTime = recvTime
				event.Origin = origin
				handler(event)

			}
			stopCh <- struct{}{}
		}()

		for {
			select {
			case <-stopCh:
				silent = true
				return
			case <-doneCh:
			}
		}
	}()

	return
}

type UserDataEvent struct {
	Origin         string
	RecvTime       time.Time
	Event          UserDataEventType
	AccountUpdates []AccountUpdate
	ExecutionReport
	BalanceChange
	ListStatus
	Terminated
}

type Terminated struct {
	Time int64
}

type ListStatusResponse struct {
	Event ListStatusEvent `json:"event"`
}

type ListStatusEvent struct {
	Event             string `json:"e"`
	Time              int64  `json:"E"`
	TransactionTime   int64  `json:"T"`
	Symbol            string `json:"s"`
	OrderListId       int64  `json:"g"`
	RejectReason      string `json:"r"`
	ContingencyType   string `json:"c"`
	ListStatusType    string `json:"l"`
	ListOrderStatus   string `json:"L"`
	ListClientOrderId string `json:"C"`
	OrderReports      []struct {
		Symbol        string `json:"s"`
		OrderId       int64  `json:"i"`
		ClientOrderId string `json:"c"`
	} `json:"O"`
}

type AlgoOrder struct {
	ClientAlgoID  string
	AlgoID        int64
	AlgoType      string
	OrderType     OrderType
	Symbol        string
	Side          Side
	PositionSide  PositionSide
	TimeInForce   TimeInForce
	Quantity      float64
	Price         string
	OrderStatus   OrderStatus
	OrderID       int64
	AvgPrice      float64
	FilledVolume  float64
	Act           string // 触发后在撮合引擎中实际的订单类型，仅当订单被触发并进入撮合引擎时显示
	TriggerPrice  float64
	StopMode      STPMode
	WorkingType   WorkingType
	PriceMatch    PriceMatch
	ClosePosition bool
	PriceProtect  string
	ReduceOnly    bool
	TriggerTime   int64
	GoodTillDate  int64
	RejectReason  string
}

var ParseUserDataJsonRsp = parseUserDataJsonRsp

func parseUserDataJsonRsp(message []byte) (*UserDataEvent, error) {
	j, err := newJSONv2(message)
	if err != nil {
		return nil, err
	}

	var (
		data      = j.Get("event")
		eventType = UserDataEventType(string(data.GetStringBytes("e")))
		event     = UserDataEvent{Event: eventType}
	)
	switch eventType {
	case UserDataEventTypeListStatus:
		response := new(ListStatusResponse)
		if err := Unmarshal(message, response); err != nil {
			return nil, err
		}
		event.ListStatus = ListStatus{
			Symbol:            response.Event.Symbol,
			OrderListId:       response.Event.OrderListId,
			RejectReason:      response.Event.RejectReason,
			ContingencyType:   response.Event.ContingencyType,
			ListStatusType:    response.Event.ListStatusType,
			ListOrderStatus:   response.Event.ListOrderStatus,
			ListClientOrderId: response.Event.ListClientOrderId,
			EventTime:         response.Event.Time,
			TransactTime:      response.Event.TransactionTime,
		}
		for _, ele := range response.Event.OrderReports {
			event.ListStatus.OrderReports = append(event.ListStatus.OrderReports, orderReport{
				Symbol:        ele.Symbol,
				OrderId:       ele.OrderId,
				ClientOrderId: ele.ClientOrderId,
			})
		}
		return &event, nil
	case UserDataEventTypeExecutionReport:
		orderStatus := OrderStatus(data.GetStringBytes("X"))
		switch orderStatus {
		case PARTIALLY_FILLED, FILLED:
			event.ExecutionReport = ExecutionReport{
				EventTime:               data.GetInt64("E"),
				Symbol:                  string(data.GetStringBytes("s")),
				ClientOrderId:           string(data.GetStringBytes("c")),
				OrigCustomOrderId:       string(data.GetStringBytes("C")),
				Side:                    string(data.GetStringBytes("S")),
				Type:                    string(data.GetStringBytes("o")),
				TimeInForce:             string(data.GetStringBytes("f")),
				Price:                   StringToFloat64(string(data.GetStringBytes("p"))),
				Volume:                  StringToFloat64(string(data.GetStringBytes("q"))),
				Status:                  orderStatus,
				Id:                      data.GetInt64("i"),
				LatestVolume:            StringToFloat64(string(data.GetStringBytes("l"))),
				FilledVolume:            StringToFloat64(string(data.GetStringBytes("z"))),
				LatestPrice:             StringToFloat64(string(data.GetStringBytes("L"))),
				FeeAsset:                string(data.GetStringBytes("N")),
				FeeCost:                 StringToFloat64(string(data.GetStringBytes("n"))),
				TransactionTime:         data.GetInt64("T"),
				TradeId:                 data.GetInt64("t"),
				CreateTime:              data.GetInt64("O"),
				QuoteVolume:             StringToFloat64(string(data.GetStringBytes("Q"))),
				FilledQuoteVolume:       StringToFloat64(string(data.GetStringBytes("Z"))),
				LatestQuoteVolume:       StringToFloat64(string(data.GetStringBytes("Y"))),
				SelfTradePreventionMode: string(data.GetStringBytes("V")),

				// 暂未用到
				// StopPrice:     StringToFloat64(string(data.GetStringBytes("P"))),
				// IceBergVolume: StringToFloat64(string(data.GetStringBytes("F"))),
				// OrderListId:   data.GetInt64("g"),
				// ExecutionType: string(data.GetStringBytes("x")),
				// RejectReason:  string(data.GetStringBytes("r")),
				// IsInOrderBook: data.GetBool("w"),
				// IsMaker:       data.GetBool("m"),
				// IsReduceOnly:  data.GetBool("M"),
				// WorkingTime:   data.GetInt64("W"),
			}
			return &event, nil
		case AlgoUpdate:
			
		default:
			event.ExecutionReport = ExecutionReport{
				Symbol:                  string(data.GetStringBytes("s")),
				ClientOrderId:           string(data.GetStringBytes("c")),
				OrigCustomOrderId:       string(data.GetStringBytes("C")),
				Side:                    string(data.GetStringBytes("S")),
				Type:                    string(data.GetStringBytes("o")),
				TimeInForce:             string(data.GetStringBytes("f")),
				Price:                   StringToFloat64(string(data.GetStringBytes("p"))),
				Volume:                  StringToFloat64(string(data.GetStringBytes("q"))),
				Status:                  orderStatus,
				CreateTime:              data.GetInt64("O"),
				SelfTradePreventionMode: string(data.GetStringBytes("V")),
				RejectReason:            string(data.GetStringBytes("r")),
			}
			return &event, nil
		}
	}

	response := new(UserDataResponse)
	if err := Unmarshal(message, response); err != nil {
		return nil, err
	}
	switch UserDataEventType(eventType) {
	case UserDataEventTypeOutboundAccountPosition:
		for _, update := range response.Event.AccountUpdates {
			event.AccountUpdates = append(event.AccountUpdates, AccountUpdate{
				Asset:      update.Asset,
				Free:       StringToFloat64(update.Free),
				Locked:     StringToFloat64(update.Locked),
				EventTime:  response.Event.Time,
				UpdateTime: response.Event.AccountUpdateTime,
			})
		}
	case UserDataEventTypeBalanceUpdate:
		event.BalanceChange = BalanceChange{
			Asset:     response.Event.BalanceUpdate.Asset,
			Change:    StringToFloat64(response.Event.BalanceUpdate.Change),
			ClearTime: response.Event.TransactionTime,
			EventTime: response.Event.Time,
		}
	}
	return &event, nil
}

type ExecutionReport struct {
	EventTime               int64
	Symbol                  string
	ClientOrderId           string
	Side                    string
	Type                    string
	TimeInForce             string
	Volume                  float64
	Price                   float64
	StopPrice               float64
	IceBergVolume           float64
	OrderListId             int64
	OrigCustomOrderId       string
	ExecutionType           string
	Status                  OrderStatus
	RejectReason            string
	Id                      int64
	LatestVolume            float64
	FilledVolume            float64
	LatestPrice             float64
	FeeAsset                string
	FeeCost                 float64
	TransactionTime         int64
	TradeId                 int64
	IsInOrderBook           bool
	IsMaker                 bool
	IsReduceOnly            bool
	CreateTime              int64
	FilledQuoteVolume       float64
	LatestQuoteVolume       float64
	QuoteVolume             float64
	WorkingTime             int64
	SelfTradePreventionMode string
}

type AccountUpdate struct {
	Asset      string
	Free       float64
	Locked     float64
	EventTime  int64
	UpdateTime int64
}

type BalanceChange struct {
	Asset     string
	Change    float64
	ClearTime int64
	EventTime int64
}

type ListStatus struct {
	EventTime         int64
	TransactTime      int64
	Symbol            string
	OrderListId       int64
	RejectReason      string
	ContingencyType   string
	ListStatusType    string
	ListOrderStatus   string
	ListClientOrderId string
	OrderReports      []orderReport
}

type orderReport struct {
	Symbol        string
	OrderId       int64
	ClientOrderId string
}

type ConditionalOrderReject struct {
	Symbol       string
	OrderID      int64
	RejectReason string
}

type SubscribeUserDataStreamResponse struct {
	ID     string   `json:"id"`
	Status int      `json:"status"`
	Result struct{} `json:"result,omitempty"`
}

type UnsubscribeUserDataStreamService struct {
	websocketAPI *WebsocketAPIClient
}

func (s *UnsubscribeUserDataStreamService) Do(ctx context.Context) (*UnsubscribeUserDataStreamResponse, error) {
	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "userDataStream.unsubscribe",
	}

	messageCh := make(chan []byte)
	s.websocketAPI.ReqResponseMap.Store(id, messageCh)

	err := s.websocketAPI.SendMessage(payload)
	if err != nil {
		return nil, err
	}

	defer s.websocketAPI.ReqResponseMap.Delete(id)

	select {
	case response := <-messageCh:
		var unsubscribeResponse UnsubscribeUserDataStreamResponse
		err = Unmarshal(response, &unsubscribeResponse)
		if err != nil {
			return nil, err
		}
		return &unsubscribeResponse, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type UnsubscribeUserDataStreamResponse struct {
	ID     string   `json:"id"`
	Status int      `json:"status"`
	Result struct{} `json:"result,omitempty"`
}

func StringToFloat64(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}
