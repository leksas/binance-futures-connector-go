package binance_futures_connector

import (
	"context"
	"strconv"

	"github.com/leksas/binance-futures-connector-go/handlers"
)

type OrderPlacementService struct {
	websocketAPI     *WebsocketAPIClient
	symbol           string
	side             Side
	positionSide     *PositionSide
	orderType        OrderType
	reduceOnly       *bool
	price            *float64
	quantity         *float64
	newClientOrderId *string
	stopPrice        *float64
	closePosition    *bool
	activationPrice  *float64
	callbackRate     *float64
	timeInForce      *TimeInForce
	workingType      *WorkingType
	priceProtect     *bool
	newOrderRespType *string
	priceMatch       *PriceMatch
	stpMode          *STPMode
	goodTillDate     *int64
	recvWindow       *int64
}

func (s *OrderPlacementService) Symbol(symbol string) *OrderPlacementService {
	s.symbol = symbol
	return s
}

// 买卖方向 SELL, BUY
func (s *OrderPlacementService) Side(side Side) *OrderPlacementService {
	s.side = side
	return s
}

// 持仓方向，单向持仓模式下非必填，默认且仅可填BOTH;在双向持仓模式下必填,且仅可选择 LONG 或 SHORT
func (s *OrderPlacementService) PositionSide(positionSide PositionSide) *OrderPlacementService {
	s.positionSide = &positionSide
	return s
}

// 订单类型 LIMIT, MARKET, STOP, TAKE_PROFIT, STOP_MARKET, TAKE_PROFIT_MARKET, TRAILING_STOP_MARKET
func (s *OrderPlacementService) OrderType(orderType OrderType) *OrderPlacementService {
	s.orderType = orderType
	return s
}

// true, false; 非双开模式下默认false；双开模式下不接受此参数； 使用closePosition不支持此参数。
func (s *OrderPlacementService) ReduceOnly(reduceOnly bool) *OrderPlacementService {
	s.reduceOnly = &reduceOnly
	return s
}

func (s *OrderPlacementService) Price(price float64) *OrderPlacementService {
	s.price = &price
	return s
}

func (s *OrderPlacementService) Quantity(quantity float64) *OrderPlacementService {
	s.quantity = &quantity
	return s
}

func (s *OrderPlacementService) NewClientOrderId(newClientOrderId string) *OrderPlacementService {
	s.newClientOrderId = &newClientOrderId
	return s
}

// 触发价, 仅 STOP, STOP_MARKET, TAKE_PROFIT, TAKE_PROFIT_MARKET 需要此参数
func (s *OrderPlacementService) StopPrice(stopPrice float64) *OrderPlacementService {
	s.stopPrice = &stopPrice
	return s
}

// true, false；触发后全部平仓，仅支持STOP_MARKET和TAKE_PROFIT_MARKET；不与quantity合用；自带只平仓效果，不与reduceOnly 合用
func (s *OrderPlacementService) ClosePosition(closePosition bool) *OrderPlacementService {
	s.closePosition = &closePosition
	return s
}

// 追踪止损激活价格，仅TRAILING_STOP_MARKET 需要此参数, 默认为下单当前市场价格(支持不同workingType)
func (s *OrderPlacementService) ActivationPrice(activationPrice float64) *OrderPlacementService {
	s.activationPrice = &activationPrice
	return s
}

// 追踪止损回调比例，可取值范围[0.1, 10],其中 1代表1% ,仅TRAILING_STOP_MARKET 需要此参数
func (s *OrderPlacementService) CallbackRate(callbackRate float64) *OrderPlacementService {
	s.callbackRate = &callbackRate
	return s
}

func (s *OrderPlacementService) TimeInForce(timeInForce string) *OrderPlacementService {
	s.timeInForce = &timeInForce
	return s
}

// stopPrice 触发类型: MARK_PRICE(标记价格), CONTRACT_PRICE(合约最新价). 默认 CONTRACT_PRICE
func (s *OrderPlacementService) WorkingType(workingType WorkingType) *OrderPlacementService {
	s.workingType = &workingType
	return s
}

// 条件单触发保护："TRUE","FALSE", 默认"FALSE". 仅 STOP, STOP_MARKET, TAKE_PROFIT, TAKE_PROFIT_MARKET 需要此参数
// 如果订单参数priceProtect为true:
//
//	达到触发价时，MARK_PRICE(标记价格)与CONTRACT_PRICE(合约最新价)之间的价差不能超过改symbol触发保护阈值
//	触发保护阈值请参考接口GET /fapi/v1/exchangeInfo 返回内容相应symbol中"triggerProtect"字段
func (s *OrderPlacementService) PriceProtect(priceProtect bool) *OrderPlacementService {
	s.priceProtect = &priceProtect
	return s
}

// "ACK", "RESULT", 默认 "ACK"
func (s *OrderPlacementService) NewOrderRespType(newOrderRespType string) *OrderPlacementService {
	s.newOrderRespType = &newOrderRespType
	return s
}

func (s *OrderPlacementService) PriceMatch(priceMatch PriceMatch) *OrderPlacementService {
	s.priceMatch = &priceMatch
	return s
}

func (s *OrderPlacementService) StpMode(stpMode STPMode) *OrderPlacementService {
	s.stpMode = &stpMode
	return s
}

// TIF为GTD时订单的自动取消时间， 当timeInforce为GTD时必传；传入的时间戳仅保留秒级精度，毫秒级部分会被自动忽略，时间戳需大于当前时间+600s且小于253402300799000
func (s *OrderPlacementService) GoodTillDate(goodTillDate int64) *OrderPlacementService {
	s.goodTillDate = &goodTillDate
	return s
}

func (s *OrderPlacementService) RecvWindow(recvWindow int64) *OrderPlacementService {
	s.recvWindow = &recvWindow
	return s
}

func (s *OrderPlacementService) Do(ctx context.Context) (*OrderPlacementResponse, error) {
	respType := ACK
	parameters := map[string]string{
		"symbol": s.symbol,
		"side":   s.side,
		"type":   s.orderType,
	}
	switch {
	case s.orderType == Market,
		s.timeInForce != nil && (*s.timeInForce == IOC || *s.timeInForce == FOK):
		respType = RESULT
	}
	if s.positionSide != nil {
		parameters["positionSide"] = *s.positionSide
	}
	if s.reduceOnly != nil {
		parameters["reduceOnly"] = BoolToString(*s.reduceOnly)
	}
	if s.price != nil {
		parameters["price"] = Float64ToString(*s.price)
	}
	if s.quantity != nil {
		parameters["quantity"] = Float64ToString(*s.quantity)
	}
	if s.newClientOrderId != nil {
		parameters["newClientOrderId"] = *s.newClientOrderId
	}
	if s.stopPrice != nil {
		parameters["stopPrice"] = Float64ToString(*s.stopPrice)
	}
	if s.closePosition != nil {
		parameters["closePosition"] = BoolToString(*s.closePosition)
	}
	if s.activationPrice != nil {
		parameters["activationPrice"] = Float64ToString(*s.activationPrice)
	}
	if s.callbackRate != nil {
		parameters["callbackRate"] = Float64ToString(*s.callbackRate)
	}
	if s.timeInForce != nil {
		parameters["timeInForce"] = *s.timeInForce
	}
	if s.workingType != nil {
		parameters["workingType"] = *s.workingType
	}
	if s.priceProtect != nil {
		parameters["priceProtect"] = BoolToString(*s.priceProtect)
	}
	if s.newOrderRespType != nil {
		parameters["newOrderRespType"] = *s.newOrderRespType
	} else {
		parameters["newOrderRespType"] = respType
	}
	if s.priceMatch != nil {
		parameters["priceMatch"] = *s.priceMatch
	}
	if s.stpMode != nil {
		parameters["selfTradePreventionMode"] = *s.stpMode
	}
	if s.goodTillDate != nil {
		parameters["goodTillDate"] = Int64ToString(*s.goodTillDate)
	}
	if s.recvWindow != nil {
		parameters["recvWindow"] = Int64ToString(*s.recvWindow)
	}

	signedParams, err := s.websocketAPI.Sign(parameters)
	if err != nil {
		panic(err)
	}

	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "order.place",
		"params": signedParams,
	}

	messageCh := make(chan []byte)
	s.websocketAPI.ReqResponseMap.Store(id, messageCh)

	err2 := s.websocketAPI.SendMessage(payload)
	if err2 != nil {
		return nil, err2
	}

	defer s.websocketAPI.ReqResponseMap.Delete(id)

	select {
	case response := <-messageCh:
		var rsp OrderPlacementResponse
		err = Unmarshal(response, &rsp)
		if err != nil {
			return nil, err
		}
		if rsp.Status != SuccessCode {
			return nil, handlers.NewAPIError(rsp.Error.Code, rsp.Error.Message)
		}
		return &rsp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type OrderPlacementResponse struct {
	ID         string                `json:"id"`
	Status     int                   `json:"status"`
	Error      *WsAPIErrorResponse   `json:"error,omitempty"`
	Result     *OrderPlacementResult `json:"result,omitempty"`
	RateLimits []*WsAPIRateLimit     `json:"rateLimits"`
}

type OrderPlacementResult struct {
	OrderID       int64        `json:"orderId,omitempty"`
	Symbol        string       `json:"symbol,omitempty"`
	Status        OrderStatus  `json:"status,omitempty"`
	ClientOrderID string       `json:"clientOrderId,omitempty"`
	Price         string       `json:"price,omitempty"`
	AvgPrice      string       `json:"avgPrice,omitempty"`
	OrigQty       string       `json:"origQty,omitempty"`
	ExecutedQty   string       `json:"executedQty,omitempty"`
	CumQty        string       `json:"cumQty,omitempty"`
	CumQuote      string       `json:"cumQuote,omitempty"`
	TimeInForce   TimeInForce  `json:"timeInForce,omitempty"`
	OrderType     OrderType    `json:"type,omitempty"`
	ReduceOnly    bool         `json:"reduceOnly,omitempty"`
	ClosePosition bool         `json:"closePosition,omitempty"`
	Side          Side         `json:"side,omitempty"`
	PositionSide  PositionSide `json:"positionSide,omitempty"`
	StopPrice     string       `json:"stopPrice,omitempty"`
	WorkingType   WorkingType  `json:"workingType,omitempty"`
	PriceProtect  bool         `json:"priceProtect,omitempty"`
	OrigType      OrderType    `json:"origType,omitempty"`
	PriceMatch    PriceMatch   `json:"priceMatch,omitempty"`
	STPMode       STPMode      `json:"selfTradePreventionMode,omitempty"`
	GoodTillDate  int64        `json:"goodTillDate,omitempty"`
	UpdateTime    int64        `json:"updateTime,omitempty"`
}

type OrderFill struct {
	Price           string `json:"price"`
	Qty             string `json:"qty"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
	TradeID         int64  `json:"tradeId"`
}

type OrderModifyService struct {
	websocketAPI      *WebsocketAPIClient
	orderId           *int64
	origClientOrderId *string
	symbol            string
	side              Side
	quantity          float64
	price             float64
	priceMatch        *PriceMatch
	recvWindow        *int64
}

func (s *OrderModifyService) OrderId(orderId int64) *OrderModifyService {
	s.orderId = &orderId
	return s
}

func (s *OrderModifyService) OrigClientOrderId(origClientOrderId string) *OrderModifyService {
	s.origClientOrderId = &origClientOrderId
	return s
}

func (s *OrderModifyService) Symbol(symbol string) *OrderModifyService {
	s.symbol = symbol
	return s
}

func (s *OrderModifyService) Side(side Side) *OrderModifyService {
	s.side = side
	return s
}

func (s *OrderModifyService) Quantity(quantity float64) *OrderModifyService {
	s.quantity = quantity
	return s
}

func (s *OrderModifyService) Price(price float64) *OrderModifyService {
	s.price = price
	return s
}

func (s *OrderModifyService) PriceMatch(priceMatch *PriceMatch) *OrderModifyService {
	s.priceMatch = priceMatch
	return s
}

func (s *OrderModifyService) RecvWindow(recvWindow *int64) *OrderModifyService {
	s.recvWindow = recvWindow
	return s
}

func (s *OrderModifyService) Do(ctx context.Context) (*OrderPlacementResponse, error) {
	parameters := map[string]string{
		"symbol":   s.symbol,
		"side":     s.side,
		"quantity": Float64ToString(s.quantity),
		"price":    Float64ToString(s.price),
	}
	if s.orderId != nil {
		parameters["orderId"] = Int64ToString(*s.orderId)
	}
	if s.origClientOrderId != nil {
		parameters["origClientOrderId"] = *s.origClientOrderId
	}
	if s.priceMatch != nil {
		parameters["priceMatch"] = *s.priceMatch
	}
	if s.recvWindow != nil {
		parameters["recvWindow"] = Int64ToString(*s.recvWindow)
	}

	signedParams, err := s.websocketAPI.Sign(parameters)
	if err != nil {
		panic(err)
	}

	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "order.modify",
		"params": signedParams,
	}

	messageCh := make(chan []byte)
	s.websocketAPI.ReqResponseMap.Store(id, messageCh)

	err2 := s.websocketAPI.SendMessage(payload)
	if err2 != nil {
		return nil, err2
	}

	defer s.websocketAPI.ReqResponseMap.Delete(id)

	select {
	case response := <-messageCh:
		var rsp OrderPlacementResponse
		err = Unmarshal(response, &rsp)
		if err != nil {
			return nil, err
		}
		if rsp.Status != SuccessCode {
			return nil, handlers.NewAPIError(rsp.Error.Code, rsp.Error.Message)
		}
		return &rsp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type OrderCancelService struct {
	websocketAPI      *WebsocketAPIClient
	symbol            string
	orderId           *int64
	origClientOrderId *string
	recvWindow        *int64
}

func (s *OrderCancelService) Symbol(symbol string) *OrderCancelService {
	s.symbol = symbol
	return s
}

func (s *OrderCancelService) OrderId(orderId int64) *OrderCancelService {
	s.orderId = &orderId
	return s
}

func (s *OrderCancelService) OrigClientOrderId(origClientOrderId string) *OrderCancelService {
	s.origClientOrderId = &origClientOrderId
	return s
}

func (s *OrderCancelService) RecvWindow(recvWindow int64) *OrderCancelService {
	s.recvWindow = &recvWindow
	return s
}

func (s *OrderCancelService) Do(ctx context.Context) (response *OrderPlacementResponse, err error) {
	parameters := map[string]string{
		"symbol": s.symbol,
	}
	if s.orderId != nil {
		parameters["orderId"] = Int64ToString(*s.orderId)
	}
	if s.origClientOrderId != nil {
		parameters["origClientOrderId"] = *s.origClientOrderId
	}
	if s.recvWindow != nil {
		parameters["recvWindow"] = Int64ToString(*s.recvWindow)
	}

	signedParams, err := s.websocketAPI.Sign(parameters)
	if err != nil {
		panic(err)
	}

	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "order.cancel",
		"params": signedParams,
	}

	messageCh := make(chan []byte)
	s.websocketAPI.ReqResponseMap.Store(id, messageCh)

	err2 := s.websocketAPI.SendMessage(payload)
	if err2 != nil {
		return nil, err2
	}

	defer s.websocketAPI.ReqResponseMap.Delete(id)

	select {
	case response := <-messageCh:
		var rsp OrderPlacementResponse
		err = Unmarshal(response, &rsp)
		if err != nil {
			return nil, err
		}
		if rsp.Status != SuccessCode {
			return nil, handlers.NewAPIError(rsp.Error.Code, rsp.Error.Message)
		}
		return &rsp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *OrderCancelService) Send() {
	parameters := map[string]string{
		"symbol": s.symbol,
	}
	if s.orderId != nil {
		parameters["orderId"] = Int64ToString(*s.orderId)
	}
	if s.origClientOrderId != nil {
		parameters["origClientOrderId"] = *s.origClientOrderId
	}
	if s.recvWindow != nil {
		parameters["recvWindow"] = Int64ToString(*s.recvWindow)
	}

	signedParams, err := s.websocketAPI.Sign(parameters)
	if err != nil {
		panic(err)
	}

	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "order.cancel",
		"params": signedParams,
	}

	_ = s.websocketAPI.SendMessage(payload)
}

type OrderStatusService struct {
	websocketAPI      *WebsocketAPIClient
	symbol            string
	orderId           *int64
	origClientOrderId *string
	recvWindow        *int64
}

func (s *OrderStatusService) Symbol(symbol string) *OrderStatusService {
	s.symbol = symbol
	return s
}

func (s *OrderStatusService) OrderId(orderId int64) *OrderStatusService {
	s.orderId = &orderId
	return s
}

func (s *OrderStatusService) OrigClientOrderId(origClientOrderId string) *OrderStatusService {
	s.origClientOrderId = &origClientOrderId
	return s
}

func (s *OrderStatusService) RecvWindow(recvWindow int64) *OrderStatusService {
	s.recvWindow = &recvWindow
	return s
}

func (s *OrderStatusService) Do(ctx context.Context, opts ...RequestOption) (res *OrderPlacementResponse, err error) {
	parameters := map[string]string{
		"symbol": s.symbol,
	}
	if s.orderId != nil {
		parameters["orderId"] = Int64ToString(*s.orderId)
	}
	if s.origClientOrderId != nil {
		parameters["origClientOrderId"] = *s.origClientOrderId
	}
	if s.recvWindow != nil {
		parameters["recvWindow"] = Int64ToString(*s.recvWindow)
	}

	signedParams, err := s.websocketAPI.Sign(parameters)
	if err != nil {
		panic(err)
	}

	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "order.status",
		"params": signedParams,
	}

	messageCh := make(chan []byte)
	s.websocketAPI.ReqResponseMap.Store(id, messageCh)

	err2 := s.websocketAPI.SendMessage(payload)
	if err2 != nil {
		return nil, err2
	}

	defer s.websocketAPI.ReqResponseMap.Delete(id)

	select {
	case response := <-messageCh:
		var rsp OrderPlacementResponse
		err = Unmarshal(response, &rsp)
		if err != nil {
			return nil, err
		}
		if rsp.Status != SuccessCode {
			return nil, handlers.NewAPIError(rsp.Error.Code, rsp.Error.Message)
		}
		return &rsp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type AccountPositionService struct {
	websocketAPI *WebsocketAPIClient
	symbol       *string
	recvWindow   *int64
}

func (s *AccountPositionService) Symbol(symbol string) *AccountPositionService {
	s.symbol = &symbol
	return s
}

func (s *AccountPositionService) RecvWindow(recvWindow int64) *AccountPositionService {
	s.recvWindow = &recvWindow
	return s
}

func (s *AccountPositionService) Do(ctx context.Context, opts ...RequestOption) (res *AccountPositionResponse, err error) {
	parameters := make(map[string]string, 0)
	if s.symbol != nil {
		parameters["symbol"] = *s.symbol
	}
	if s.recvWindow != nil {
		parameters["recvWindow"] = Int64ToString(*s.recvWindow)
	}

	signedParams, err := s.websocketAPI.Sign(parameters)
	if err != nil {
		panic(err)
	}

	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "v2/account.position",
		"params": signedParams,
	}

	messageCh := make(chan []byte)
	s.websocketAPI.ReqResponseMap.Store(id, messageCh)

	err2 := s.websocketAPI.SendMessage(payload)
	if err2 != nil {
		return nil, err2
	}

	defer s.websocketAPI.ReqResponseMap.Delete(id)

	select {
	case response := <-messageCh:
		var rsp AccountPositionResponse
		err = Unmarshal(response, &rsp)
		if err != nil {
			return nil, err
		}
		return &rsp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type AccountPositionResponse struct {
	ID         string            `json:"id"`
	Status     int               `json:"status"`
	Result     []PositionResult  `json:"result"`
	RateLimits []*WsAPIRateLimit `json:"rateLimits"`
}

type PositionResult struct {
	Symbol                 string       `json:"symbol"`
	PositionSide           PositionSide `json:"positionSide"`
	PositionAmt            string       `json:"positionAmt"`
	EntryPrice             string       `json:"entryPrice"`
	BreakEvenPrice         string       `json:"breakEvenPrice"`
	MarkPrice              string       `json:"markPrice"`
	UnRealizedProfit       string       `json:"unRealizedProfit"`
	LiquidationPrice       string       `json:"liquidationPrice"`
	IsolatedMargin         string       `json:"isolatedMargin"`
	Notional               string       `json:"notional"`
	MarginAsset            string       `json:"marginAsset"`
	IsolatedWallet         string       `json:"isolatedWallet"`
	InitialMargin          string       `json:"initialMargin"`          // 初始保证金
	MaintMargin            string       `json:"maintMargin"`            // 维持保证金
	PositionInitialMargin  string       `json:"positionInitialMargin"`  // 仓位初始保证金
	OpenOrderInitialMargin string       `json:"openOrderInitialMargin"` // 订单初始保证金
	ADL                    int          `json:"adl"`
	BidNotional            string       `json:"bidNotional"`
	AskNotional            string       `json:"askNotional"`
	UpdateTime             int64        `json:"updateTime"`
}

func Float64ToString(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func Int64ToString(i int64) string {
	return strconv.FormatInt(i, 10)
}

func BoolToString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

type AlgoOrderPlacementService struct {
	websocketAPI     *WebsocketAPIClient
	algoType         string
	symbol           string
	side             Side
	positionSide     *PositionSide
	orderType        OrderType
	timeInForce      *TimeInForce
	quantity         *float64
	price            *float64
	triggerPrice     *float64
	workingType      *WorkingType
	priceMatch       *string
	closePosition    *bool
	priceProtect     *string
	reduceOnly       *bool
	activatePrice    *float64
	callbackRate     *float64
	clientAlgoID     *string
	newOrderRespType *OrderRespType
	stpMode          *STPMode
	goodTillDate     *int64
	recvWindow       *int64
}

func (s *AlgoOrderPlacementService) AlgoType(algoType string) *AlgoOrderPlacementService {
	s.algoType = algoType
	return s
}

func (s *AlgoOrderPlacementService) Symbol(symbol string) *AlgoOrderPlacementService {
	s.symbol = symbol
	return s
}

func (s *AlgoOrderPlacementService) Side(side Side) *AlgoOrderPlacementService {
	s.side = side
	return s
}

func (s *AlgoOrderPlacementService) PositionSide(positionSide PositionSide) *AlgoOrderPlacementService {
	s.positionSide = &positionSide
	return s
}

func (s *AlgoOrderPlacementService) OrderType(orderType OrderType) *AlgoOrderPlacementService {
	s.orderType = orderType
	return s
}

func (s *AlgoOrderPlacementService) TimeInForce(timeInForce TimeInForce) *AlgoOrderPlacementService {
	s.timeInForce = &timeInForce
	return s
}

func (s *AlgoOrderPlacementService) Quantity(quantity float64) *AlgoOrderPlacementService {
	s.quantity = &quantity
	return s
}

func (s *AlgoOrderPlacementService) Price(price float64) *AlgoOrderPlacementService {
	s.price = &price
	return s
}

func (s *AlgoOrderPlacementService) TriggerPrice(triggerPrice float64) *AlgoOrderPlacementService {
	s.triggerPrice = &triggerPrice
	return s
}

func (s *AlgoOrderPlacementService) WorkingType(workingType WorkingType) *AlgoOrderPlacementService {
	s.workingType = &workingType
	return s
}

func (s *AlgoOrderPlacementService) PriceMatch(priceMatch string) *AlgoOrderPlacementService {
	s.priceMatch = &priceMatch
	return s
}

func (s *AlgoOrderPlacementService) ClosePosition(closePosition bool) *AlgoOrderPlacementService {
	s.closePosition = &closePosition
	return s
}

func (s *AlgoOrderPlacementService) PriceProtect(priceProtect string) *AlgoOrderPlacementService {
	s.priceProtect = &priceProtect
	return s
}

func (s *AlgoOrderPlacementService) ReduceOnly(reduceOnly bool) *AlgoOrderPlacementService {
	s.reduceOnly = &reduceOnly
	return s
}

func (s *AlgoOrderPlacementService) ActivatePrice(activatePrice float64) *AlgoOrderPlacementService {
	s.activatePrice = &activatePrice
	return s
}

func (s *AlgoOrderPlacementService) CallbackRate(callbackRate float64) *AlgoOrderPlacementService {
	s.callbackRate = &callbackRate
	return s
}

func (s *AlgoOrderPlacementService) ClientAlgoID(clientAlgoID string) *AlgoOrderPlacementService {
	s.clientAlgoID = &clientAlgoID
	return s
}

func (s *AlgoOrderPlacementService) NewOrderRespType(newOrderRespType OrderRespType) *AlgoOrderPlacementService {
	s.newOrderRespType = &newOrderRespType
	return s
}

func (s *AlgoOrderPlacementService) STPMode(stpMode STPMode) *AlgoOrderPlacementService {
	s.stpMode = &stpMode
	return s
}

func (s *AlgoOrderPlacementService) GoodTillDate(goodTillDate int64) *AlgoOrderPlacementService {
	s.goodTillDate = &goodTillDate
	return s
}

func (s *AlgoOrderPlacementService) RecvWindow(recvWindow int64) *AlgoOrderPlacementService {
	s.recvWindow = &recvWindow
	return s
}

func (s *AlgoOrderPlacementService) Do(ctx context.Context) (*AlgoOrderPlacementResponse, error) {
	respType := ACK
	parameters := map[string]string{
		"algoType": s.algoType,
		"symbol":   s.symbol,
		"side":     s.side,
		"type":     s.orderType,
	}
	if s.positionSide != nil {
		parameters["positionSide"] = *s.positionSide
	}
	if s.timeInForce != nil {
		parameters["timeInForce"] = *s.timeInForce
	}
	if s.price != nil {
		parameters["price"] = Float64ToString(*s.price)
	}
	if s.quantity != nil {
		parameters["quantity"] = Float64ToString(*s.quantity)
	}
	if s.triggerPrice != nil {
		parameters["triggerPrice"] = Float64ToString(*s.triggerPrice)
	}
	if s.workingType != nil {
		parameters["workingType"] = *s.workingType
	}
	if s.priceMatch != nil {
		parameters["priceMatch"] = *s.priceMatch
	}
	if s.closePosition != nil {
		parameters["closePosition"] = BoolToString(*s.closePosition)
	}
	if s.priceProtect != nil {
		parameters["priceProtect"] = *s.priceProtect
	}
	if s.reduceOnly != nil {
		parameters["reduceOnly"] = BoolToString(*s.reduceOnly)
	}
	if s.activatePrice != nil {
		parameters["activatePrice"] = Float64ToString(*s.activatePrice)
	}
	if s.callbackRate != nil {
		parameters["callbackRate"] = Float64ToString(*s.callbackRate)
	}
	if s.clientAlgoID != nil {
		parameters["clientAlgoId"] = *s.clientAlgoID
	}
	if s.newOrderRespType != nil {
		parameters["newOrderRespType"] = *s.newOrderRespType
	} else {
		parameters["newOrderRespType"] = respType
	}
	if s.stpMode != nil {
		parameters["selfTradePreventionMode"] = *s.stpMode
	}
	if s.goodTillDate != nil {
		parameters["goodTillDate"] = Int64ToString(*s.goodTillDate)
	}
	if s.recvWindow != nil {
		parameters["recvWindow"] = Int64ToString(*s.recvWindow)
	}

	signedParams, err := s.websocketAPI.Sign(parameters)
	if err != nil {
		panic(err)
	}

	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "algoOrder.place",
		"params": signedParams,
	}

	messageCh := make(chan []byte)
	s.websocketAPI.ReqResponseMap.Store(id, messageCh)

	err2 := s.websocketAPI.SendMessage(payload)
	if err2 != nil {
		return nil, err2
	}

	defer s.websocketAPI.ReqResponseMap.Delete(id)

	select {
	case response := <-messageCh:
		var rsp AlgoOrderPlacementResponse
		err = Unmarshal(response, &rsp)
		if err != nil {
			return nil, err
		}
		if rsp.Status != SuccessCode {
			return nil, handlers.NewAPIError(rsp.Error.Code, rsp.Error.Message)
		}
		return &rsp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type AlgoOrderPlacementResponse struct {
	ID         string                    `json:"id"`
	Status     int                       `json:"status"`
	Error      *WsAPIErrorResponse       `json:"error,omitempty"`
	Result     *AlgoOrderPlacementResult `json:"result"`
	RateLimits []*WsAPIRateLimit         `json:"rateLimits,omitempty"`
}

type AlgoOrderPlacementResult struct {
	AlgoID          int64        `json:"algoId"`
	ClientAlgoID    string       `json:"clientAlgoId"`
	AlgoType        string       `json:"algoType"`
	OrderType       OrderType    `json:"orderType"`
	Symbol          string       `json:"symbol"`
	Side            Side         `json:"side"`
	PositionSide    PositionSide `json:"positionSide"`
	TimeInForce     TimeInForce  `json:"timeInForce"`
	Quantity        string       `json:"quantity"`
	AlgoStatus      string       `json:"algoStatus"`
	TriggerPrice    string       `json:"triggerPrice"`
	Price           string       `json:"price"`
	IcebergQuantity string       `json:"icebergQuantity"`
	STPMode         STPMode      `json:"selfTradePreventionMode"`
	WorkingType     WorkingType  `json:"workingType"`
	PriceMatch      string       `json:"priceMatch"`
	ClosePosition   bool         `json:"closePosition"`
	PriceProtect    bool         `json:"priceProtect"`
	ReduceOnly      bool         `json:"reduceOnly"`
	CreateTime      int64        `json:"createTime"`
	UpdateTime      int64        `json:"updateTime"`
	TriggerTime     int64        `json:"triggerTime"`
	GoodTillDate    int64        `json:"goodTillDate"`
}

type AlgoOrderCancelService struct {
	websocketAPI *WebsocketAPIClient
	algoID       *int64
	clientAlgoID *string
	recvWindow   *int64
}

func (s *AlgoOrderCancelService) AlgoID(algoID int64) *AlgoOrderCancelService {
	s.algoID = &algoID
	return s
}

func (s *AlgoOrderCancelService) ClientAlgoID(clientAlgoID string) *AlgoOrderCancelService {
	s.clientAlgoID = &clientAlgoID
	return s
}

func (s *AlgoOrderCancelService) RecvWindow(recvWindow int64) *AlgoOrderCancelService {
	s.recvWindow = &recvWindow
	return s
}

func (s *AlgoOrderCancelService) Do(ctx context.Context) (*AlgoOrderPlacementResponse, error) {
	parameters := make(map[string]string, 0)
	if s.algoID != nil {
		parameters["algoId"] = Int64ToString(*s.algoID)
	}
	if s.clientAlgoID != nil {
		parameters["clientAlgoId"] = *s.clientAlgoID
	}
	if s.recvWindow != nil {
		parameters["recvWindow"] = Int64ToString(*s.recvWindow)
	}

	signedParams, err := s.websocketAPI.Sign(parameters)
	if err != nil {
		panic(err)
	}

	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "algoOrder.cancel",
		"params": signedParams,
	}

	messageCh := make(chan []byte)
	s.websocketAPI.ReqResponseMap.Store(id, messageCh)

	err2 := s.websocketAPI.SendMessage(payload)
	if err2 != nil {
		return nil, err2
	}

	defer s.websocketAPI.ReqResponseMap.Delete(id)

	select {
	case response := <-messageCh:
		var rsp AlgoOrderPlacementResponse
		err = Unmarshal(response, &rsp)
		if err != nil {
			return nil, err
		}
		if rsp.Status != SuccessCode {
			return nil, handlers.NewAPIError(rsp.Error.Code, rsp.Error.Message)
		}
		return &rsp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
