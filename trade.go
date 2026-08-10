package binance_futures_connector

import (
	"context"
	"net/http"
)

// Binance New Order endpoint (POST /fapi/v1/order)
// CreateOrderService create order
type CreateOrderService struct {
	c                *Client
	symbol           string
	side             Side
	positionSide     *PositionSide
	orderType        OrderType
	reduceOnly       *string
	quantity         *float64
	price            *float64
	newClientOrderId *string
	stopPrice        *float64
	closePosition    *string
	activationPrice  *float64
	callbackRate     *float64
	timeInForce      *TimeInForce
	workingType      *WorkingType
	priceProtect     *string
	newOrderRespType *OrderRespType
	priceMatch       *PriceMatch
	stpMode          *STPMode
	goodTillDate     *int64
}

// Symbol set symbol
func (s *CreateOrderService) Symbol(symbol string) *CreateOrderService {
	s.symbol = symbol
	return s
}

// Side set side
func (s *CreateOrderService) Side(side Side) *CreateOrderService {
	s.side = side
	return s
}

// PositionSide set position side
func (s *CreateOrderService) PositionSide(positionSide PositionSide) *CreateOrderService {
	s.positionSide = &positionSide
	return s
}

// OrderType set order type
func (s *CreateOrderService) OrderType(orderType OrderType) *CreateOrderService {
	s.orderType = orderType
	return s
}

// ReduceOnly set reduce only
func (s *CreateOrderService) ReduceOnly(reduceOnly string) *CreateOrderService {
	s.reduceOnly = &reduceOnly
	return s
}

// Quantity set quantity
func (s *CreateOrderService) Quantity(quantity float64) *CreateOrderService {
	s.quantity = &quantity
	return s
}

// Price set price
func (s *CreateOrderService) Price(price float64) *CreateOrderService {
	s.price = &price
	return s
}

// NewClientOrderId set new client order id
func (s *CreateOrderService) NewClientOrderId(newClientOrderId string) *CreateOrderService {
	s.newClientOrderId = &newClientOrderId
	return s
}

// StopPrice set stop price
func (s *CreateOrderService) StopPrice(stopPrice float64) *CreateOrderService {
	s.stopPrice = &stopPrice
	return s
}

// ClosePosition set close position
func (s *CreateOrderService) ClosePosition(closePosition string) *CreateOrderService {
	s.closePosition = &closePosition
	return s
}

// ActivationPrice set activation price
func (s *CreateOrderService) ActivationPrice(activationPrice float64) *CreateOrderService {
	s.activationPrice = &activationPrice
	return s
}

// TimeInForce set time in force
func (s *CreateOrderService) TimeInForce(timeInForce TimeInForce) *CreateOrderService {
	s.timeInForce = &timeInForce
	return s
}

// WorkingType set working type
func (s *CreateOrderService) WorkingType(workingType WorkingType) *CreateOrderService {
	s.workingType = &workingType
	return s
}

// PriceProtect set price protect
func (s *CreateOrderService) PriceProtect(priceProtect string) *CreateOrderService {
	s.priceProtect = &priceProtect
	return s
}

// NewOrderRespType set new order response type
func (s *CreateOrderService) NewOrderRespType(newOrderRespType OrderRespType) *CreateOrderService {
	s.newOrderRespType = &newOrderRespType
	return s
}

// PriceMatch set price match
func (s *CreateOrderService) PriceMatch(priceMatch PriceMatch) *CreateOrderService {
	s.priceMatch = &priceMatch
	return s
}

// SelfTradePreventionMode set self trade prevention mode
func (s *CreateOrderService) STPMode(stpMode STPMode) *CreateOrderService {
	s.stpMode = &stpMode
	return s
}

// GoodTillDate set good till date
func (s *CreateOrderService) GoodTillDate(goodTillDate int64) *CreateOrderService {
	s.goodTillDate = &goodTillDate
	return s
}

// Do send request
func (s *CreateOrderService) Do(ctx context.Context, opts ...RequestOption) (res interface{}, err error) {
	respType := ACK
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/order",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
		"side":   s.side,
		"type":   s.orderType,
	}
	switch s.orderType {
	case "MARKET":
		respType = FULL
	case "LIMIT":
		respType = FULL
	}
	if s.positionSide != nil {
		m["positionSide"] = *s.positionSide
	}
	if s.reduceOnly != nil {
		m["reduceOnly"] = *s.reduceOnly
	}
	if s.quantity != nil {
		m["quantity"] = *s.quantity
	}
	if s.price != nil {
		m["price"] = *s.price
	}
	if s.newClientOrderId != nil {
		m["newClientOrderId"] = *s.newClientOrderId
	}
	if s.stopPrice != nil {
		m["stopPrice"] = *s.stopPrice
	}
	if s.closePosition != nil {
		m["closePosition"] = *s.closePosition
	}
	if s.activationPrice != nil {
		m["activationPrice"] = *s.activationPrice
	}
	if s.callbackRate != nil {
		m["callbackRate"] = *s.callbackRate
	}
	if s.timeInForce != nil {
		m["timeInForce"] = *s.timeInForce
	}
	if s.workingType != nil {
		m["workingType"] = *s.workingType
	}
	if s.priceProtect != nil {
		m["priceProtect"] = *s.priceProtect
	}
	if s.stpMode != nil {
		m["selfTradePreventionMode"] = *s.stpMode
	}
	if s.goodTillDate != nil {
		m["goodTillDate"] = *s.goodTillDate
	}
	if s.newOrderRespType != nil {
		m["newOrderRespType"] = *s.newOrderRespType
		switch *s.newOrderRespType {
		case "ACK":
			respType = ACK
		case "RESULT":
			respType = RESULT
		case "FULL":
			respType = FULL
		}
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	switch respType {
	case ACK:
		res = new(CreateOrderResponseACK)
	case RESULT:
		res = new(CreateOrderResponseRESULT)
	case FULL:
		res = new(CreateOrderResponseFULL)
	}
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Create CreateOrderResponseACK
type CreateOrderResponseACK struct {
	Symbol        string `json:"symbol"`
	OrderId       int64  `json:"orderId"`
	ClientOrderId string `json:"clientOrderId"`
	UpdateTime    uint64 `json:"updateTime"`
}

// Create CreateOrderResponseRESULT
type CreateOrderResponseRESULT struct {
	Symbol        string       `json:"symbol"`        // 交易对
	OrderId       int64        `json:"orderId"`       // 系统订单号
	ClientOrderId string       `json:"clientOrderId"` // 用户自定义的订单号
	Price         string       `json:"price"`
	OrigQty       string       `json:"origQty"`
	ExecutedQty   string       `json:"executedQty"` // 成交量
	CumQty        string       `json:"cumQty"`
	CumQuote      string       `json:"cumQuote"`            // 成交金额
	Status        string       `json:"status"`              // 订单状态
	TimeInForce   TimeInForce  `json:"timeInForce"`         // 有效方法
	Type          OrderType    `json:"type"`                // 订单类型
	OrigType      string       `json:"origType"`            // 触发前订单类型
	Side          Side         `json:"side"`                // 买卖方向
	PositionSide  PositionSide `json:"positionSide"`        // 持仓方向
	StopPrice     string       `json:"stopPrice,omitempty"` // 触发价，对`TRAILING_STOP_MARKET`无效
	AvgPrice      string       `json:"avgPrice"`
	ReduceOnly    bool         `json:"reduceOnly"`              // 仅减仓
	ClosePosition bool         `json:"closePosition"`           // 是否条件全平仓
	ActivatePrice string       `json:"activatePrice,omitempty"` // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
	PriceRate     string       `json:"priceRate,omitempty"`     // 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
	UpdateTime    int64        `json:"updateTime"`              // 更新时间
	WorkingType   WorkingType  `json:"workingType"`             // 条件价格触发类型
	PriceProtect  bool         `json:"priceProtect"`            // 是否开启条件单触发保护
	PriceMatch    PriceMatch   `json:"priceMatch"`              // 盘口价格下单模式
	StpMode       STPMode      `json:"selfTradePreventionMode"` // 订单自成交保护模式
	GoodTillDate  int64        `json:"goodTillDate"`            // 订单TIF为GTD时的自动取消时间
}

// Create CreateOrderResponseFULL
type CreateOrderResponseFULL struct {
	Symbol        string       `json:"symbol"`        // 交易对
	OrderId       int64        `json:"orderId"`       // 系统订单号
	ClientOrderId string       `json:"clientOrderId"` // 用户自定义的订单号
	Price         string       `json:"price"`
	OrigQty       string       `json:"origQty"`
	ExecutedQty   string       `json:"executedQty"` // 成交量
	CumQty        string       `json:"cumQty"`
	CumQuote      string       `json:"cumQuote"`            // 成交金额
	Status        string       `json:"status"`              // 订单状态
	TimeInForce   TimeInForce  `json:"timeInForce"`         // 有效方法
	Type          OrderType    `json:"type"`                // 订单类型
	OrigType      string       `json:"origType"`            // 触发前订单类型
	Side          Side         `json:"side"`                // 买卖方向
	PositionSide  PositionSide `json:"positionSide"`        // 持仓方向
	StopPrice     string       `json:"stopPrice,omitempty"` // 触发价，对`TRAILING_STOP_MARKET`无效
	AvgPrice      string       `json:"avgPrice"`
	ReduceOnly    bool         `json:"reduceOnly"`              // 仅减仓
	ClosePosition bool         `json:"closePosition"`           // 是否条件全平仓
	ActivatePrice string       `json:"activatePrice,omitempty"` // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
	PriceRate     string       `json:"priceRate,omitempty"`     // 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
	UpdateTime    int64        `json:"updateTime"`              // 更新时间
	WorkingType   WorkingType  `json:"workingType"`             // 条件价格触发类型
	PriceProtect  bool         `json:"priceProtect"`            // 是否开启条件单触发保护
	PriceMatch    PriceMatch   `json:"priceMatch"`              // 盘口价格下单模式
	StpMode       STPMode      `json:"selfTradePreventionMode"` // 订单自成交保护模式
	GoodTillDate  int64        `json:"goodTillDate"`            // 订单TIF为GTD时的自动取消时间
	Code          int          `json:"code"`
	Msg           string       `json:"msg"`
}

// Binance Batch Create Order endpoint (POST /fapi/v1/batchOrders)
type BatchCreateOrder struct {
	c           *Client
	batchOrders []Order
}

type Order struct {
	OrderId           int64         `json:"orderId,omitempty"`           // Modify
	OrigClientOrderId string        `json:"origClientOrderId,omitempty"` // Modify
	Symbol            string        `json:"symbol"`                      // Required
	Side              Side          `json:"side"`                        // Required
	PositionSide      PositionSide  `json:"positionSide,omitempty"`
	OrderType         OrderType     `json:"type"`
	ReduceOnly        string        `json:"reduceOnly,omitempty"`
	Price             string        `json:"price"`    // Required
	Quantity          string        `json:"quantity"` // Required
	NewClientOrderId  string        `json:"newClientOrderId,omitempty"`
	StopPrice         float64       `json:"stopPrice,omitempty"` // Modify
	ActivationPrice   float64       `json:"activationPrice,omitempty"`
	CallbackRate      float64       `json:"callbackRate,omitempty"`
	TimeInForce       TimeInForce   `json:"timeInForce,omitempty"`
	WorkingType       WorkingType   `json:"workingType,omitempty"`
	PriceProtect      string        `json:"priceProtect,omitempty"`
	NewOrderRespType  OrderRespType `json:"newOrderRespType,omitempty"`
	PriceMatch        PriceMatch    `json:"priceMatch,omitempty"` // Modify
	StpMode           STPMode       `json:"selfTradePreventionMode,omitempty"`
	GoodTillDate      int64         `json:"goodTillDate,omitempty"`
}

func (s *BatchCreateOrder) AddOrder(order Order) *BatchCreateOrder {
	if s.batchOrders == nil {
		s.batchOrders = make([]Order, 0)
	}
	s.batchOrders = append(s.batchOrders, order)
	return s
}

func (s *BatchCreateOrder) Do(ctx context.Context, opts ...RequestOption) (res []CreateOrderResponseFULL, err error) {
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/batchOrders",
		secType:  secTypeSigned,
	}
	orders, err := Marshal(s.batchOrders)
	if err == nil {
		r.setParam("batchOrders", string(orders))
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Binance Modify Order endpoint (PUT /fapi/v1/order)
// ModifyOrder modify order
type ModifyOrder struct {
	c                 *Client
	orderId           *int64
	origClientOrderId *string
	symbol            string
	side              Side
	quantity          float64
	price             float64
	priceMatch        *string
}

// OrderId set orderId
func (s *ModifyOrder) OrderId(orderId int64) *ModifyOrder {
	s.orderId = &orderId
	return s
}

// OrigClientOrderId set origClientOrderId
func (s *ModifyOrder) OrigClientOrderId(origClientOrderId string) *ModifyOrder {
	s.origClientOrderId = &origClientOrderId
	return s
}

// Symbol set symbol
func (s *ModifyOrder) Symbol(symbol string) *ModifyOrder {
	s.symbol = symbol
	return s
}

// Side set side
func (s *ModifyOrder) Side(side Side) *ModifyOrder {
	s.side = side
	return s
}

// Quantity set quantity
func (s *ModifyOrder) Quantity(quantity float64) *ModifyOrder {
	s.quantity = quantity
	return s
}

// Price set price
func (s *ModifyOrder) Price(price float64) *ModifyOrder {
	s.price = price
	return s
}

// PriceMatch set priceMatch
func (s *ModifyOrder) PriceMatch(priceMatch string) *ModifyOrder {
	s.priceMatch = &priceMatch
	return s
}

// Do send request
func (s *ModifyOrder) Do(ctx context.Context, opts ...RequestOption) (res *ModifyOrderResponse, err error) {
	r := &request{
		method:   http.MethodPut,
		endpoint: "/fapi/v1/order",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol":   s.symbol,
		"side":     s.side,
		"quantity": s.quantity,
		"price":    s.price,
	}
	if s.orderId != nil {
		m["orderId"] = *s.orderId
	}
	if s.origClientOrderId != nil {
		m["origClientOrderId"] = *s.origClientOrderId
	}
	if s.priceMatch != nil {
		m["priceMatch"] = *s.priceMatch
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(ModifyOrderResponse)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type ModifyOrderResponse struct {
	OrderId       int64        `json:"orderId"`
	Symbol        string       `json:"symbol"`
	Pair          string       `json:"pair"`
	Status        string       `json:"status"`
	ClientOrderId string       `json:"clientOrderId"`
	Price         string       `json:"price"`
	AvgPrice      string       `json:"avgPrice"`
	OrigQty       string       `json:"origQty"`
	ExecutedQty   string       `json:"executedQty"`
	CumQty        string       `json:"cumQty"`
	CumBase       string       `json:"cumBase"`
	TimeInForce   TimeInForce  `json:"timeInForce"`
	OrderType     OrderType    `json:"type"`
	ReduceOnly    bool         `json:"reduceOnly"`
	ClosePosition bool         `json:"closePosition"`
	Side          Side         `json:"side"`
	PositionSide  PositionSide `json:"positionSide"`
	StopPrice     string       `json:"stopPrice"`
	WorkingType   WorkingType  `json:"workingType"`
	PriceProtect  bool         `json:"priceProtect"`
	OrigType      string       `json:"origType"`
	PriceMatch    PriceMatch   `json:"priceMatch"`              //盘口价格下单模式
	StpMode       STPMode      `json:"selfTradePreventionMode"` //订单自成交保护模式
	GoodTillDate  int64        `json:"goodTillDate"`            //订单TIF为GTD时的自动取消时间
	UpdateTime    int64        `json:"updateTime"`

	Code int64  `json:"code"`
	Msg  string `json:"Msg"`
}

// BatchModifyOrder 批量修改订单 (PUT /fapi/v1/batchOrders)
type BatchModifyOrder struct {
	c           *Client
	batchOrders []Order
}

func (s *BatchModifyOrder) AddOrder(order Order) *BatchModifyOrder {
	s.batchOrders = append(s.batchOrders, order)
	return s
}

func (s *BatchModifyOrder) Do(ctx context.Context, opts ...RequestOption) (res []*ModifyOrderResponse, err error) {
	r := &request{
		method:   http.MethodPut,
		endpoint: "/fapi/v1/batchOrders",
		secType:  secTypeSigned,
	}
	orders, err := Marshal(s.batchOrders)
	if err == nil {
		r.setParam("batchOrders", string(orders))
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*ModifyOrderResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// OrderAmendent 查询订单修改历史 (GET /fapi/v1/orderAmendment)
type OrderAmendent struct {
	c                 *Client
	symbol            string
	orderId           *int64
	origClientOrderId *string
	startTime         *int64
	endTime           *int64
	limit             *int
}

func (s *OrderAmendent) Symbol(symbol string) *OrderAmendent {
	s.symbol = symbol
	return s
}

func (s *OrderAmendent) OrderId(orderId int64) *OrderAmendent {
	s.orderId = &orderId
	return s
}

func (s *OrderAmendent) OrigClientOrderId(origClientOrderId string) *OrderAmendent {
	s.origClientOrderId = &origClientOrderId
	return s
}

func (s *OrderAmendent) StartTime(startTime int64) *OrderAmendent {
	s.startTime = &startTime
	return s
}

func (s *OrderAmendent) EndTime(endTime int64) *OrderAmendent {
	s.endTime = &endTime
	return s
}

func (s *OrderAmendent) Limit(limit int) *OrderAmendent {
	s.limit = &limit
	return s
}

func (s *OrderAmendent) Do(ctx context.Context, opts ...RequestOption) (res *OrderAmendentResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/orderAmendment",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
	}
	if s.orderId != nil {
		m["orderId"] = *s.orderId
	}
	if s.origClientOrderId != nil {
		m["origClientOrderId"] = *s.origClientOrderId
	}
	if s.startTime != nil {
		m["startTime"] = *s.startTime
	}
	if s.endTime != nil {
		m["endTime"] = *s.endTime
	}
	if s.limit != nil {
		m["limit"] = *s.limit
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(OrderAmendentResponse)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type OrderAmendentResponse struct {
	AmendmentId   int64  `json:"amendmentId"`
	Symbol        string `json:"symbol"`
	Pair          string `json:"pair"`
	OrderId       int64  `json:"orderId"`
	ClientOrderId string `json:"clientOrderId"`
	Amendment     struct {
		Price struct {
			Before string `json:"before"`
			After  string `json:"after"`
		}
		OrigQty struct {
			Before string `json:"before"`
			After  string `json:"after"`
		}
	} `json:"amendment"`
}

// TODO: Cancel Order 撤销订单 (DELETE /fapi/v1/order)
type CancelOrderService struct {
	c                 *Client
	symbol            string
	orderId           *int64
	origClientOrderId *string
}

func (s *CancelOrderService) Symbol(symbol string) *CancelOrderService {
	s.symbol = symbol
	return s
}

func (s *CancelOrderService) OrderId(orderId int64) *CancelOrderService {
	s.orderId = &orderId
	return s
}

func (s *CancelOrderService) OrigClientOrderId(origClientOrderId string) *CancelOrderService {
	s.origClientOrderId = &origClientOrderId
	return s
}

func (s *CancelOrderService) Do(ctx context.Context, opts ...RequestOption) (res *NewOpenOrdersResponse, err error) {
	r := &request{
		method:   http.MethodDelete,
		endpoint: "/fapi/v1/order",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
	}
	if s.orderId != nil {
		m["orderId"] = *s.orderId
	}
	if s.origClientOrderId != nil {
		m["origClientOrderId"] = *s.origClientOrderId
	}
	r.setParams(m)

	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(NewOpenOrdersResponse)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// TODO: Batch Cancel Order 批量撤销订单 (DELETE /fapi/v1/batchOrders)

// 撤销全部订单 (DELETE /fapi/v1/allOpenOrders)
// CancelAllOpenOrdersService cancel open orders
type CancelAllOpenOrdersService struct {
	c      *Client
	symbol string
}

// Symbol set symbol
func (s *CancelAllOpenOrdersService) Symbol(symbol string) *CancelAllOpenOrdersService {
	s.symbol = symbol
	return s
}

// Do send request
func (s *CancelAllOpenOrdersService) Do(ctx context.Context, opts ...RequestOption) (res *CancelAllOrderResponse, err error) {
	r := &request{
		method:   http.MethodDelete,
		endpoint: "/fapi/v1/allOpenOrders",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
	}
	r.setParams(m)

	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(CancelAllOrderResponse)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Create CancelOrderResponse
type CancelAllOrderResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// Binance Get current open orders (GET /fapi/v1/openOrders)
// GetOpenOrdersService get open orders
type GetOpenOrdersService struct {
	c      *Client
	symbol *string
}

// Symbol set symbol
func (s *GetOpenOrdersService) Symbol(symbol string) *GetOpenOrdersService {
	s.symbol = &symbol
	return s
}

// Do send request
func (s *GetOpenOrdersService) Do(ctx context.Context, opts ...RequestOption) (res []*NewOpenOrdersResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/openOrders",
		secType:  secTypeSigned,
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*NewOpenOrdersResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Create NewOpenOrdersResponse
type NewOpenOrdersResponse struct {
	Symbol        string       `json:"symbol"`                  // 交易对
	OrderId       int64        `json:"orderId"`                 // 系统订单号
	ClientOrderId string       `json:"clientOrderId"`           // 用户自定义的订单号
	Side          Side         `json:"side"`                    // 买卖方向
	PositionSide  PositionSide `json:"positionSide"`            // 持仓方向
	OrigType      OrderType    `json:"origType"`                // 触发前订单类型
	Price         string       `json:"price"`                   // 委托价格
	OrigQty       string       `json:"origQty"`                 // 原始委托数量
	ReduceOnly    bool         `json:"reduceOnly"`              // 是否仅减仓
	ClosePosition bool         `json:"closePosition"`           // 是否条件全平仓
	StopPrice     string       `json:"stopPrice"`               // 触发价，对`TRAILING_STOP_MARKET`无效
	AvgPrice      string       `json:"avgPrice"`                // 平均成交价
	ExecutedQty   string       `json:"executedQty"`             // 成交量
	CumQty        string       `json:"cumQty"`                  // 累计成交量
	CumQuote      string       `json:"cumQuote"`                // 成交金额
	Status        OrderStatus  `json:"status"`                  // 订单状态
	TimeInForce   string       `json:"timeInForce"`             // 有效方法
	Type          string       `json:"type"`                    // 订单类型
	TradeTime     int64        `json:"time"`                    // 订单时间
	UpdateTime    int64        `json:"updateTime"`              // 更新时间
	ActivatePrice string       `json:"activatePrice"`           // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
	PriceRate     string       `json:"priceRate"`               // 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
	WorkingType   WorkingType  `json:"workingType"`             // 条件价格触发类型
	PriceProtect  bool         `json:"priceProtect"`            // 是否开启条件单触发保护
	PriceMatch    string       `json:"priceMatch"`              //price match mode
	STPMode       string       `json:"selfTradePreventionMode"` //self trading preventation mode
	GoodTillDate  int64        `json:"goodTillDate"`            //order pre-set auot cancel time for TIF GTD order
}

// Countdown Cancel All (POST /fapi/v1/countdownCancelAll)
// 请求权重 10
type CountdownCancelAllService struct {
	c             *Client
	symbol        string
	countdownTime int64
}

func (s *CountdownCancelAllService) Symbol(symbol string) *CountdownCancelAllService {
	s.symbol = symbol
	return s
}

func (s *CountdownCancelAllService) CountdownTime(countdownTime int64) *CountdownCancelAllService {
	s.countdownTime = countdownTime
	return s
}

func (s *CountdownCancelAllService) Do(ctx context.Context, opts ...RequestOption) (res *CountdownCancelAllResponse, err error) {
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/countdownCancelAll",
		secType:  secTypeSigned,
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(CountdownCancelAllResponse)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type CountdownCancelAllResponse struct {
	Symbol        string `json:"symbol"`
	CountdownTime int64  `json:"countdownTime"`
}

// Get Order 查询订单 (GET /fapi/v1/order)
// 请求权重 1
type GetOrderService struct {
	c                 *Client
	symbol            string
	orderId           *int64
	origClientOrderId *string
}

func (s *GetOrderService) Symbol(symbol string) *GetOrderService {
	s.symbol = symbol
	return s
}

func (s *GetOrderService) OrderId(orderId int64) *GetOrderService {
	s.orderId = &orderId
	return s
}

func (s *GetOrderService) OrigClientOrderId(origClientOrderId string) *GetOrderService {
	s.origClientOrderId = &origClientOrderId
	return s
}

func (s *GetOrderService) Do(ctx context.Context, opts ...RequestOption) (res *GetOrderResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/order",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
	}
	if s.orderId != nil {
		m["orderId"] = *s.orderId
	}
	if s.origClientOrderId != nil {
		m["origClientOrderId"] = *s.origClientOrderId
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(GetOrderResponse)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type GetOrderResponse struct {
	ClientOrderId string       `json:"clientOrderId"`
	OrderId       int64        `json:"orderId"`
	Symbol        string       `json:"symbol"`
	Side          Side         `json:"side"`
	PositionSide  PositionSide `json:"positionSide"`
	Type          OrderType    `json:"type"`
	StopPrice     string       `json:"stopPrice"`
	Price         string       `json:"price"`
	ReduceOnly    bool         `json:"reduceOnly"`
	ClosePosition bool         `json:"closePosition"`
	TimeInForce   string       `json:"timeInForce"`
	Status        OrderStatus  `json:"status"`
	OrigQty       string       `json:"origQty"`
	OrigType      OrderType    `json:"origType"`
	AvgPrice      string       `json:"avgPrice"`
	CumQuote      string       `json:"cumQuote"`
	ExecutedQty   string       `json:"executedQty"`
	Time          int64        `json:"time"`
	ActivatePrice string       `json:"activatePrice"`
	PriceRate     string       `json:"priceRate"`
	UpdateTime    int64        `json:"updateTime"`
	WorkingType   WorkingType  `json:"workingType"`
	PriceProtect  bool         `json:"priceProtect"`
	PriceMatch    PriceMatch   `json:"priceMatch"`
	StpMode       STPMode      `json:"selfTradePreventionMode"`
	GoodTillDate  int64        `json:"goodTillDate"`
}

// Get All Order 查询所有订单 (GET /fapi/v1/allOrders)
// 请求权重 5
type GetAllOrdersService struct {
	c         *Client
	symbol    string
	orderId   *int64
	startTime *int64
	endTime   *int64
	limit     *int
}

func (s *GetAllOrdersService) Symbol(symbol string) *GetAllOrdersService {
	s.symbol = symbol
	return s
}

func (s *GetAllOrdersService) OrderId(orderId int64) *GetAllOrdersService {
	s.orderId = &orderId
	return s
}

func (s *GetAllOrdersService) StartTime(startTime int64) *GetAllOrdersService {
	s.startTime = &startTime
	return s
}

func (s *GetAllOrdersService) EndTime(endTime int64) *GetAllOrdersService {
	s.endTime = &endTime
	return s
}

func (s *GetAllOrdersService) Limit(limit int) *GetAllOrdersService {
	s.limit = &limit
	return s
}

func (s *GetAllOrdersService) Do(ctx context.Context, opts ...RequestOption) (res []*NewAllOrdersResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/allOrders",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
	}
	if s.orderId != nil {
		m["orderId"] = *s.orderId
	}
	if s.startTime != nil {
		m["startTime"] = *s.startTime
	}
	if s.endTime != nil {
		m["endTime"] = *s.endTime
	}
	if s.limit != nil {
		m["limit"] = *s.limit
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*NewAllOrdersResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Create NewAllOrdersResponse
type NewAllOrdersResponse struct {
	ClientOrderId string       `json:"clientOrderId"`
	OrderId       int64        `json:"orderId"`
	Symbol        string       `json:"symbol"`
	Side          Side         `json:"side"`
	PositionSide  PositionSide `json:"positionSide"`
	Type          OrderType    `json:"type"`
	StopPrice     string       `json:"stopPrice"`
	Price         string       `json:"price"`
	ReduceOnly    bool         `json:"reduceOnly"`
	ClosePosition bool         `json:"closePosition"`
	TimeInForce   string       `json:"timeInForce"`
	Status        OrderStatus  `json:"status"`
	OrigQty       string       `json:"origQty"`
	OrigType      OrderType    `json:"origType"`
	AvgPrice      string       `json:"avgPrice"`
	CumQuote      string       `json:"cumQuote"`
	ExecutedQty   string       `json:"executedQty"`
	Time          int64        `json:"time"`
	ActivatePrice string       `json:"activatePrice"`
	PriceRate     string       `json:"priceRate"`
	UpdateTime    int64        `json:"updateTime"`
	WorkingType   WorkingType  `json:"workingType"`
	PriceProtect  bool         `json:"priceProtect"`
	PriceMatch    PriceMatch   `json:"priceMatch"`
	StpMode       STPMode      `json:"selfTradePreventionMode"`
	GoodTillDate  int64        `json:"goodTillDate"`
}

// Get open Order 查询当前挂单 (GET /fapi/v1/openOrder)
// 请求权重: 1
type GetOpenOrderService struct {
	c                 *Client
	symbol            string
	orderId           *int64
	origClientOrderId *string
}

func (s *GetOpenOrderService) Symbol(symbol string) *GetOpenOrderService {
	s.symbol = symbol
	return s
}

func (s *GetOpenOrderService) OrderId(orderId int64) *GetOpenOrderService {
	s.orderId = &orderId
	return s
}

func (s *GetOpenOrderService) OrigClientOrderId(origClientOrderId *string) *GetOpenOrderService {
	s.origClientOrderId = origClientOrderId
	return s
}

func (s *GetOpenOrderService) Do(ctx context.Context, opts ...RequestOption) (res *NewOpenOrdersResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/openOrder",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
	}
	if s.orderId != nil {
		m["orderId"] = *s.orderId
	}
	if s.origClientOrderId != nil {
		m["origClientOrderId"] = *s.origClientOrderId
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(NewOpenOrdersResponse)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Get force orders 用户强平单历史 (GET /fapi/v1/forceOrders)
// 请求权重 20 (不带symbol 50)
type GetForceOrdersService struct {
	c             *Client
	symbol        *string
	autoCloseType *AutoCloseType
	startTime     *int64
	endTime       *int64
	limit         *int
}

func (s *GetForceOrdersService) Symbol(symbol string) *GetForceOrdersService {
	s.symbol = &symbol
	return s
}

func (s *GetForceOrdersService) AutoCloseType(autoCloseType AutoCloseType) *GetForceOrdersService {
	s.autoCloseType = &autoCloseType
	return s
}

func (s *GetForceOrdersService) StartTime(startTime int64) *GetForceOrdersService {
	s.startTime = &startTime
	return s
}

func (s *GetForceOrdersService) EndTime(endTime int64) *GetForceOrdersService {
	s.endTime = &endTime
	return s
}

func (s *GetForceOrdersService) Limit(limit int) *GetForceOrdersService {
	s.limit = &limit
	return s
}

func (s *GetForceOrdersService) Do(ctx context.Context, opts ...RequestOption) (res []*NewForceOrderResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/forceOrder",
		secType:  secTypeSigned,
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	if s.autoCloseType != nil {
		r.setParam("autoCloseType", *s.autoCloseType)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*NewForceOrderResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type NewForceOrderResponse struct {
	OrderId       int64        `json:"orderId"`
	Symbol        string       `json:"symbol"`
	Status        OrderStatus  `json:"status"`
	ClientOrderId string       `json:"clientOrderId"`
	Price         string       `json:"price"`
	AvgPrice      string       `json:"avgPrice"`
	OrigQty       string       `json:"origQty"`
	ExecutedQty   string       `json:"executedQty"`
	CumQuote      string       `json:"cumQuote"`
	TimeInForce   TimeInForce  `json:"timeInForce"`
	Type          OrderType    `json:"type"`
	ReduceOnly    bool         `json:"reduceOnly"`
	ClosePosition bool         `json:"closePosition"`
	Side          Side         `json:"side"`
	PositionSide  PositionSide `json:"positionSide"`
	StopPrice     string       `json:"stopPrice"`
	WorkingType   WorkingType  `json:"workingType"`
	OrigType      OrderType    `json:"origType"`
	Time          int64        `json:"time"`
	UpdateTime    int64        `json:"updateTime"`
}

// Get User Trades 账户成交历史 (GET /fapi/v1/userTrades)
// 请求权重 5
type GetUserTradesService struct {
	c         *Client
	symbol    string
	orderId   *int64
	startTime *int64
	endTime   *int64
	fromId    *int64
	limit     *int // 默认500, 最大1000
}

func (s *GetUserTradesService) Symbol(symbol string) *GetUserTradesService {
	s.symbol = symbol
	return s
}

func (s *GetUserTradesService) OrderId(orderId int64) *GetUserTradesService {
	s.orderId = &orderId
	return s
}

func (s *GetUserTradesService) StartTime(startTime int64) *GetUserTradesService {
	s.startTime = &startTime
	return s
}

func (s *GetUserTradesService) EndTime(endTime int64) *GetUserTradesService {
	s.endTime = &endTime
	return s
}

func (s *GetUserTradesService) FromId(fromId int64) *GetUserTradesService {
	s.fromId = &fromId
	return s
}

func (s *GetUserTradesService) Limit(limit int) *GetUserTradesService {
	s.limit = &limit
	return s
}

func (s *GetUserTradesService) Do(ctx context.Context, opts ...RequestOption) (res []*UserTradeResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/userTrades",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
	}
	if s.orderId != nil {
		m["orderId"] = *s.orderId
	}
	if s.startTime != nil {
		m["startTime"] = *s.startTime
	}
	if s.endTime != nil {
		m["endTime"] = *s.endTime
	}
	if s.fromId != nil {
		m["fromId"] = *s.fromId
	}
	if s.limit != nil {
		m["limit"] = *s.limit
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*UserTradeResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type UserTradeResponse struct {
	Buyer           bool   `json:"buyer"`           // 是否是买方
	Commission      string `json:"commission"`      // 手续费
	CommissionAsset string `json:"commissionAsset"` // 手续费计价单位
	Id              int64  `json:"id"`              // 交易ID
	Maker           bool   `json:"maker"`           // 是否是挂单方
	OrderId         int64  `json:"orderId"`         // 订单编号
	Price           string `json:"price"`           // 成交价
	Qty             string `json:"qty"`             // 成交量
	QuoteQty        string `json:"quoteQty"`        // 成交额
	RealizedPnl     string `json:"realizedPnl"`     // 实现盈亏
	Side            Side   `json:"side"`            // 买卖方向
	PositionSide    string `json:"positionSide"`    // 持仓方向
	Symbol          string `json:"symbol"`          // 交易对
	Time            int64  `json:"time"`            // 时间
}

// SetMarginTypeService 变换逐全仓模式 (POST /fapi/v1/marginType)
// 请求权重 1
type SetMarginTypeService struct {
	c          *Client
	symbol     string
	marginType MarginType // 保证金模式 ISOLATED(逐仓), CROSSED(全仓)
}

func (s *SetMarginTypeService) Symbol(symbol string) *SetMarginTypeService {
	s.symbol = symbol
	return s
}

func (s *SetMarginTypeService) MarginType(marginType MarginType) *SetMarginTypeService {
	s.marginType = marginType
	return s
}

func (s *SetMarginTypeService) Do(ctx context.Context, opts ...RequestOption) (res *Response, err error) {
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/marginType",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol":     s.symbol,
		"marginType": s.marginType,
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(Response)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type Response struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// Position Side Dual 更改持仓模式 (POST /fapi/v1/positionSide/dual)
// 请求权重 1
type PositionSideDualService struct {
	c                *Client
	dualSidePosition bool // "true": 双向持仓模式；"false": 单向持仓模式
}

func (s *PositionSideDualService) DualSidePosition(dualSidePosition bool) *PositionSideDualService {
	s.dualSidePosition = dualSidePosition
	return s
}

func (s *PositionSideDualService) Do(ctx context.Context, opts ...RequestOption) (res *Response, err error) {
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/positionSide/dual",
		secType:  secTypeSigned,
	}
	r.setParam("dualSidePosition", s.dualSidePosition)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(Response)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Leverage 调整开仓杠杆 (POST /fapi/v1/leverage)
// 请求权重 1
type SetLeverageService struct {
	c        *Client
	symbol   string
	leverage int // 目标杠杆倍数：1 到 125 整数
}

func (s *SetLeverageService) Symbol(symbol string) *SetLeverageService {
	s.symbol = symbol
	return s
}

func (s *SetLeverageService) Leverage(leverage int) *SetLeverageService {
	s.leverage = leverage
	return s
}

func (s *SetLeverageService) Do(ctx context.Context, opts ...RequestOption) (res *LeverageResponse, err error) {
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/leverage",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol":   s.symbol,
		"leverage": s.leverage,
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(LeverageResponse)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type LeverageResponse struct {
	Leverage         int    `json:"leverage"`         // 杠杆倍数
	MaxNotionalValue string `json:"maxNotionalValue"` // 当前杠杆倍数下允许的最大名义价值
	Symbol           string `json:"symbol"`
}

// Multi Asset Margin 更改联合保证金模式 (POST /fapi/v1/multiAssetsMargin)
// 变换用户在 所有symbol 合约上的联合保证金模式：开启或关闭联合保证金模式
// 请求权重 1
type SetMultiAssetMarginService struct {
	c                 *Client
	multiAssetsMargin bool // "true": 联合保证金模式开启；"false": 联合保证金模式关闭
}

func (s *SetMultiAssetMarginService) MultiAssetsMargin(multiAssetsMargin bool) *SetMultiAssetMarginService {
	s.multiAssetsMargin = multiAssetsMargin
	return s
}

func (s *SetMultiAssetMarginService) Do(ctx context.Context, opts ...RequestOption) (res *Response, err error) {
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/multiAssetsMargin",
		secType:  secTypeSigned,
	}
	r.setParam("multiAssetsMargin", s.multiAssetsMargin)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(Response)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Position Margin 调整持仓保证金 (POST /fapi/v1/positionMargin)
// 针对逐仓模式下的仓位，调整其逐仓保证金资金
// 只针对逐仓symbol 与 positionSide(如有)
// 请求权重 1
type PositionMarginService struct {
	c            *Client
	symbol       string
	positionSide *string
	amount       float64
	typ          int // 调整方向 1: 增加逐仓保证金，2: 减少逐仓保证金
}

func (s *PositionMarginService) Symbol(symbol string) *PositionMarginService {
	s.symbol = symbol
	return s
}

func (s *PositionMarginService) PositionSide(positionSide string) *PositionMarginService {
	s.positionSide = &positionSide
	return s
}

func (s *PositionMarginService) Amount(amount float64) *PositionMarginService {
	s.amount = amount
	return s
}

func (s *PositionMarginService) Type(typ int) *PositionMarginService {
	s.typ = typ
	return s
}

func (s *PositionMarginService) Do(ctx context.Context, opts ...RequestOption) (res *Response, err error) {
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/positionMargin",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
		"amount": s.amount,
		"type":   s.typ,
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(Response)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type PositionMarginResponse struct {
	Amount float64 `json:"amount"`
	Code   int     `json:"code"`
	Msg    string  `json:"msg"`
	Type   int     `json:"type"`
}

// Position Risk 用户持仓风险 (GET /fapi/v3/positionRisk)
// 查询持仓风险，仅返回有持仓或挂单的交易对
// 请与账户推送信息ACCOUNT_UPDATE配合使用，以满足您的及时性和准确性需求
// 请求权重 5
type PositionRisk struct {
	c      *Client
	symbol *string
}

func (s *PositionRisk) Symbol(symbol string) *PositionRisk {
	s.symbol = &symbol
	return s
}

func (s *PositionRisk) Do(ctx context.Context, opts ...RequestOption) (res []*PositionRiskResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v3/positionRisk",
		secType:  secTypeSigned,
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*PositionRiskResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type PositionRiskResponse struct {
	Symbol                 string       `json:"symbol"`
	PositionSide           PositionSide `json:"positionSide"` // 持仓方向
	PositionAmt            string       `json:"positionAmt"`
	EntryPrice             string       `json:"entryPrice"`
	BreakEvenPrice         string       `json:"breakEvenPrice"`
	MarkPrice              string       `json:"markPrice"`
	UnRealizedProfit       string       `json:"unRealizedProfit"` // 持仓未实现盈亏
	LiquidationPrice       string       `json:"liquidationPrice"`
	IsolatedMargin         string       `json:"isolatedMargin"`         // 逐仓保证金
	Notional               string       `json:"notional"`               // 名义价值
	MarginAsset            string       `json:"marginAsset"`            // 保证金资产
	IsolatedWallet         string       `json:"isolatedWallet"`         // 逐仓钱包
	InitialMargin          string       `json:"initialMargin"`          // 初始保证金
	MaintMargin            string       `json:"maintMargin"`            // 维持保证金
	PositionInitialMargin  string       `json:"positionInitialMargin"`  // 仓位初始保证金
	OpenOrderInitialMargin string       `json:"openOrderInitialMargin"` // 订单初始保证金
	Adl                    int          `json:"adl"`
	BidNotional            string       `json:"bidNotional"`
	AskNotional            string       `json:"askNotional"`
	UpdateTime             int64        `json:"updateTime"` // 更新时间
}

// Adl Quantile 持仓ADL队列估算 (GET /fapi/v1/adlQuantile)
// 持仓ADL队列估算
//
//		每30秒更新数据
//		队列分数0，1，2，3，4，分数越高说明在ADL队列中的位置越靠前
//		对于单向持仓模式或者是逐仓状态下的双向持仓模式的交易对，
//		会返回 "LONG", "SHORT" 和 "BOTH" 分别表示不同持仓方向上持仓的adl队列分数
//		对于全仓状态下的双向持仓模式的交易对，会返回 "LONG", "SHORT" 和 "HEDGE",
//		其中"HEDGE"的存在仅作为标记;其中如果多空均有持仓的情况下,
//	    "LONG"和"SHORT"返回共同计算后相同的队列分数。
//
// 请求权重 5
type GetAdlQuantile struct {
	c      *Client
	symbol *string
}

func (s *GetAdlQuantile) Symbol(symbol string) *GetAdlQuantile {
	s.symbol = &symbol
	return s
}

func (s *GetAdlQuantile) Do(ctx context.Context, opts ...RequestOption) (res *AdlQuantileResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/adlQuantile",
		secType:  secTypeSigned,
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(AdlQuantileResponse)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type AdlQuantileResponse struct {
	Symbol      string      `json:"symbol"`
	AdlQuantile AdlQuantile `json:"adlQuantile"`
}

// 对于全仓状态下的双向持仓模式的交易对，会返回 "LONG", "SHORT" 和 "HEDGE",
// 其中"HEDGE"的存在仅作为标记;如果多空均有持仓的情况下,"LONG"和"SHORT"应返回共同计算后相同的队列分数
type AdlQuantile struct {
	Long  int `json:"LONG"`  // 双开模式下多头持仓的ADL队列估算分
	Short int `json:"SHORT"` // 双开模式下空头持仓的ADL队列估算分
	Hedge int `json:"HEDGE"` // HEDGE 仅作为指示出现，请忽略数值
	Both  int `json:"BOTH"`  // 单开模式下持仓的ADL队列估算分
}

// Position Margin History 逐仓保证金变动历史 (GET /fapi/v1/positionMargin/history)
// 查询逐仓保证金变动历史
// 请求权重 1
type PositionMarginHist struct {
	c         *Client
	symbol    string
	typ       *int // 调整方向 1: 增加逐仓保证金，2: 减少逐仓保证金
	startTime *int64
	endTime   *int64
	limit     *int // 返回的结果集数量 默认值: 500
}

func (s *PositionMarginHist) Symbol(symbol string) *PositionMarginHist {
	s.symbol = symbol
	return s
}

func (s *PositionMarginHist) Type(typ int) *PositionMarginHist {
	s.typ = &typ
	return s
}

func (s *PositionMarginHist) StartTime(startTime int64) *PositionMarginHist {
	s.startTime = &startTime
	return s
}

func (s *PositionMarginHist) EndTime(endTime int64) *PositionMarginHist {
	s.endTime = &endTime
	return s
}

func (s *PositionMarginHist) Limit(limit int) *PositionMarginHist {
	s.limit = &limit
	return s
}

func (s *PositionMarginHist) Do(ctx context.Context, opts ...RequestOption) (res []*PositionMarginHistResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/positionMargin/history",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
		"type":   s.typ,
	}
	if s.typ != nil {
		m["type"] = *s.typ
	}
	if s.startTime != nil {
		m["startTime"] = *s.startTime
	}
	if s.endTime != nil {
		m["endTime"] = *s.endTime
	}
	if s.limit != nil {
		m["limit"] = *s.limit
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*PositionMarginHistResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type PositionMarginHistResponse struct {
	Symbol       string `json:"symbol"`
	Type         int    `json:"type"`         // 调整方向
	DeltaType    string `json:"deltaType"`    // 划转类型
	Amount       string `json:"amount"`       // 数量
	Asset        string `json:"asset"`        // 资产
	Time         int64  `json:"time"`         // 时间
	PositionSide string `json:"positionSide"` // 持仓方向
}

// Order Test 下单测试 (GET /fapi/v1/order/test)
// 用于测试订单请求，但不会提交到撮合引擎
type OrderTestService struct {
	c                *Client
	symbol           string
	side             Side
	positionSide     *PositionSide
	orderType        OrderType
	reduceOnly       *string
	quantity         *float64
	price            *float64
	newClientOrderId *string
	stopPrice        *float64
	closePosition    *bool
	activationPrice  *float64
	callbackRate     *float64
	timeInForce      *TimeInForce
	workingType      *WorkingType
	priceProtect     *bool
	newOrderRespType *OrderRespType
	priceMatch       *PriceMatch
	stpMode          *STPMode
	goodTillDate     *int64
}

func (s *OrderTestService) Symbol(symbol string) *OrderTestService {
	s.symbol = symbol
	return s
}

func (s *OrderTestService) Side(side Side) *OrderTestService {
	s.side = side
	return s
}

func (s *OrderTestService) PositionSide(positionSide PositionSide) *OrderTestService {
	s.positionSide = &positionSide
	return s
}

func (s *OrderTestService) OrderType(orderType OrderType) *OrderTestService {
	s.orderType = orderType
	return s
}

func (s *OrderTestService) ReduceOnly(reduceOnly string) *OrderTestService {
	s.reduceOnly = &reduceOnly
	return s
}

func (s *OrderTestService) Quantity(quantity float64) *OrderTestService {
	s.quantity = &quantity
	return s
}

func (s *OrderTestService) Price(price float64) *OrderTestService {
	s.price = &price
	return s
}

func (s *OrderTestService) NewClientOrderId(newClientOrderId string) *OrderTestService {
	s.newClientOrderId = &newClientOrderId
	return s
}

func (s *OrderTestService) StopPrice(stopPrice float64) *OrderTestService {
	s.stopPrice = &stopPrice
	return s
}

func (s *OrderTestService) ClosePosition(closePosition bool) *OrderTestService {
	s.closePosition = &closePosition
	return s
}

func (s *OrderTestService) ActivationPrice(activationPrice float64) *OrderTestService {
	s.activationPrice = &activationPrice
	return s
}

func (s *OrderTestService) CallbackRate(callbackRate float64) *OrderTestService {
	s.callbackRate = &callbackRate
	return s
}

func (s *OrderTestService) TimeInForce(timeInForce TimeInForce) *OrderTestService {
	s.timeInForce = &timeInForce
	return s
}

func (s *OrderTestService) WorkingType(workingType WorkingType) *OrderTestService {
	s.workingType = &workingType
	return s
}

func (s *OrderTestService) PriceProtect(priceProtect bool) *OrderTestService {
	s.priceProtect = &priceProtect
	return s
}

func (s *OrderTestService) NewOrderRespType(newOrderRespType OrderRespType) *OrderTestService {
	s.newOrderRespType = &newOrderRespType
	return s
}

func (s *OrderTestService) PriceMatch(priceMatch PriceMatch) *OrderTestService {
	s.priceMatch = &priceMatch
	return s
}

func (s *OrderTestService) StpMode(stpMode STPMode) *OrderTestService {
	s.stpMode = &stpMode
	return s
}

func (s *OrderTestService) GoodTillDate(goodTillDate int64) *OrderTestService {
	s.goodTillDate = &goodTillDate
	return s
}

func (s *OrderTestService) Do(ctx context.Context, opts ...RequestOption) (res interface{}, err error) {
	respType := ACK
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/order/test",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
		"side":   s.side,
		"type":   s.orderType,
	}
	switch s.orderType {
	case "MARKET":
		respType = FULL
	case "LIMIT":
		respType = FULL
	}
	if s.positionSide != nil {
		m["positionSide"] = *s.positionSide
	}
	if s.reduceOnly != nil {
		m["reduceOnly"] = *s.reduceOnly
	}
	if s.quantity != nil {
		m["quantity"] = *s.quantity
	}
	if s.price != nil {
		m["price"] = *s.price
	}
	if s.newClientOrderId != nil {
		m["newClientOrderId"] = *s.newClientOrderId
	}
	if s.stopPrice != nil {
		m["stopPrice"] = *s.stopPrice
	}
	if s.closePosition != nil {
		m["closePosition"] = *s.closePosition
	}
	if s.activationPrice != nil {
		m["activationPrice"] = *s.activationPrice
	}
	if s.callbackRate != nil {
		m["callbackRate"] = *s.callbackRate
	}
	if s.timeInForce != nil {
		m["timeInForce"] = *s.timeInForce
	}
	if s.workingType != nil {
		m["workingType"] = *s.workingType
	}
	if s.priceProtect != nil {
		m["priceProtect"] = *s.priceProtect
	}
	if s.priceMatch != nil {
		m["priceMatch"] = *s.priceMatch
	}
	if s.stpMode != nil {
		m["selfTradePreventionMode"] = *s.stpMode
	}
	if s.goodTillDate != nil {
		m["goodTillDate"] = *s.goodTillDate
	}
	if s.newOrderRespType != nil {
		m["newOrderRespType"] = *s.newOrderRespType
		switch *s.newOrderRespType {
		case "ACK":
			respType = ACK
		case "RESULT":
			respType = RESULT
		case "FULL":
			respType = FULL
		}
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	switch respType {
	case ACK:
		res = new(CreateOrderResponseACK)
	case RESULT:
		res = new(CreateOrderResponseRESULT)
	case FULL:
		res = new(CreateOrderResponseFULL)
	}
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Binance New Algo Order endpoint (POST /fapi/v1/algoOrder)
// CreateAlgoOrderService create order
type CreateAlgoOrderService struct {
	c                *Client
	algoType         AlgoType
	symbol           string
	side             Side
	positionSide     *PositionSide
	orderType        OrderType
	reduceOnly       *string
	quantity         *float64
	price            *float64
	clientAlgoID     *string
	triggerPrice     *float64
	closePosition    *string
	activationPrice  *float64
	callbackRate     *float64
	timeInForce      *TimeInForce
	workingType      *WorkingType
	priceProtect     *string
	newOrderRespType *OrderRespType
	priceMatch       *PriceMatch
	stpMode          *STPMode
	goodTillDate     *int64
}

func (s *CreateAlgoOrderService) AlgoType(algoType AlgoType) *CreateAlgoOrderService {
	s.algoType = algoType
	return s
}

func (s *CreateAlgoOrderService) Symbol(symbol string) *CreateAlgoOrderService {
	s.symbol = symbol
	return s
}

func (s *CreateAlgoOrderService) Side(side Side) *CreateAlgoOrderService {
	s.side = side
	return s
}

func (s *CreateAlgoOrderService) PositionSide(positionSide PositionSide) *CreateAlgoOrderService {
	s.positionSide = &positionSide
	return s
}

func (s *CreateAlgoOrderService) OrderType(orderType OrderType) *CreateAlgoOrderService {
	s.orderType = orderType
	return s
}

func (s *CreateAlgoOrderService) ReduceOnly(reduceOnly string) *CreateAlgoOrderService {
	s.reduceOnly = &reduceOnly
	return s
}

func (s *CreateAlgoOrderService) Quantity(quantity float64) *CreateAlgoOrderService {
	s.quantity = &quantity
	return s
}

func (s *CreateAlgoOrderService) Price(price float64) *CreateAlgoOrderService {
	s.price = &price
	return s
}

func (s *CreateAlgoOrderService) ClientAlgoID(clientAlgoID string) *CreateAlgoOrderService {
	s.clientAlgoID = &clientAlgoID
	return s
}

func (s *CreateAlgoOrderService) TriggerPrice(triggerPrice float64) *CreateAlgoOrderService {
	s.triggerPrice = &triggerPrice
	return s
}

func (s *CreateAlgoOrderService) ClosePosition(closePosition string) *CreateAlgoOrderService {
	s.closePosition = &closePosition
	return s
}

func (s *CreateAlgoOrderService) ActivationPrice(activationPrice float64) *CreateAlgoOrderService {
	s.activationPrice = &activationPrice
	return s
}

func (s *CreateAlgoOrderService) CallbackRate(callbackRate float64) *CreateAlgoOrderService {
	s.callbackRate = &callbackRate
	return s
}

func (s *CreateAlgoOrderService) TimeInForce(timeInForce TimeInForce) *CreateAlgoOrderService {
	s.timeInForce = &timeInForce
	return s
}

func (s *CreateAlgoOrderService) WorkingType(workingType WorkingType) *CreateAlgoOrderService {
	s.workingType = &workingType
	return s
}

func (s *CreateAlgoOrderService) PriceProtect(priceProtect string) *CreateAlgoOrderService {
	s.priceProtect = &priceProtect
	return s
}

func (s *CreateAlgoOrderService) NewOrderRespType(newOrderRespType OrderRespType) *CreateAlgoOrderService {
	s.newOrderRespType = &newOrderRespType
	return s
}

func (s *CreateAlgoOrderService) PriceMatch(priceMatch PriceMatch) *CreateAlgoOrderService {
	s.priceMatch = &priceMatch
	return s
}

func (s *CreateAlgoOrderService) STPMode(stpMode STPMode) *CreateAlgoOrderService {
	s.stpMode = &stpMode
	return s
}

func (s *CreateAlgoOrderService) GoodTillDate(goodTillDate int64) *CreateAlgoOrderService {
	s.goodTillDate = &goodTillDate
	return s
}

func (s *CreateAlgoOrderService) Do(ctx context.Context, opts ...RequestOption) (res *CreateAlgoOrderResponse, err error) {
	respType := ACK
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/algoOrder",
		secType:  secTypeSigned,
	}
	m := params{
		"algoType": s.algoType,
		"symbol":   s.symbol,
		"side":     s.side,
		"type":     s.orderType,
	}
	if s.positionSide != nil {
		m["positionSide"] = *s.positionSide
	}
	if s.reduceOnly != nil {
		m["reduceOnly"] = *s.reduceOnly
	}
	if s.quantity != nil {
		m["quantity"] = *s.quantity
	}
	if s.price != nil {
		m["price"] = *s.price
	}
	if s.clientAlgoID != nil {
		m["clientAlgoId"] = *s.clientAlgoID
	}
	if s.triggerPrice != nil {
		m["triggerPrice"] = *s.triggerPrice
	}
	if s.closePosition != nil {
		m["closePosition"] = *s.closePosition
	}
	if s.activationPrice != nil {
		m["activationPrice"] = *s.activationPrice
	}
	if s.callbackRate != nil {
		m["callbackRate"] = *s.callbackRate
	}
	if s.timeInForce != nil {
		m["timeInForce"] = *s.timeInForce
	}
	if s.workingType != nil {
		m["workingType"] = *s.workingType
	}
	if s.priceProtect != nil {
		m["priceProtect"] = *s.priceProtect
	}
	if s.newOrderRespType != nil {
		m["newOrderRespType"] = *s.newOrderRespType
	} else {
		m["newOrderRespType"] = respType
	}
	if s.priceMatch != nil {
		m["priceMatch"] = *s.priceMatch
	}
	if s.stpMode != nil {
		m["selfTradePreventionMode"] = *s.stpMode
	}
	if s.goodTillDate != nil {
		m["goodTillDate"] = *s.goodTillDate
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(CreateAlgoOrderResponse)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, err
}

type CreateAlgoOrderResponse struct {
	AlgoID          int64        `json:"algoId"`
	ClientAlgoID    string       `json:"clientAlgoId"`
	AlgoType        AlgoType     `json:"algoType"`
	Side            Side         `json:"side"`
	PositionSide    PositionSide `json:"positionSide"`
	OrderType       OrderType    `json:"orderType"`
	Symbol          string       `json:"symbol"`
	TimeInForce     TimeInForce  `json:"timeInForce"`
	Quantity        string       `json:"quantity"`
	AlgoStatus      OrderStatus  `json:"algoStatus"`
	TriggerPrice    string       `json:"triggerPrice"`
	Price           string       `json:"price"`
	IcebergQuantity string       `json:"icebergQuantity"`
	STPMode         STPMode      `json:"selfTradePreventionMode"`
	WorkingType     WorkingType  `json:"workingType"`
	PriceMatch      PriceMatch   `json:"priceMatch"`
	ClosePosition   bool         `json:"closePosition"`
	ReduceOnly      bool         `json:"reduceOnly"`
	ActivatePrice   string       `json:"activatePrice"`
	CallbackRate    string       `json:"callbackRate"`
	CreateTime      int64        `json:"createTime"`
	UpdateTime      int64        `json:"updateTime"`
	TriggerTime     int64        `json:"triggerTime"`
	GoodTillDate    int64        `json:"goodTillDate"`
}

// Binance Cancel Algo Order Service (DELETE /fapi/v1/algoOrder)
type CancelAlgoOrderService struct {
	c            *Client
	algoID       *int64
	clientAlgoID *string
}

func (s *CancelAlgoOrderService) AlgoID(algoID int64) *CancelAlgoOrderService {
	s.algoID = &algoID
	return s
}

func (s *CancelAlgoOrderService) ClientAlgoID(clientAlgoID string) *CancelAlgoOrderService {
	s.clientAlgoID = &clientAlgoID
	return s
}

func (s *CancelAlgoOrderService) Do(ctx context.Context, opts ...RequestOption) (res *NewOpenAlgoOrdersResponse, err error) {
	r := &request{
		method:   http.MethodDelete,
		endpoint: "/fapi/v1/algoOrder",
		secType:  secTypeSigned,
	}
	m := params{}
	if s.algoID != nil {
		m["algoId"] = *s.algoID
	}
	if s.clientAlgoID != nil {
		m["clientAlgoId"] = *s.clientAlgoID
	}
	r.setParams(m)

	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(NewOpenAlgoOrdersResponse)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Binance Cancel Algo Open Orders endpoint (DELETE /fapi/v1/algoOpenOrders)
type CancelAlgoOpenOrdersService struct {
	c      *Client
	symbol string
}

func (s *CancelAlgoOpenOrdersService) Symbol(symbol string) *CancelAlgoOpenOrdersService {
	s.symbol = symbol
	return s
}

func (s *CancelAlgoOpenOrdersService) Do(ctx context.Context, opts ...RequestOption) (res *CancelAlgoOpenOrderResponse, err error) {
	r := &request{
		method:   http.MethodDelete,
		endpoint: "/fapi/v1/algoOpenOrders",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
	}
	r.setParams(m)

	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(CancelAlgoOpenOrderResponse)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type CancelAlgoOpenOrderResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// Binance Get Algo Order endpoint (GET /fapi/v1/algoOrder)
type GetAlgoOrderService struct {
	c            *Client
	algoID       *int64
	clientAlgoID *string
}

func (s *GetAlgoOrderService) AlgoID(algoID int64) *GetAlgoOrderService {
	s.algoID = &algoID
	return s
}

func (s *GetAlgoOrderService) ClientAlgoID(clientAlgoID string) *GetAlgoOrderService {
	s.clientAlgoID = &clientAlgoID
	return s
}

func (s *GetAlgoOrderService) Do(ctx context.Context, opts ...RequestOption) (res *GetAlgoOrderResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/algoOrder",
		secType:  secTypeSigned,
	}
	m := params{}
	if s.algoID != nil {
		m["algoId"] = *s.algoID
	}
	if s.clientAlgoID != nil {
		m["clientAlgoId"] = *s.clientAlgoID
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(GetAlgoOrderResponse)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type GetAlgoOrderResponse struct {
	AlgoID          int64        `json:"algoId"`
	ClientAlgoID    string       `json:"clientAlgoId"`
	AlgoType        AlgoType     `json:"algoType"`
	OrderType       OrderType    `json:"orderType"`
	Side            Side         `json:"side"`
	PositionSide    PositionSide `json:"positionSide"`
	TimeInForce     TimeInForce  `json:"timeInForce"`
	AlgoStatus      OrderStatus  `json:"algoStatus"`
	ActualOrderID   string       `json:"actualOrderId"`
	ActualPrice     string       `json:"actualPrice"`
	TriggerPrice    string       `json:"triggerPrice"`
	Price           string       `json:"price"`
	IcebergQuantity string       `json:"icebergQuantity"`
	TpTriggerPrice  string       `json:"tpTriggerPrice"`
	TpPrice         string       `json:"tpPrice"`
	SlTriggerPrice  string       `json:"slTriggerPrice"`
	SlPrice         string       `json:"slPrice"`
	TpOrderType     string       `json:"tpOrderType"`
	STPMode         STPMode      `json:"selfTradePreventionMode"`
	WorkingType     WorkingType  `json:"workingType"`
	PriceMatch      PriceMatch   `json:"priceMatch"`
	ClosePosition   bool         `json:"closePosition"`
	PriceProtect    bool         `json:"priceProtect"`
	ReduceOnly      bool         `json:"reduceOnly"`
	CreateTime      int64        `json:"createTime"`
	UpdateTime      int64        `json:"updateTime"`
	TriggerTime     int64        `json:"triggerTime"`
	GoodTillDate    int64        `json:"goodTillDate"`
}

// Binance Get Open Algo Orders endpoint (GET /fapi/v1/openAlgoOrders)
type GetOpenAlgoOrdersService struct {
	c        *Client
	algoType *AlgoType
	symbol   *string
	algoID   *int64
}

func (s *GetOpenAlgoOrdersService) AlgoType(algoType AlgoType) *GetOpenAlgoOrdersService {
	s.algoType = &algoType
	return s
}

func (s *GetOpenAlgoOrdersService) Symbol(symbol string) *GetOpenAlgoOrdersService {
	s.symbol = &symbol
	return s
}

func (s *GetOpenAlgoOrdersService) AlgoID(algoID int64) *GetOpenAlgoOrdersService {
	s.algoID = &algoID
	return s
}

func (s *GetOpenAlgoOrdersService) Do(ctx context.Context, opts ...RequestOption) (res []*NewOpenAlgoOrdersResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/openAlgoOrders",
		secType:  secTypeSigned,
	}
	if s.algoType != nil {
		r.setParam("algoType", *s.algoType)
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	if s.algoID != nil {
		r.setParam("algoId", *s.algoID)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*NewOpenAlgoOrdersResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type NewOpenAlgoOrdersResponse struct {
	AlgoID          int64        `json:"algoId"`
	ClientAlgoID    string       `json:"clientAlgoId"`
	AlgoType        AlgoType     `json:"algoType"`
	Symbol          string       `json:"symbol"`
	OrderType       OrderType    `json:"orderType"`
	Side            Side         `json:"side"`
	PositionSide    PositionSide `json:"positionSide"`
	TimeInForce     TimeInForce  `json:"timeInForce"`
	AlgoStatus      OrderStatus  `json:"algoStatus"`
	ActualOrderID   string       `json:"actualOrderId"`
	ActualPrice     string       `json:"actualPrice"`
	TriggerPrice    string       `json:"triggerPrice"`
	Price           string       `json:"price"`
	Quantity        string       `json:"quantity"`
	IcebergQuantity string       `json:"icebergQuantity"`
	TpTriggerPrice  string       `json:"tpTriggerPrice"`
	TpPrice         string       `json:"tpPrice"`
	SlTriggerPrice  string       `json:"slTriggerPrice"`
	SlPrice         string       `json:"slPrice"`
	TpOrderType     string       `json:"tpOrderType"`
	STPMode         STPMode      `json:"selfTradePreventionMode"`
	WorkingType     WorkingType  `json:"workingType"`
	PriceMatch      PriceMatch   `json:"priceMatch"`
	ClosePosition   bool         `json:"closePosition"`
	PriceProtect    bool         `json:"priceProtect"`
	ReduceOnly      bool         `json:"reduceOnly"`
	CreateTime      int64        `json:"createTime"`
	UpdateTime      int64        `json:"updateTime"`
	TriggerTime     int64        `json:"triggerTime"`
	GoodTillDate    int64        `json:"goodTillDate"`
}

// Binance Get All Algo Orders endpoint (GET /fapi/v1/allAlgoOrders)
type GetAllAlgoOrdersService struct {
	c         *Client
	symbol    string
	algoID    *int64
	startTime *int64
	endTime   *int64
	page      *int
	limit     *int
}

func (s *GetAllAlgoOrdersService) Symbol(symbol string) *GetAllAlgoOrdersService {
	s.symbol = symbol
	return s
}

func (s *GetAllAlgoOrdersService) AlgoID(algoID int64) *GetAllAlgoOrdersService {
	s.algoID = &algoID
	return s
}

func (s *GetAllAlgoOrdersService) StartTime(startTime int64) *GetAllAlgoOrdersService {
	s.startTime = &startTime
	return s
}

func (s *GetAllAlgoOrdersService) EndTime(endTime int64) *GetAllAlgoOrdersService {
	s.endTime = &endTime
	return s
}

func (s *GetAllAlgoOrdersService) Page(page int) *GetAllAlgoOrdersService {
	s.page = &page
	return s
}

func (s *GetAllAlgoOrdersService) Limit(limit int) *GetAllAlgoOrdersService {
	s.limit = &limit
	return s
}

func (s *GetAllAlgoOrdersService) Do(ctx context.Context, opts ...RequestOption) (res []*NewAllAlgoOrdersResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/allAlgoOrders",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
	}
	if s.algoID != nil {
		m["algoId"] = *s.algoID
	}
	if s.startTime != nil {
		m["startTime"] = *s.startTime
	}
	if s.endTime != nil {
		m["endTime"] = *s.endTime
	}
	if s.page != nil {
		m["page"] = *s.page
	}
	if s.limit != nil {
		m["limit"] = *s.limit
	}
	r.setParams(m)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*NewAllAlgoOrdersResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type NewAllAlgoOrdersResponse struct {
	AlgoID          int64        `json:"algoId"`
	ClientAlgoID    string       `json:"clientAlgoId"`
	AlgoType        AlgoType     `json:"algoType"`
	OrderType       OrderType    `json:"orderType"`
	Side            Side         `json:"side"`
	PositionSide    PositionSide `json:"positionSide"`
	TimeInForce     TimeInForce  `json:"timeInForce"`
	AlgoStatus      OrderStatus  `json:"algoStatus"`
	ActualOrderID   string       `json:"actualOrderId"`
	ActualPrice     string       `json:"actualPrice"`
	TriggerPrice    string       `json:"triggerPrice"`
	Price           string       `json:"price"`
	IcebergQuantity string       `json:"icebergQuantity"`
	TpTriggerPrice  string       `json:"tpTriggerPrice"`
	TpPrice         string       `json:"tpPrice"`
	SlTriggerPrice  string       `json:"slTriggerPrice"`
	SlPrice         string       `json:"slPrice"`
	TpOrderType     string       `json:"tpOrderType"`
	STPMode         STPMode      `json:"selfTradePreventionMode"`
	WorkingType     WorkingType  `json:"workingType"`
	PriceMatch      PriceMatch   `json:"priceMatch"`
	ClosePosition   bool         `json:"closePosition"`
	PriceProtect    bool         `json:"priceProtect"`
	ReduceOnly      bool         `json:"reduceOnly"`
	CreateTime      int64        `json:"createTime"`
	UpdateTime      int64        `json:"updateTime"`
	TriggerTime     int64        `json:"triggerTime"`
	GoodTillDate    int64        `json:"goodTillDate"`
}

// Binance Get API Trading Status endpoint (GET /fapi/v1/apiTradingStatus)
// https://developers.binance.com/legacy-docs/zh-CN/derivatives/usds-margined-futures/account/rest-api/Futures-Trading-Quantitative-Rules-Indicators#http%E8%AF%B7%E6%B1%82
type GetAPITradingStatusService struct {
	c      *Client
	symbol *string
}

func (s *GetAPITradingStatusService) Symbol(symbol string) *GetAPITradingStatusService {
	s.symbol = &symbol
	return s
}

func (s *GetAPITradingStatusService) Do(ctx context.Context, opts ...RequestOption) (res *APITradingStatus, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/apiTradingStatus",
		secType:  secTypeSigned,
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(APITradingStatus)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type APITradingStatus struct {
	Indicators map[string][]Indicator `json:"indicators"`
	UpdateTime int64                  `json:"updateTime"`
}

type Indicator struct {
	Indicator          string  `json:"indicator"`
	Value              float64 `json:"value"`
	TriggerValue       float64 `json:"triggerValue"`
	PlannedRecoverTime int64   `json:"plannedRecoverTime"`
	IsLocked           bool    `json:"isLocked"`
}
