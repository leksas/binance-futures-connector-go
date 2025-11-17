package binance_futures_connector

import (
	"context"
	"encoding/json"
	"log"
	"math/big"
	"net/http"
)

// Binance Test Connectivity endpoint (Get /fapi/v1/ping)
type Ping struct {
	c *Client
}

func (s *Ping) Do(ctx context.Context, opts ...RequestOption) error {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/ping",
		secType:  secTypeNone,
	}
	_, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return err
	}
	return nil
}

// Binance Check Server Time endpoint (GET /fapi/v1/time)
type ServerTime struct {
	c *Client
}

func (s *ServerTime) Do(ctx context.Context, opts ...RequestOption) (*ServerTimeResponse, error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/time",
		secType:  secTypeNone,
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res := new(ServerTimeResponse)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ServerTimeResponse define server time response
type ServerTimeResponse struct {
	ServerTime int64 `json:"serverTime"`
}

// Binance Exchange Information endpoint (GET /api/v3/exchangeInfo)
type ExchangeInfo struct {
	c *Client
}

// Send the request
func (s *ExchangeInfo) Do(ctx context.Context, opts ...RequestOption) (res *ExchangeInfoResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/exchangeInfo",
		secType:  secTypeNone,
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(ExchangeInfoResponse)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ExchangeInfoResponse define exchange info response
type ExchangeInfoResponse struct {
	Timezone        string            `json:"timezone"`
	ServerTime      uint64            `json:"serverTime"`
	FuturesType     string            `json:"futuresType"`
	RateLimits      []*RateLimit      `json:"rateLimits"`
	ExchangeFilters []*ExchangeFilter `json:"exchangeFilters"`
	Assets          []*AssetInfo      `json:"assets"`
	Symbols         []*SymbolInfo     `json:"symbols"`
}

// RateLimit define rate limit
type RateLimit struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   int    `json:"intervalNum"`
	Limit         int    `json:"limit"`
}

// ExchangeFilter define exchange filter
type ExchangeFilter struct {
	FilterType string `json:"filterType"`
	MaxNumAlgo int64  `json:"maxNumAlgoOrders"`
}

type AssetInfo struct {
	Asset             string `json:"asset"`
	MarginAvailable   bool   `json:"marginAvailable"`
	AutoAssetExchange string `json:"autoAssetExchange"`
	MarginBalance     string `json:"marginBalance"`
	UpdateTime        int64  `json:"updateTime"`
}

// Symbol define symbol
type SymbolInfo struct {
	Symbol                string          `json:"symbol"`
	Pair                  string          `json:"pair"`
	ContractType          ContractType    `json:"contractType"`
	DeliveryDate          int64           `json:"deliveryDate"`
	OnboardDate           int64           `json:"onboardDate"`
	Status                ContractStatus  `json:"status"`
	MaintMarginPercent    string          `json:"maintMarginPercent"`
	RequiredMarginPercent string          `json:"requiredMarginPercent"`
	BaseAsset             string          `json:"baseAsset"`
	QuoteAsset            string          `json:"quoteAsset"`
	MarginAsset           string          `json:"marginAsset"`
	PricePrecision        int             `json:"pricePrecision"`
	QuantityPrecision     int             `json:"quantityPrecision"`
	BaseAssetPrecision    int             `json:"baseAssetPrecision"`
	QuotePrecision        int             `json:"quotePrecision"`
	UnderlyingType        string          `json:"underlyingType"`
	UnderlyingSubType     []string        `json:"underlyingSubType"`
	SettlePlan            int             `json:"settlePlan"`
	TriggerProtect        string          `json:"triggerProtect"`
	Filters               []*SymbolFilter `json:"filters"`
	OrderType             []string        `json:"orderTypes"`
	TimeInForce           []string        `json:"timeInForce"`
	PermissionSets        []string        `json:"permissionSets"`
	LiquidationFee        string          `json:"liquidationFee"`
	MarketTakeBound       string          `json:"marketTakeBound"`
	MaxMoveOrderLimit     int64           `json:"maxMoveOrderLimit"`
}

// SymbolFilter define symbol filter
type SymbolFilter struct {
	FilterType          string `json:"filterType"`
	MaxPrice            string `json:"maxPrice"`
	MinPrice            string `json:"minPrice"`
	TickSize            string `json:"tickSize"`
	MaxQty              string `json:"maxQty"`
	MinQty              string `json:"minQty"`
	StepSize            string `json:"stepSize"`
	Limit               int    `json:"limit"`
	Notional            string `json:"notional"`
	MultiplierUp        string `json:"multiplierUp"`
	MultiplierDown      string `json:"multiplierDown"`
	MultiplierDecimal   string `json:"multiplierDecimal"`
	PositionControlSide string `json:"positionControlSide"`
}

// Binance Order Book endpoint (GET /fapi/v1/depth)
type OrderBook struct {
	c      *Client
	symbol string
	limit  *int
}

// Symbol set symbol
func (s *OrderBook) Symbol(symbol string) *OrderBook {
	s.symbol = symbol
	return s
}

// Limit set limit
func (s *OrderBook) Limit(limit int) *OrderBook {
	s.limit = &limit
	return s
}

// Send the request
func (s *OrderBook) Do(ctx context.Context, opts ...RequestOption) (res *OrderBookResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/depth",
		secType:  secTypeNone,
	}
	r.setParam("symbol", s.symbol)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(OrderBookResponse)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// OrderBookResponse define order book response
type OrderBookResponse struct {
	LastUpdateID    int64          `json:"lastUpdateId"`
	EventTime       int64          `json:"E"`
	TransactionTime int64          `json:"T"`
	Bids            [][]*big.Float `json:"bids"`
	Asks            [][]*big.Float `json:"asks"`
}

// Binance Recent Trades List endpoint (GET /fapi/v1/trades)
type RecentTradesList struct {
	c      *Client
	symbol string
	limit  *int
}

// Symbol set symbol
func (s *RecentTradesList) Symbol(symbol string) *RecentTradesList {
	s.symbol = symbol
	return s
}

// Limit set limit
func (s *RecentTradesList) Limit(limit int) *RecentTradesList {
	s.limit = &limit
	return s
}

// Send the request
func (s *RecentTradesList) Do(ctx context.Context, opts ...RequestOption) (res []*RecentTradesListResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/trades",
		secType:  secTypeNone,
	}
	r.setParam("symbol", s.symbol)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// RecentTradesListResponse define recent trades list response
type RecentTradesListResponse struct {
	Id           uint64 `json:"id"`
	Price        string `json:"price"`
	Qty          string `json:"qty"`
	Time         uint64 `json:"time"`
	QuoteQty     string `json:"quoteQty"`
	IsBuyerMaker bool   `json:"isBuyerMaker"`
}

// Binance Old Trade Lookup endpoint (GET /fapi/v1/historicalTrades)
type HistoricalTradeLookup struct {
	c      *Client
	symbol string
	limit  *uint
	fromId *int64
}

// Symbol set symbol
func (s *HistoricalTradeLookup) Symbol(symbol string) *HistoricalTradeLookup {
	s.symbol = symbol
	return s
}

// Limit set limit
func (s *HistoricalTradeLookup) Limit(limit uint) *HistoricalTradeLookup {
	s.limit = &limit
	return s
}

// FromId set fromId
func (s *HistoricalTradeLookup) FromId(fromId int64) *HistoricalTradeLookup {
	s.fromId = &fromId
	return s
}

// Send the request
func (s *HistoricalTradeLookup) Do(ctx context.Context, opts ...RequestOption) (res []*RecentTradesListResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/historicalTrades",
		secType:  secTypeAPIKey,
	}
	r.setParam("symbol", s.symbol)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.fromId != nil {
		r.setParam("fromId", *s.fromId)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Binance Compressed/Aggregate Trades List endpoint (GET /fapi/v1/aggTrades)
type AggTradesList struct {
	c         *Client
	symbol    string
	limit     *int
	fromId    *int
	startTime *uint64
	endTime   *uint64
}

// Symbol set symbol
func (s *AggTradesList) Symbol(symbol string) *AggTradesList {
	s.symbol = symbol
	return s
}

// Limit set limit
func (s *AggTradesList) Limit(limit int) *AggTradesList {
	s.limit = &limit
	return s
}

// FromId set fromId
func (s *AggTradesList) FromId(fromId int) *AggTradesList {
	s.fromId = &fromId
	return s
}

// StartTime set startTime
func (s *AggTradesList) StartTime(startTime uint64) *AggTradesList {
	s.startTime = &startTime
	return s
}

// EndTime set endTime
func (s *AggTradesList) EndTime(endTime uint64) *AggTradesList {
	s.endTime = &endTime
	return s
}

// Send the request
func (s *AggTradesList) Do(ctx context.Context, opts ...RequestOption) (res []*AggTradesListResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/aggTrades",
		secType:  secTypeNone,
	}
	r.setParam("symbol", s.symbol)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.fromId != nil {
		r.setParam("fromId", *s.fromId)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// AggTradesListResponse define compressed trades list response
type AggTradesListResponse struct {
	AggTradeId   uint64 `json:"a"`
	Price        string `json:"p"`
	Qty          string `json:"q"`
	FirstTradeId uint64 `json:"f"`
	LastTradeId  uint64 `json:"l"`
	Time         uint64 `json:"T"`
	IsBuyer      bool   `json:"m"`
}

// Binance Kline/Candlestick Data endpoint (GET /fapi/v1/klines)
type Klines struct {
	c         *Client
	symbol    string
	interval  string
	limit     *int
	startTime *uint64
	endTime   *uint64
}

// Symbol set symbol
func (s *Klines) Symbol(symbol string) *Klines {
	s.symbol = symbol
	return s
}

// Interval set interval
func (s *Klines) Interval(interval string) *Klines {
	s.interval = interval
	return s
}

// Limit set limit
func (s *Klines) Limit(limit int) *Klines {
	s.limit = &limit
	return s
}

// StartTime set startTime
func (s *Klines) StartTime(startTime uint64) *Klines {
	s.startTime = &startTime
	return s
}

// EndTime set endTime
func (s *Klines) EndTime(endTime uint64) *Klines {
	s.endTime = &endTime
	return s
}

func (s *Klines) Do(ctx context.Context, opts ...RequestOption) (res []*KlinesResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/klines",
		secType:  secTypeNone,
	}
	r.setParam("symbol", s.symbol)
	r.setParam("interval", s.interval)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	return parseKlinesResponse(data)
}

func parseKlinesResponse(data []byte) (res []*KlinesResponse, err error) {
	var klinesResponseArray KlinesResponseArray
	if err = Unmarshal(data, &klinesResponseArray); err != nil {
		log.Println("Error unmarshaling JSON:", err, "Message:", string(data))
		return nil, &UnmarshalError{
			InnerError: err,
			Message:    data,
		}
	}

	for _, kline := range klinesResponseArray {
		openTime := kline[0].(float64)
		open := kline[1].(string)
		high := kline[2].(string)
		low := kline[3].(string)
		clos := kline[4].(string)
		volume := kline[5].(string)
		closeTime := kline[6].(float64)
		quoteAssetVolume := kline[7].(string)
		numberOfTrades := kline[8].(float64)
		takerBuyBaseAssetVolume := kline[9].(string)
		takerBuyQuoteAssetVolume := kline[10].(string)

		// create a KlinesResponse struct using the parsed fields
		klinesResponse := &KlinesResponse{
			OpenTime:                 uint64(openTime),
			Open:                     open,
			High:                     high,
			Low:                      low,
			Close:                    clos,
			Volume:                   volume,
			CloseTime:                uint64(closeTime),
			QuoteAssetVolume:         quoteAssetVolume,
			NumberOfTrades:           uint64(numberOfTrades),
			TakerBuyBaseAssetVolume:  takerBuyBaseAssetVolume,
			TakerBuyQuoteAssetVolume: takerBuyQuoteAssetVolume,
		}
		res = append(res, klinesResponse)
	}
	return res, nil
}

type KlinesResponseArray [][]interface{}

// Define Klines response data
type KlinesResponse struct {
	OpenTime                 uint64 `json:"openTime"`
	Open                     string `json:"open"`
	High                     string `json:"high"`
	Low                      string `json:"low"`
	Close                    string `json:"close"`
	Volume                   string `json:"volume"`
	CloseTime                uint64 `json:"closeTime"`
	QuoteAssetVolume         string `json:"quoteAssetVolume"`
	NumberOfTrades           uint64 `json:"numberOfTrades"`
	TakerBuyBaseAssetVolume  string `json:"takerBuyBaseAssetVolume"`
	TakerBuyQuoteAssetVolume string `json:"takerBuyQuoteAssetVolume"`
}

// Binance Continuous Kline/Candlestick Data endpoint (GET /fapi/v1/continuousKlines)
type ContinuousKlines struct {
	c            *Client
	pair         string
	contractType ContractType
	interval     string
	limit        *int
	startTime    *uint64
	endTime      *uint64
}

// Pair set pair
func (s *ContinuousKlines) Pair(pair string) *ContinuousKlines {
	s.pair = pair
	return s
}

// ContractType set contractType
func (s *ContinuousKlines) ContractType(contractType ContractType) *ContinuousKlines {
	s.contractType = contractType
	return s
}

// Interval set interval
func (s *ContinuousKlines) Interval(interval string) *ContinuousKlines {
	s.interval = interval
	return s
}

// Limit set limit
func (s *ContinuousKlines) Limit(limit int) *ContinuousKlines {
	s.limit = &limit
	return s
}

// StartTime set startTime
func (s *ContinuousKlines) StartTime(startTime uint64) *ContinuousKlines {
	s.startTime = &startTime
	return s
}

// EndTime set endTime
func (s *ContinuousKlines) EndTime(endTime uint64) *ContinuousKlines {
	s.endTime = &endTime
	return s
}

// Send the request
func (s *ContinuousKlines) Do(ctx context.Context, opts ...RequestOption) (res []*KlinesResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/continuousKlines",
		secType:  secTypeNone,
	}
	r.setParam("pair", s.pair)
	r.setParam("contractType", s.contractType)
	r.setParam("interval", s.interval)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	return parseKlinesResponse(data)
}

// Binance Index Price Klines (GET /fapi/v1/indexPriceKlines)
type IndexPriceKlines struct {
	c         *Client
	pair      string
	interval  string
	startTime *int64
	endTime   *int64
	limit     *int
}

func (s *IndexPriceKlines) Pair(pair string) *IndexPriceKlines {
	s.pair = pair
	return s
}

// Interval set interval
func (s *IndexPriceKlines) Interval(interval string) *IndexPriceKlines {
	s.interval = interval
	return s
}

// StartTime set startTime
func (s *IndexPriceKlines) StartTime(startTime int64) *IndexPriceKlines {
	s.startTime = &startTime
	return s
}

// EndTime set endTime
func (s *IndexPriceKlines) EndTime(endTime int64) *IndexPriceKlines {
	s.endTime = &endTime
	return s
}

// Limit set limit
func (s *IndexPriceKlines) Limit(limit int) *IndexPriceKlines {
	s.limit = &limit
	return s
}

func (s *IndexPriceKlines) Do(ctx context.Context, opts ...RequestOption) (res []*KlinesResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/indexPriceKlines",
		secType:  secTypeNone,
	}
	r.setParam("pair", s.pair)
	r.setParam("interval", s.interval)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	return parseKlinesResponse(data)
}

// Binance Mark Price Klines (GET /fapi/v1/markPriceKlines)
type MarkPriceKlines struct {
	c         *Client
	symbol    string
	interval  string
	startTime *int64
	endTime   *int64
	limit     *int
}

func (s *MarkPriceKlines) Symbol(symbol string) *MarkPriceKlines {
	s.symbol = symbol
	return s
}

// Interval set interval
func (s *MarkPriceKlines) Interval(interval string) *MarkPriceKlines {
	s.interval = interval
	return s
}

// StartTime set startTime
func (s *MarkPriceKlines) StartTime(startTime int64) *MarkPriceKlines {
	s.startTime = &startTime
	return s
}

// EndTime set endTime
func (s *MarkPriceKlines) EndTime(endTime int64) *MarkPriceKlines {
	s.endTime = &endTime
	return s
}

// Limit set limit
func (s *MarkPriceKlines) Limit(limit int) *MarkPriceKlines {
	s.limit = &limit
	return s
}

func (s *MarkPriceKlines) Do(ctx context.Context, opts ...RequestOption) (res []*KlinesResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/markPriceKlines",
		secType:  secTypeNone,
	}
	r.setParam("symbol", s.symbol)
	r.setParam("interval", s.interval)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	return parseKlinesResponse(data)
}

// Binance Premium Index Klines (GET /fapi/v1/premiumIndexKlines)
type PremiumIndexKlines struct {
	c         *Client
	symbol    string
	interval  string
	startTime *int64
	endTime   *int64
	limit     *int
}

// Symbol set symbol
func (s *PremiumIndexKlines) Symbol(symbol string) *PremiumIndexKlines {
	s.symbol = symbol
	return s
}

// Interval set interval
func (s *PremiumIndexKlines) Interval(interval string) *PremiumIndexKlines {
	s.interval = interval
	return s
}

// StartTime set startTime
func (s *PremiumIndexKlines) StartTime(startTime int64) *PremiumIndexKlines {
	s.startTime = &startTime
	return s
}

// Endtime set endTime
func (s *PremiumIndexKlines) EndTime(endTime int64) *PremiumIndexKlines {
	s.endTime = &endTime
	return s
}

// Limit set limit
func (s *PremiumIndexKlines) Limit(limit int) *PremiumIndexKlines {
	s.limit = &limit
	return s
}

func (s *PremiumIndexKlines) Do(ctx context.Context, opts ...RequestOption) (res []*KlinesResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/premiumIndexKlines",
		secType:  secTypeNone,
	}
	r.setParam("symbol", s.symbol)
	r.setParam("interval", s.interval)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	return parseKlinesResponse(data)
}

// Binance Premium Index (GET /fapi/v1/premiumIndex)
type PremiumIndex struct {
	c      *Client
	symbol string
}

func (s *PremiumIndex) Symbol(symbol string) *PremiumIndex {
	s.symbol = symbol
	return s
}

func (s *PremiumIndex) Do(ctx context.Context, opts ...RequestOption) (res []*PremiumIndexResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/premiumIndex",
		secType:  secTypeNone,
	}
	r.setParam("symbol", s.symbol)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	var raw json.RawMessage
	err = Unmarshal(data, &raw)
	if err != nil {
		return []*PremiumIndexResponse{}, err
	}

	if raw[0] == '[' {
		res = make([]*PremiumIndexResponse, 0)
		err = Unmarshal(data, &res)
		if err != nil {
			return []*PremiumIndexResponse{}, err
		}
	} else {
		// The response is a single object, not an array, make sure to add it to the slice
		singleRes := new(PremiumIndexResponse)
		err = Unmarshal(data, &singleRes)
		if err != nil {
			return []*PremiumIndexResponse{}, err
		}
		res = append(res, singleRes)
	}
	return res, nil
}

type PremiumIndexResponse struct {
	Symbol               string `json:"symbol"`
	MarkPrice            string `json:"markPrice"`
	IndexPrice           string `json:"indexPrice"`
	EstimatedSettlePrice string `json:"estimatedSettlePrice"`
	LastFundingRate      string `json:"lastFundingRate"`
	InterestRate         string `json:"interestRate"`
	NextFundingTime      int64  `json:"nextFundingTime"`
	Time                 int64  `json:"time"`
}

// Binance Funding Rate (GET /fapi/v1/fundingRate)
type FundingRate struct {
	c         *Client
	symbol    *string
	startTime *int64
	endTime   *int64
	limit     *int
}

// Symbol set symbol
func (s *FundingRate) Symbol(symbol string) *FundingRate {
	s.symbol = &symbol
	return s
}

// StartTime set startTime
func (s *FundingRate) StartTime(startTime int64) *FundingRate {
	s.startTime = &startTime
	return s
}

// EndTime set endTime
func (s *FundingRate) EndTime(endTime int64) *FundingRate {
	s.endTime = &endTime
	return s
}

// Limit set limit
func (s *FundingRate) Limit(limit int) *FundingRate {
	s.limit = &limit
	return s
}

func (s *FundingRate) Do(ctx context.Context, opts ...RequestOption) (res []*FundingRateResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/fundingRate",
		secType:  secTypeNone,
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*FundingRateResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return []*FundingRateResponse{}, err
	}
	return res, nil
}

type FundingRateResponse struct {
	Symbol      string
	FundingRate string
	FundingTime int64
	MarkPrice   string
}

// Binance Funding Info (GET /fapi/v1/fundingInfo)
type FundingInfo struct {
	c *Client
}

func (s *FundingInfo) Do(ctx context.Context, opts ...RequestOption) (res []*FundingInfoResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/fundingInfo",
		secType:  secTypeNone,
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*FundingInfoResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return []*FundingInfoResponse{}, err
	}
	return res, nil
}

type FundingInfoResponse struct {
	Symbol                   string `json:"symbol"`
	AdjustedFundingRateCap   string `json:"adjustedFundingRateCap"`
	AdjustedFundingRateFloor string `json:"adjustedFundingRateFloor"`
	FundingIntervalHours     int    `json:"fundingIntervalHours"`
	Disclaimer               bool   `json:"disclaimer"`
}

// Binance Ticker 24hr (GET /fapi/v1/ticker/24hr)
type Ticker24hr struct {
	c      *Client
	symbol *string
}

func (s *Ticker24hr) Symbol(symbol string) *Ticker24hr {
	s.symbol = &symbol
	return s
}

func (s *Ticker24hr) Do(ctx context.Context, opts ...RequestOption) (res []*Ticker24hrResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/ticker/24hr",
		secType:  secTypeNone,
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	var raw json.RawMessage
	err = Unmarshal(data, &raw)
	if err != nil {
		return []*Ticker24hrResponse{}, err
	}

	if raw[0] == '[' {
		res = make([]*Ticker24hrResponse, 0)
		err = Unmarshal(data, &res)
		if err != nil {
			return []*Ticker24hrResponse{}, err
		}
	} else {
		// The response is a single object, not an array, make sure to add it to the slice
		singleRes := new(Ticker24hrResponse)
		err = Unmarshal(data, &singleRes)
		if err != nil {
			return []*Ticker24hrResponse{}, err
		}
		res = append(res, singleRes)
	}
	return res, nil
}

type Ticker24hrResponse struct {
	Symbol             string `json:"symbol"`
	PriceChange        string `json:"priceChange"`        // 24小时价格变动
	PriceChangePercent string `json:"priceChangePercent"` // 24小时价格变动百分比
	WeightedAvgPrice   string `json:"weightedAvgPrice"`   // 加权平均价
	LastPrice          string `json:"lastPrice"`          // 最近一次成交价
	LastQty            string `json:"lastQty"`            // 最近一次成交额
	OpenPrice          string `json:"openPrice"`          // 24小时内第一次成交的价格
	HighPrice          string `json:"highPrice"`          // 24小时最高价
	LowPrice           string `json:"lowPrice"`           // 24小时最低价
	Volume             string `json:"volume"`             // 24小时成交量
	QuoteVolume        string `json:"quoteVolume"`        // 24小时成交额
	OpenTime           int64  `json:"openTime"`           // 24小时内，第一笔交易的发生时间
	CloseTime          int64  `json:"closeTime"`          // 24小时内，最后一笔交易的发生时间
	FirstTradeID       int64  `json:"firstTradeID"`       // 首笔成交id
	LastTradeID        int64  `json:"lastTradeID"`        // 末笔成交id
	TradeCount         int64  `json:"tradeCount"`         // 成交笔数
}

// Binance Symbol Price Ticker (GET /fapi/v2/ticker/price)
type TickerPrice struct {
	c      *Client
	symbol *string
}

// Symbol set symbol
func (s *TickerPrice) Symbol(symbol string) *TickerPrice {
	s.symbol = &symbol
	return s
}

// Send the request
func (s *TickerPrice) Do(ctx context.Context, opts ...RequestOption) (res []*TickerPriceResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v2/ticker/price",
		secType:  secTypeNone,
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	var raw json.RawMessage
	err = Unmarshal(data, &raw)
	if err != nil {
		return []*TickerPriceResponse{}, err
	}

	if raw[0] == '[' {
		res = make([]*TickerPriceResponse, 0)
		err = Unmarshal(data, &res)
		if err != nil {
			return []*TickerPriceResponse{}, err
		}
	} else {
		// The response is a single object, not an array, make sure to add it to the slice
		singleRes := new(TickerPriceResponse)
		err = Unmarshal(data, &singleRes)
		if err != nil {
			return []*TickerPriceResponse{}, err
		}
		res = append(res, singleRes)
	}
	return res, nil
}

// Define TickerPrice response data
type TickerPriceResponse struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
	Time   int64  `json:"time"`
}

// Binance Symbol Book Ticker (GET /fapi/v1/ticker/bookTicker)
type BookTicker struct {
	c      *Client
	symbol *string
}

// Symbol set symbol
func (s *BookTicker) Symbol(symbol string) *BookTicker {
	s.symbol = &symbol
	return s
}

func (s *BookTicker) Do(ctx context.Context, opts ...RequestOption) (res []*BookTickerResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/ticker/bookTicker",
		secType:  secTypeNone,
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}

	var raw json.RawMessage
	err = Unmarshal(data, &raw)
	if err != nil {
		return []*BookTickerResponse{}, err
	}

	if raw[0] == '[' {
		res = make([]*BookTickerResponse, 0)
		err = Unmarshal(data, &res)
		if err != nil {
			return []*BookTickerResponse{}, err
		}
	} else {
		// The response is a single object, not an array, make sure to add it to the slice
		singleRes := new(BookTickerResponse)
		err = Unmarshal(data, &singleRes)
		if err != nil {
			return []*BookTickerResponse{}, err
		}
		res = append(res, singleRes)
	}
	return res, nil
}

type BookTickerResponse struct {
	Symbol   string `json:"symbol"`
	BidPrice string `json:"bidPrice"`
	BidQty   string `json:"bidQty"`
	AskPrice string `json:"askPrice"`
	AskQty   string `json:"askQty"`
	Time     int64  `json:"time"`
}

// Binance Delivery Price (GET /futures/data/delivery-price)
type DeliveryPrice struct {
	c    *Client
	pair string
}

func (s *DeliveryPrice) Pair(pair string) *DeliveryPrice {
	s.pair = pair
	return s
}

func (s *DeliveryPrice) Do(ctx context.Context, opts ...RequestOption) (res []*DeliveryPriceResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/futures/data/delivery-price",
		secType:  secTypeNone,
	}
	r.setParam("pair", s.pair)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*DeliveryPriceResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type DeliveryPriceResponse struct {
	DeliveryTime  int64   `json:"deliveryTime"`
	DeliveryPrice float64 `json:"deliveryPrice"`
}

// Binance Open Interest (GET /fapi/v1/openInterest)
type OpenInterest struct {
	c      *Client
	symbol string
}

func (s *OpenInterest) Symbol(symbol string) *OpenInterest {
	s.symbol = symbol
	return s
}

func (s *OpenInterest) Do(ctx context.Context, opts ...RequestOption) (res *OpenInterestResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/openInterest",
		secType:  secTypeNone,
	}
	r.addParam("symbol", s.symbol)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(OpenInterestResponse)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type OpenInterestResponse struct {
	OpenInterest string `json:"openInterest"`
	Symbol       string `json:"symbol"`
	Time         int64  `json:"time"`
}

// Binance Future Open Interest History (GET /futures/data/openInterestHist)
type OpenInterestHist struct {
	c         *Client
	symbol    string
	period    string
	limit     *int64
	startTime *int64
	endTime   *int64
}

func (s *OpenInterestHist) Symbol(symbol string) *OpenInterestHist {
	s.symbol = symbol
	return s
}

func (s *OpenInterestHist) Period(period string) *OpenInterestHist {
	s.period = period
	return s
}

func (s *OpenInterestHist) Limit(limit int64) *OpenInterestHist {
	s.limit = &limit
	return s
}

func (s *OpenInterestHist) StartTime(startTime int64) *OpenInterestHist {
	s.startTime = &startTime
	return s
}

func (s *OpenInterestHist) EndTime(endTime int64) *OpenInterestHist {
	s.endTime = &endTime
	return s
}

func (s *OpenInterestHist) Do(ctx context.Context, opts ...RequestOption) (res []*OpenInterestHistResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/futures/data/openInterestHist",
		secType:  secTypeNone,
	}
	r.setParam("symbol", s.symbol)
	r.setParam("period", s.period)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}

	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*OpenInterestHistResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type OpenInterestHistResponse struct {
	Symbol               string `json:"symbol"`
	SumOpenInterest      string `json:"sumOpenInterest"`
	SumOpenInterestValue string `json:"sumOpenInterestValue"`
	Timestamp            int64  `json:"timestamp"`
}

// Binance Top Long/Short Position Ratio (GET /futures/data/topLongShortPositionRatio)
// 大户持仓量多空比
// 大户的多头和空头总持仓量占比，大户指保证金余额排名前20%的用户。
// 多仓持仓量比例 = 大户多仓持仓量 / 大户总持仓量
// 空仓持仓量比例 = 大户空仓持仓量 / 大户总持仓量
// 多空持仓量比值 = 多仓持仓量比例 / 空仓持仓量比例
type PositionRatio struct {
	c         *Client
	symbol    string
	period    string
	limit     *int64
	startTime *int64
	endTime   *int64
}

func (s *PositionRatio) Symbol(symbol string) *PositionRatio {
	s.symbol = symbol
	return s
}

func (s *PositionRatio) Period(period string) *PositionRatio {
	s.period = period
	return s
}

func (s *PositionRatio) Limit(limit int64) *PositionRatio {
	s.limit = &limit
	return s
}

func (s *PositionRatio) StartTime(startTime int64) *PositionRatio {
	s.startTime = &startTime
	return s
}

func (s *PositionRatio) EndTime(endTime int64) *PositionRatio {
	s.endTime = &endTime
	return s
}

// Do send request
// 若无 startime 和 endtime 限制， 则默认返回当前时间往前的limit值
// 仅支持最近30天的数据
// IP限频为1000次/5min
func (s *PositionRatio) Do(ctx context.Context) (res []*PositionRatioResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/futures/data/topLongShortPositionRatio",
		secType:  secTypeNone,
	}
	r.setParam("symbol", s.symbol)
	r.setParam("period", s.period)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	data, err := s.c.callAPI(ctx, r)
	if err != nil {
		return nil, err
	}
	res = make([]*PositionRatioResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type PositionRatioResponse struct {
	Symbol         string `json:"symbol"`
	LongShortRatio string `json:"longShortRatio"` // 大户多空持仓量比值
	LongAccount    string `json:"longAccount"`    // 大户多仓持仓量比例
	ShortAccount   string `json:"shortAccount"`   // 大户空仓持仓量比例
	Timestamp      int64  `json:"timestamp"`
}

// Binance Top Long/Short Account Ratio (GET /futures/data/topLongShortAccountRatio)
// 大户账户数多空比
// 持仓大户的净持仓多头和空头账户数占比，大户指保证金余额排名前20%的用户。一个账户记一次。
// 多仓账户数比例 = 持多仓大户数 / 总持仓大户数
// 空仓账户数比例 = 持空仓大户数 / 总持仓大户数
// 多空账户数比值 = 多仓账户数比例 / 空仓账户数比例
type AccountRatio struct {
	c         *Client
	symbol    string
	period    string
	limit     *int64
	startTime *int64
	endTime   *int64
}

func (s *AccountRatio) Symbol(symbol string) *AccountRatio {
	s.symbol = symbol
	return s
}

func (s *AccountRatio) Period(period string) *AccountRatio {
	s.period = period
	return s
}

func (s *AccountRatio) Limit(limit int64) *AccountRatio {
	s.limit = &limit
	return s
}

func (s *AccountRatio) StartTime(startTime int64) *AccountRatio {
	s.startTime = &startTime
	return s
}

func (s *AccountRatio) EndTime(endTime int64) *AccountRatio {
	s.endTime = &endTime
	return s
}

// Do send request
// 若无 startime 和 endtime 限制， 则默认返回当前时间往前的limit值
// 仅支持最近30天的数据
// IP限频为1000次/5min
func (s *AccountRatio) Do(ctx context.Context) (res []*AccountRatioResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/futures/data/topLongShortAccountRatio",
		secType:  secTypeNone,
	}
	r.setParam("symbol", s.symbol)
	r.setParam("period", s.period)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	data, err := s.c.callAPI(ctx, r)
	if err != nil {
		return nil, err
	}
	res = make([]*AccountRatioResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type AccountRatioResponse struct {
	Symbol         string `json:"symbol"`
	LongShortRatio string `json:"longShortRatio"` // 大户多空账户数比值
	LongAccount    string `json:"longAccount"`    // 大户多仓账户数比例
	ShortAccount   string `json:"shortAccount"`   // 大户空仓账户数比例
	Timestamp      int64  `json:"timestamp"`
}

// Binance Global Long/Short Account Ratio (GET /futures/data/globalLongShortAccountRatio)
// 多空持仓人数比
type GlobalAccountRatio struct {
	c         *Client
	symbol    string
	period    string
	limit     *int64
	startTime *int64
	endTime   *int64
}

func (s *GlobalAccountRatio) Symbol(symbol string) *GlobalAccountRatio {
	s.symbol = symbol
	return s
}

func (s *GlobalAccountRatio) Period(period string) *GlobalAccountRatio {
	s.period = period
	return s
}

func (s *GlobalAccountRatio) Limit(limit int64) *GlobalAccountRatio {
	s.limit = &limit
	return s
}

func (s *GlobalAccountRatio) StartTime(startTime int64) *GlobalAccountRatio {
	s.startTime = &startTime
	return s
}

func (s *GlobalAccountRatio) EndTime(endTime int64) *GlobalAccountRatio {
	s.endTime = &endTime
	return s
}

// Do send request
// 若无 startime 和 endtime 限制， 则默认返回当前时间往前的limit值
// 仅支持最近30天的数据
// IP限频为1000次/5min
func (s *GlobalAccountRatio) Do(ctx context.Context) (res []*GlobalAccountRatioResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/futures/data/globalLongShortAccountRatio",
		secType:  secTypeNone,
	}
	r.setParam("symbol", s.symbol)
	r.setParam("period", s.period)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	data, err := s.c.callAPI(ctx, r)
	if err != nil {
		return nil, err
	}
	res = make([]*GlobalAccountRatioResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type GlobalAccountRatioResponse struct {
	Symbol         string `json:"symbol"`
	LongShortRatio string `json:"longShortRatio"` // 多空持仓人数比
	LongAccount    string `json:"longAccount"`    // 多仓人数比例
	ShortAccount   string `json:"shortAccount"`   // 空仓人数比例
	Timestamp      int64  `json:"timestamp"`
}

// Binance Taker Long/Short Ratio (GET /futures/data/takerlongshortRatio)
// 合约主动买卖量
type TakerRatio struct {
	c         *Client
	symbol    string
	period    string
	limit     *int64
	startTime *int64
	endTime   *int64
}

func (s *TakerRatio) Symbol(symbol string) *TakerRatio {
	s.symbol = symbol
	return s
}

func (s *TakerRatio) Period(period string) *TakerRatio {
	s.period = period
	return s
}

func (s *TakerRatio) Limit(limit int64) *TakerRatio {
	s.limit = &limit
	return s
}

func (s *TakerRatio) StartTime(startTime int64) *TakerRatio {
	s.startTime = &startTime
	return s
}

func (s *TakerRatio) EndTime(endTime int64) *TakerRatio {
	s.endTime = &endTime
	return s
}

// Do send request
// 若无 startime 和 endtime 限制， 则默认返回当前时间往前的limit值
// 仅支持最近30天的数据
// IP限频为1000次/5min
func (s *TakerRatio) Do(ctx context.Context) (res []*TakerRatioResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/futures/data/takerlongshortRatio",
		secType:  secTypeNone,
	}
	r.setParam("symbol", s.symbol)
	r.setParam("period", s.period)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	data, err := s.c.callAPI(ctx, r)
	if err != nil {
		return nil, err
	}
	res = make([]*TakerRatioResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type TakerRatioResponse struct {
	BuySellRatio string `json:"buySellRatio"`
	BuyVol       string `json:"buyVol"`  // 主动买入量
	SellVol      string `json:"sellVol"` // 主动卖出量
	Timestamp    int64  `json:"timestamp"`
}

// Binance Basis (GET /futures/data/basis)
// 查询期货基差
type Basis struct {
	c            *Client
	pair         string
	contractType ContractType // 合约类型
	period       string       // 时间周期
	limit        int64
	startTime    *int64
	endTime      *int64
}

func (s *Basis) Pair(pair string) *Basis {
	s.pair = pair
	return s
}

func (s *Basis) ContractType(contractType ContractType) *Basis {
	s.contractType = contractType
	return s
}

func (s *Basis) Period(period string) *Basis {
	s.period = period
	return s
}

func (s *Basis) Limit(limit int64) *Basis {
	s.limit = limit
	return s
}

func (s *Basis) StartTime(startTime int64) *Basis {
	s.startTime = &startTime
	return s
}

func (s *Basis) EndTime(endTime int64) *Basis {
	s.endTime = &endTime
	return s
}

// Do send request
// 若无 startime 和 endtime 限制， 则默认返回当前时间往前的limit值
// 仅支持最近30天的数据
func (s *Basis) Do(ctx context.Context) (res []*BasisResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/futures/data/basis",
		secType:  secTypeNone,
	}
	r.setParam("pair", s.pair)
	r.setParam("contractType", s.contractType)
	r.setParam("period", s.period)
	if s.limit != 0 {
		r.setParam("limit", s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	data, err := s.c.callAPI(ctx, r)
	if err != nil {
		return nil, err
	}
	res = make([]*BasisResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type BasisResponse struct {
	IndexPrice          string       `json:"indexPrice"`
	ContractType        ContractType `json:"contractType"`
	BasisRate           string       `json:"basisRate"`
	FuturePrice         string       `json:"futurePrice"`
	AnnualizedBasisRate string       `json:"annualizedBasisRate"`
	Basis               string       `json:"basis"`
	Pair                string       `json:"pair"`
	Timestamp           int64        `json:"timestamp"`
}

// Binance Index Info (GET /fapi/v1/indexInfo)
// 综合指数交易对信息
// 获取交易对为综合指数的基础成分信息
type IndexInfo struct {
	c      *Client
	symbol *string
}

func (s *IndexInfo) Symbol(symbol string) *IndexInfo {
	s.symbol = &symbol
	return s
}

// Do send request
func (s *IndexInfo) Do(ctx context.Context) (res []*IndexInfoResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/indexInfo",
		secType:  secTypeSigned,
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	data, err := s.c.callAPI(ctx, r)
	if err != nil {
		return nil, err
	}
	res = make([]*IndexInfoResponse, 0)
	err = Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type IndexInfoResponse struct {
	Symbol        string      `json:"symbol"`
	Time          int64       `json:"time"`      // 请求时间
	Component     string      `json:"component"` //成分资产
	BaseAssetList []BaseAsset `json:"baseAssetList"`
}

type BaseAsset struct {
	BaseAsset          string // 基础资产
	QuoteAsset         string // 报价资产
	WeightInQuanitty   string //权重(数量)
	WeightInPercentage string //权重(比例)
}

// Binance Asset Index (GET /fapi/v1/assetIndex)
// 多资产模式资产汇率指数
type AssetIndex struct {
	c      *Client
	symbol *string
}

func (s *AssetIndex) Symbol(symbol string) *AssetIndex {
	s.symbol = &symbol
	return s
}

// Do send request
func (s *AssetIndex) Do(ctx context.Context) (res []*AssetIndexResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/assetIndex",
		secType:  secTypeSigned,
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	data, err := s.c.callAPI(ctx, r)
	if err != nil {
		return nil, err
	}
	var raw json.RawMessage
	err = Unmarshal(data, &raw)
	if err != nil {
		return []*AssetIndexResponse{}, err
	}

	if raw[0] == '[' {
		res = make([]*AssetIndexResponse, 0)
		err = Unmarshal(data, &res)
		if err != nil {
			return []*AssetIndexResponse{}, err
		}
	} else {
		// The response is a single object, not an array, make sure to add it to the slice
		singleRes := new(AssetIndexResponse)
		err = Unmarshal(data, &singleRes)
		if err != nil {
			return []*AssetIndexResponse{}, err
		}
		res = append(res, singleRes)
	}
	return res, nil
}

type AssetIndexResponse struct {
	Symbol                string `json:"symbol"`
	Time                  int64  `json:"time"`
	Index                 string `json:"index"`
	BidBuffer             string `json:"bidBuffer"`
	AskBuffer             string `json:"askBuffer"`
	BidRate               string `json:"bidRate"`
	AskRate               string `json:"askRate"`
	AutoExchangeBidBuffer string `json:"autoExchangeBidBuffer"`
	AutoExchangeAskBuffer string `json:"autoExchangeAskBuffer"`
	AutoExchangeBidRate   string `json:"autoExchangeBidRate"`
	AutoExchangeAskRate   string `json:"autoExchangeAskRate"`
}

// Binance Constituents (GET /fapi/v1/constituents)
// 查询指数价格成分
type Constituents struct {
	c      *Client
	symbol string
}

func (s *Constituents) Symbol(symbol string) *Constituents {
	s.symbol = symbol
	return s
}

func (s *Constituents) Do(ctx context.Context) (res []*ConstituentsResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/constituents",
		secType:  secTypeSigned,
	}
	if s.symbol != "" {
		r.setParam("symbol", s.symbol)
	}
	data, err := s.c.callAPI(ctx, r)
	if err != nil {
		return nil, err
	}
	res = make([]*ConstituentsResponse, 0)
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type ConstituentsResponse struct {
	Symbol       string        `json:"symbol"`
	Time         int64         `json:"time"`
	Constituents []Constituent `json:"constituents"`
}

type Constituent struct {
	Exchange string `json:"exchange"`
	Symbol   string `json:"symbol"`
	Price    string `json:"price"`
	Weight   string `json:"weight"`
}

// Binance Insurance Balance (GET /fapi/v1/insuranceBalance)
// 查询保险基金余额快照
type InsuranceBalance struct {
	c      *Client
	symbol *string
}

func (s *InsuranceBalance) Symbol(symbol string) *InsuranceBalance {
	s.symbol = &symbol
	return s
}

func (s *InsuranceBalance) Do(ctx context.Context) (res []*InsuranceBalanceResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/insuranceBalance",
		secType:  secTypeSigned,
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	data, err := s.c.callAPI(ctx, r)
	if err != nil {
		return nil, err
	}
	var raw json.RawMessage
	err = Unmarshal(data, &raw)
	if err != nil {
		return []*InsuranceBalanceResponse{}, err
	}

	if raw[0] == '[' {
		res = make([]*InsuranceBalanceResponse, 0)
		err = Unmarshal(data, &res)
		if err != nil {
			return []*InsuranceBalanceResponse{}, err
		}
	} else {
		// The response is a single object, not an array, make sure to add it to the slice
		singleRes := new(InsuranceBalanceResponse)
		err = Unmarshal(data, &singleRes)
		if err != nil {
			return []*InsuranceBalanceResponse{}, err
		}
		res = append(res, singleRes)
	}
	return res, nil
}

type InsuranceBalanceResponse struct {
	Symbols []string `json:"symbols"`
	Assets  []Asset  `json:"assets"`
}
