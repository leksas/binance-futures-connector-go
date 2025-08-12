package binance_futures_connector

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/binance/binance-connector-go/handlers"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson"
)

// TimeInForceType define time in force type of order
type TimeInForceType string

// UserDataEventType define spot user data event type
type UserDataEventType string

// Client define API client
type Client struct {
	APIKey            string
	SecretKey         string
	Ed25519APIKey     string
	Ed25519PrivateKey ed25519.PrivateKey
	UseEd25519        bool
	BaseURL           string
	HTTPClient        *http.Client
	Debug             bool
	Logger            *log.Logger
	TimeOffset        int64
	useWeight1m       int
	do                doFunc
}

type doFunc func(req *http.Request) (*http.Response, error)

// Globals
const (
	timestampKey  = "timestamp"
	signatureKey  = "signature"
	recvWindowKey = "recvWindow"
)

func currentTimestamp() int64 {
	return FormatTimestamp(time.Now())
}

// FormatTimestamp formats a time into Unix timestamp in milliseconds, as requested by Binance.
func FormatTimestamp(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func PrettyPrint(i interface{}) string {
	s, _ := MarshalIndent(i, "", "  ")
	return string(s)
}

func (c *Client) debug(format string, v ...interface{}) {
	if c.Debug {
		c.Logger.Printf(format, v...)
	}
}

// Create client function for initialising new Binance client
func NewClient(apiKey string, secretKey string, baseURL ...string) *Client {
	url := "https://fapi.binance.com"

	if len(baseURL) > 0 {
		url = baseURL[0]
	}

	return &Client{
		APIKey:     apiKey,
		SecretKey:  secretKey,
		BaseURL:    url,
		HTTPClient: http.DefaultClient,
		Logger:     log.New(os.Stderr, Name, log.LstdFlags),
		UseEd25519: false,
	}
}

func (c *Client) UseEd25519Keys(apiKey string, privateKey ed25519.PrivateKey) {
	c.Ed25519APIKey = apiKey
	c.Ed25519PrivateKey = privateKey
	c.UseEd25519 = true
}

func (c *Client) UseWeight1m() int {
	return c.useWeight1m
}

func (c *Client) parseRequest(r *request, opts ...RequestOption) (err error) {
	// set request options from user
	for _, opt := range opts {
		opt(r)
	}
	err = r.validate()
	if err != nil {
		return err
	}

	fullURL := fmt.Sprintf("%s%s", c.BaseURL, r.endpoint)
	if r.recvWindow > 0 {
		r.setParam(recvWindowKey, r.recvWindow)
	}
	if r.secType == secTypeSigned {
		r.setParam(timestampKey, currentTimestamp()-c.TimeOffset)
	}
	queryString := r.query.Encode()
	body := &bytes.Buffer{}
	bodyString := r.form.Encode()
	header := http.Header{}
	if r.header != nil {
		header = r.header.Clone()
	}
	header.Set("User-Agent", fmt.Sprintf("%s/%s", Name, Version))
	if bodyString != "" {
		header.Set("Content-Type", "application/x-www-form-urlencoded")
		body = bytes.NewBufferString(bodyString)
	}
	if r.secType == secTypeAPIKey || r.secType == secTypeSigned {
		if c.UseEd25519 {
			header.Set("X-MBX-APIKEY", c.Ed25519APIKey)
		} else {
			header.Set("X-MBX-APIKEY", c.APIKey)
		}
	}

	if r.secType == secTypeSigned {
		signature, err := c.sign(queryString, bodyString)
		if err != nil {
			return err
		}
		v := url.Values{}
		v.Set(signatureKey, signature)
		if queryString == "" {
			queryString = v.Encode()
		} else {
			queryString = fmt.Sprintf("%s&%s", queryString, v.Encode())
		}
	}
	if queryString != "" {
		fullURL = fmt.Sprintf("%s?%s", fullURL, queryString)
	}
	c.debug("full url: %s, body: %s", fullURL, bodyString)
	r.fullURL = fullURL
	r.header = header
	r.body = body
	return nil
}

func (c *Client) sign(queryString, bodyString string) (string, error) {
	if c.UseEd25519 {
		return c.ed25519Sign(queryString, bodyString), nil
	}
	return c.hmacSign(queryString, bodyString)
}

func (c *Client) hmacSign(queryString, bodyString string) (string, error) {
	raw := fmt.Sprintf("%s%s", queryString, bodyString)
	mac := hmac.New(sha256.New, []byte(c.SecretKey))
	_, err := mac.Write([]byte(raw))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", (mac.Sum(nil))), nil
}

func (c *Client) ed25519Sign(queryString, bodyString string) string {
	raw := fmt.Sprintf("%s%s", queryString, bodyString)
	return base64.StdEncoding.EncodeToString(ed25519.Sign(c.Ed25519PrivateKey, []byte(raw)))
}

func (c *Client) UseFastHTTPClient() {
	c.do = doWithFastHTTP
}

// convertToFastHTTPRequest converts http.Request to fasthttp.Request
func convertToFastHTTPRequest(httpReq *http.Request) (*fasthttp.Request, error) {
	fastReq := fasthttp.AcquireRequest()
	fastReq.SetRequestURI(httpReq.URL.String())
	fastReq.Header.SetMethod(httpReq.Method)

	// Copy headers
	for key, values := range httpReq.Header {
		for _, value := range values {
			fastReq.Header.Add(key, value)
		}
	}

	// Copy request body
	if httpReq.Body != nil {
		body, err := io.ReadAll(httpReq.Body)
		if err != nil {
			fasthttp.ReleaseRequest(fastReq)
			return nil, err
		}
		fastReq.SetBody(body)
	}

	return fastReq, nil
}

// convertFromFastHTTPResponse converts fasthttp.Response to http.Response
func convertFromFastHTTPResponse(fastResp *fasthttp.Response) (*http.Response, error) {
	httpResp := &http.Response{
		StatusCode: fastResp.StatusCode(),
		Header:     make(http.Header),
	}

	// Copy headers
	for key, value := range fastResp.Header.All() {
		httpResp.Header.Add(string(key), string(value))
	}

	// Set response body
	httpResp.Body = io.NopCloser(bytes.NewReader(fastResp.Body()))

	return httpResp, nil
}

func doWithFastHTTP(httpReq *http.Request) (*http.Response, error) {
	fastReq, err := convertToFastHTTPRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer fasthttp.ReleaseRequest(fastReq)

	fastResp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(fastResp)

	client := &fasthttp.Client{
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
	}
	err = client.Do(fastReq, fastResp)
	if err != nil {
		return nil, err
	}

	return convertFromFastHTTPResponse(fastResp)
}

func (c *Client) callAPI(ctx context.Context, r *request, opts ...RequestOption) (data []byte, err error) {
	err = c.parseRequest(r, opts...)
	if r.endpoint != "/api/v3/order/cancelReplace" {
		if err != nil {
			return []byte{}, err
		}
	}
	req, err := http.NewRequest(r.method, r.fullURL, r.body)
	if err != nil {
		return []byte{}, err
	}
	req = req.WithContext(ctx)
	req.Header = r.header
	c.debug("request: %#v", req)
	f := c.do
	if f == nil {
		f = c.HTTPClient.Do
	}
	res, err := f(req)
	if err != nil {
		return []byte{}, err
	}
	c.useWeight1m, _ = strconv.Atoi(res.Header.Get("X-Mbx-Used-Weight-1m"))
	data, err = io.ReadAll(res.Body)
	if err != nil {
		return []byte{}, err
	}
	defer func() {
		cerr := res.Body.Close()
		// Only overwrite the retured error if the original error was nil and an
		// error occurred while closing the body.
		if err == nil && cerr != nil {
			err = cerr
		}
	}()
	c.debug("response: %#v", res)
	c.debug("response body: %s", string(data))
	c.debug("response status code: %d", res.StatusCode)

	if res.StatusCode >= http.StatusBadRequest {
		apiErr := new(handlers.APIError)
		e := Unmarshal(data, apiErr)
		if e != nil {
			c.debug("failed to unmarshal json: %s", e)
		}
		if r.endpoint != "/api/v3/order/cancelReplace" {
			return nil, apiErr
		}
	}
	return data, nil
}

func newJSONV2(data []byte) (*fastjson.Value, error) {
	var parser fastjson.Parser
	v, err := parser.ParseBytes(data)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// Binance Ping (GET /fapi/v1/ping)
// 测试服务器连通性
func (c *Client) NewPingService() *Ping {
	return &Ping{c: c}
}

// Binance Server Time (GET /fapi/v1/time)
// 获取服务器时间
func (c *Client) NewServerTimeService() *ServerTime {
	return &ServerTime{c: c}
}

// Binance Exchange Info (GET /fapi/v1/exchangeInfo)
// 获取交易规则和符号信息
func (c *Client) NewExchangeInfoService() *ExchangeInfo {
	return &ExchangeInfo{c: c}
}

// Binance Order Book (GET /fapi/v1/depth)
// 获取深度信息
func (c *Client) NewOrderBookService() *OrderBook {
	return &OrderBook{c: c}
}

// Binance Recent Trades List (GET /fapi/v1/trades)
// 近期成交
func (c *Client) NewRecentTradesListService() *RecentTradesList {
	return &RecentTradesList{c: c}
}

// Binance Historical Trades Lookup (GET /fapi/v1/historicalTrades)
// 查询历史成交
func (c *Client) NewHistoricalTradeLookupService() *HistoricalTradeLookup {
	return &HistoricalTradeLookup{c: c}
}

// Binance Aggregate Trades List (GET /fapi/v1/aggTrades)
// 近期成交(归集)
func (c *Client) NewAggTradesListService() *AggTradesList {
	return &AggTradesList{c: c}
}

// Binance Kline Data (GET /fapi/v1/klines)
// k 线数据
func (c *Client) NewKlinesService() *Klines {
	return &Klines{c: c}
}

// Binance Continuous Kline Data (GET /fapi/v1/continuousKlines)
// 连续合约 k 线数据
func (c *Client) NewContinuousKlinesService() *ContinuousKlines {
	return &ContinuousKlines{c: c}
}

// Binance Index Price Kline Data (GET /fapi/v1/indexPriceKlines)
// 价格指数 k 线数据
func (c *Client) NewIndexPriceKlinesService() *IndexPriceKlines {
	return &IndexPriceKlines{c: c}
}

// Binance Mark Price Kline Data (GET /fapi/v1/markPriceKlines)
// 标记价格 k 线数据
func (c *Client) NewMarkPriceKlinesService() *MarkPriceKlines {
	return &MarkPriceKlines{c: c}
}

// Binance Premium Index Klines Data (GET /fapi/v1/premiumIndexKlines)
// 溢价指数K线数据
func (c *Client) NewPremiumIndexKlinesService() *PremiumIndexKlines {
	return &PremiumIndexKlines{c: c}
}

// Binance Premium Index Data (GET /fapi/v1/premiumIndex)
// 最新标记价格和资金费率
func (c *Client) NewPremiumIndexService() *PremiumIndex {
	return &PremiumIndex{c: c}
}

// Binance Funding Rate Data (GET /fapi/v1/fundingRate)
// 查询资金费率历史
func (c *Client) NewFundingRateService() *FundingRate {
	return &FundingRate{c: c}
}

// Binance Funding Info Data (GET /fapi/v1/fundingInfo)
// 查询资金费率信息
func (c *Client) NewFundingInfoService() *FundingInfo {
	return &FundingInfo{c: c}
}

// Binance Ticker 24hr Data (GET /fapi/v1/ticker/24hr)
// 24小时价格变动情况
func (c *Client) NewTicker24hrService() *Ticker24hr {
	return &Ticker24hr{c: c}
}

// Binance Ticker Price Data (GET /fapi/v1/ticker/price)
// 最新价格
func (c *Client) NewTickerPriceService() *TickerPrice {
	return &TickerPrice{c: c}
}

// Binance Ticker BookTicker Data (GET /fapi/v1/ticker/bookTicker)
// 当前最优挂单
func (c *Client) NewBookTickerService() *BookTicker {
	return &BookTicker{c: c}
}

// Binance Delivery Price (GET /fapi/v1/deliveryPrice)
// 季度合约历史结算价
func (c *Client) NewDeliveryPriceService() *DeliveryPrice {
	return &DeliveryPrice{c: c}
}

// Binance Open Interest Data (GET /fapi/v1/openInterest)
// 获取未平仓合约数
func (c *Client) NewOpenInterestService() *OpenInterest {
	return &OpenInterest{c: c}
}

// Binance Open Interest History Data (GET /fapi/v1/openInterestHist)
// 合约持仓量历史
func (c *Client) NewOpenInterestHistService() *OpenInterestHist {
	return &OpenInterestHist{c: c}
}

// Binance Position Account Ratio (GET /futures/data/topLongShortPositionRatio)
// 大户持仓量多空比
func (c *Client) NewPositionRatioService() *PositionRatio {
	return &PositionRatio{c: c}
}

// Binance Top Long/Short Account Ratio (GET /futures/data/topLongShortAccountRatio)
// 大户账户数多空比
func (c *Client) NewAccountRatioService() *AccountRatio {
	return &AccountRatio{c: c}
}

// Binance Global Account Ratio (GET /futures/data/globalLongShortAccountRatio)
// 多空持仓人数比
func (c *Client) NewGlobalAccountRatioService() *GlobalAccountRatio {
	return &GlobalAccountRatio{c: c}
}

// Binance Taker Long/Short Ratio (GET /futures/data/takerlongshortRatio)
// 合约主动买卖量
func (c *Client) NewTakerRatioService() *TakerRatio {
	return &TakerRatio{c: c}
}

// Binance Basis (GET /futures/data/basis)
// 查询期货基差
func (c *Client) NewBasisService() *Basis {
	return &Basis{c: c}
}

// Binance Index Info (GET /fapi/v1/indexInfo)
// 综合指数交易对信息
// 获取交易对为综合指数的基础成分信息
func (c *Client) NewIndexInfoService() *IndexInfo {
	return &IndexInfo{c: c}
}

// Binance Asset Index (GET /fapi/v1/assetIndex)
// 多资产模式资产汇率指数
func (c *Client) NewAssetIndexService() *AssetIndex {
	return &AssetIndex{c: c}
}

// Binance Constituents (GET /fapi/v1/constituents)
// 查询指数价格成分
func (c *Client) NewIndexConstituentsService() *Constituents {
	return &Constituents{c: c}
}

// Binance Insurance Balance (GET /fapi/v1/insuranceBalance)
// 查询保险基金余额快照
func (c *Client) NewInsuranceBalanceService() *InsuranceBalance {
	return &InsuranceBalance{c: c}
}

// -----------------------User Data API---------------------------------

// Binance Create Listen Key (POST /fapi/v1/listenKey)
// 生成listenKey
func (c *Client) NewCreateListenKeyService() *CreateListenKey {
	return &CreateListenKey{c: c}
}

// Binance Ping User Stream (PUT /fapi/v1/listenKey)
// 延长listenKey有效期
func (c *Client) NewPingUserStream() *PingUserStream {
	return &PingUserStream{c: c}
}

// -------------------------Account API---------------------------------

// Binance Get Balance (GET /fapi/v1/balance)
// 获取账户余额
func (c *Client) NewGetBalanceService() *GetBalanceService {
	return &GetBalanceService{c: c}
}

// Binance Get Account (GET /fapi/v1/account)
// 获取账户信息
func (c *Client) NewGetAccountService() *GetAccountService {
	return &GetAccountService{c: c}
}

// Binance Get Commission Rate (GET /fapi/v1/commissionRate)
// 获取手续费率
func (c *Client) NewGetCommissionRateService() *CommissionRateService {
	return &CommissionRateService{c: c}
}

// Binance Get Account Config (GET /fapi/v1/account)
// 获取账户配置信息
func (c *Client) NewGetAccountConfigService() *AccountConfigService {
	return &AccountConfigService{c: c}
}

// ---------------------------Trade API---------------------------------

// Binance Create Order (POST /fapi/v1/order)
// 下单
func (c *Client) NewCreateOrderService() *CreateOrderService {
	return &CreateOrderService{c: c}
}

// Binance Cancel All Open Orders (DELETE /fapi/v1/allOpenOrders)
// 撤销全部订单
func (c *Client) NewCancelOpenOrdersService() *CancelAllOpenOrdersService {
	return &CancelAllOpenOrdersService{c: c}
}

// Binance Get Open Orders (GET /fapi/v1/openOrders)
// 查看当前全部挂单
func (c *Client) NewGetOpenOrdersService() *GetOpenOrdersService {
	return &GetOpenOrdersService{c: c}
}

// Binance Set Position Side Dual (POST /fapi/v1/positionSide/dual)
// 更改持仓模式
func (c *Client) NewSetPositionSideDualService() *PositionSideDualService {
	return &PositionSideDualService{c: c}
}

// Binance Set Leverage (POST /fapi/v1/leverage)
// 调整开仓杠杆
func (c *Client) NewSetLeverageService() *SetLeverageService {
	return &SetLeverageService{c: c}
}

// Binance Set Multi-Asset Margin (POST /fapi/v1/multiAssetsMargin)
// 更改联合保证金模式
func (c *Client) NewSetMultiAssetMarginService() *SetMultiAssetMarginService {
	return &SetMultiAssetMarginService{c: c}
}
