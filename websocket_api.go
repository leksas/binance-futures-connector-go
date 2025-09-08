package binance_futures_connector

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

type WebsocketAPIClient struct {
	APIKey            string
	APISecret         string
	Ed25519APIKey     string
	Ed25519PrivateKey ed25519.PrivateKey
	UseEd25519        bool
	UseSBE            bool
	Endpoint          string
	Conn              *websocket.Conn
	Dialer            *websocket.Dialer
	ReqResponseMap    sync.Map
	Mu                sync.Mutex
	BindIP            string

	stopChan       chan struct{}
	maxBackoff     time.Duration // 最大退避间隔
	initialBackoff time.Duration // 初始退避间隔
}

type WsAPIRateLimit struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   int    `json:"intervalNum"`
	Limit         int    `json:"limit"`
	Count         int    `json:"count"`
}

type WsAPIErrorResponse struct {
	Code    int    `json:"code"`
	ID      string `json:"id"`
	Message string `json:"msg"`
}

var (
	// WebsocketAPITimeout is an interval for sending ping/pong messages if WebsocketKeepalive is enabled
	WebsocketAPITimeout = time.Second * 60
	// WebsocketAPIKeepalive enables sending ping/pong messages to check the connection stability
	WebsocketAPIKeepalive = true
)

func NewWebsocketAPIClient(apiKey string, apiSecret string, baseURL ...string) *WebsocketAPIClient {
	// Set default base URL to production WS URL
	url := "wss://ws-fapi.binance.com/ws-fapi/v1"

	if len(baseURL) > 0 {
		url = baseURL[0]
	}

	return &WebsocketAPIClient{
		APIKey:    apiKey,
		APISecret: apiSecret,
		Endpoint:  url,
		Dialer: &websocket.Dialer{
			Proxy:             http.ProxyFromEnvironment,
			HandshakeTimeout:  45 * time.Second,
			EnableCompression: false,
		},
		initialBackoff: 1 * time.Second,  // 初始重试间隔
		maxBackoff:     30 * time.Second, // 最大重试间隔，避免间隔过大
	}
}

func NewEdWebsocketAPIClient(apiKey string, privateKey string, baseURL ...string) (*WebsocketAPIClient, error) {
	client := NewWebsocketAPIClient("", "", baseURL...)

	pk, err := ParseEd25519PrivateKey(privateKey)
	if err != nil {
		return nil, err
	}
	client.UseEd25519Keys(apiKey, pk)
	return client, nil
}

func (c *WebsocketAPIClient) UseEd25519Keys(apiKey string, privateKey ed25519.PrivateKey) {
	c.APIKey = apiKey
	c.Ed25519APIKey = apiKey
	c.Ed25519PrivateKey = privateKey
	c.UseEd25519 = true
}

func (c *WebsocketAPIClient) UseSBEStreams() {
	c.UseSBE = true
	if strings.Contains(c.Endpoint, "responseFormat=sbe") {
		return // 已经包含SBE参数则直接返回
	}

	// 检查URL是否已有查询参数
	separator := "?"
	if strings.Contains(c.Endpoint, "?") {
		separator = "&"
	}

	// 安全地追加SBE参数
	c.Endpoint = fmt.Sprintf("%s%sresponseFormat=sbe&sbeSchemaId=2&sbeSchemaVersion=1",
		c.Endpoint, separator)
}

func (c *WebsocketAPIClient) SetBindIP(ip string) {
	if ip == "" {
		return
	}

	c.Endpoint = "wss://ws-fapi-mm.binance.com/ws-fapi/v1"
	c.Dialer.NetDial = func(network, addr string) (net.Conn, error) {
		lAddr, err := net.ResolveTCPAddr(network, ip+":0")
		if err != nil {
			return nil, err
		}
		dialer := net.Dialer{
			LocalAddr: lAddr,
		}
		return dialer.Dial(network, addr)
	}
}

func (c *WebsocketAPIClient) Connect() error {
	if c.Dialer == nil {
		return fmt.Errorf("dialer not initialized")
	}

	// 初始化关闭信号通道
	if c.stopChan == nil {
		c.stopChan = make(chan struct{})
	}

	headers := http.Header{}
	headers.Add("User-Agent", fmt.Sprintf("%s/%s", Name, Version))
	conn, _, err := c.Dialer.Dial(c.Endpoint, headers)
	if err != nil {
		return err
	}

	fmt.Println("Connected to Binance Websocket API")
	conn.SetReadLimit(655350)
	c.Conn = conn

	c.startReader() // start reader again
	return nil
}

func (c *WebsocketAPIClient) startReader() {
	go func() {
		for {
			select {
			case <-c.stopChan:
				// 收到关闭信号，退出循环
				log.Println("Stopping reader...")
				return
			default:
				_, message, err := c.Conn.ReadMessage()
				if err != nil {
					log.Printf("Error reading: %v, attempting to reconnect...", err)
					// 发生错误时关闭当前连接
					if c.Conn != nil {
						_ = c.Conn.Close()
					}
					// 尝试重新连接
					c.reconnect()
					return
				}
				// 处理消息
				c.Handler(message)
			}
		}
	}()
}

// 重连逻辑，包含指数退避策略
func (c *WebsocketAPIClient) reconnect() {
	backoff := c.initialBackoff // 从初始间隔开始

	for {
		select {
		case <-c.stopChan:
			log.Println("Reconnect canceled")
			return
		default:
			log.Printf("Attempting to reconnect (next delay: %v)...", backoff)
			err := c.Connect()
			if err == nil {
				log.Println("Reconnected successfully")
				return // 重连成功，退出重连循环
			}

			log.Printf("Reconnection failed: %v, waiting %v before next attempt", err, backoff)
			time.Sleep(backoff)

			// 退避间隔递增，但不超过最大限制
			if backoff < c.maxBackoff {
				backoff *= 2
				if backoff > c.maxBackoff {
					backoff = c.maxBackoff
				}
			}
		}
	}
}

// Handler function to handle responses
// 支持并发执行, 但不支持用户数据流订阅, 用户数据流相应中没有 ID 字段
// 需要在 HandlerV2 中处理
func (c *WebsocketAPIClient) Handler(message []byte) {
	var response WsAPIErrorResponse
	err := Unmarshal(message, &response)
	if err != nil {
		log.Println("Error unmarshaling:", err)
		return
	}
	// Send the message to the corresponding request
	if val, exists := c.ReqResponseMap.Load(response.ID); exists {
		if channel, ok := val.(chan []byte); ok {
			channel <- message
		}
	}
}

// Handler function to handle responses
// 不再支持并发执行请求, 会导致数据错乱, 如 A 请求会收到 B 请求的响应
// 直接返回, 不做 JSON 解析, 速度更快
func (c *WebsocketAPIClient) HandlerV2(message []byte) {
	c.ReqResponseMap.Range(func(key, value interface{}) bool {
		if channel, ok := value.(chan []byte); ok {
			channel <- message
		}
		return true
	})
}

func (c *WebsocketAPIClient) WaitForCloseSignal() {
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)
	<-stopCh
}

func (c *WebsocketAPIClient) Close() error {
	if c.stopChan != nil {
		close(c.stopChan)
	}
	return c.Conn.Close()
}

func (c *WebsocketAPIClient) SendMessage(msg interface{}) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	return c.Conn.WriteJSON(msg)
}

func (c *WebsocketAPIClient) RequestHandler(req interface{}, handler WsHandler, errHandler ErrHandler) (stopCh chan struct{}, err error) {
	err = c.SendMessage(req)
	if err != nil {
		return nil, err
	}
	stopCh, err = wsApiServe(c.Conn, handler, errHandler)
	if err != nil {
		return nil, err
	}
	return stopCh, nil
}

func wsApiServe(c *websocket.Conn, handler WsHandler, errHandler ErrHandler) (stopCh chan struct{}, err error) {
	stopCh = make(chan struct{})
	go func() {
		if WebsocketAPIKeepalive {
			keepAlive(c, WebsocketAPITimeout)
		}

		for {
			select {
			case <-stopCh:
				return
			default:
				_, message, err := c.ReadMessage()
				if err != nil {
					fmt.Println(err)
					errHandler(err)
					continue
				}
				handler(message)
			}
		}
	}()
	return stopCh, nil
}

func (c *WebsocketAPIClient) Sign(parameters map[string]string) (map[string]string, error) {
	if c.UseEd25519 {
		return websocketAPIEdSignature(c.Ed25519APIKey, c.Ed25519PrivateKey, parameters)
	} else {
		return websocketAPISignature(c.APIKey, c.APISecret, parameters)
	}
}

func websocketAPISignature(apiKey string, apiSecret string, parameters map[string]string) (map[string]string, error) {
	if apiKey == "" || apiSecret == "" {
		return nil, &WebsocketClientError{
			Message: "api_key and api_secret are required for websocket API signature",
		}
	}

	parameters["timestamp"] = strconv.FormatInt(time.Now().Unix()*1000, 10)
	parameters["apiKey"] = apiKey

	// Sort parameters by key
	keys := make([]string, 0, len(parameters))
	for key := range parameters {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Build sorted query string
	var sortedParams []string
	for _, key := range keys {
		sortedParams = append(sortedParams, key+"="+parameters[key])
	}

	// Calculate signature
	queryString := strings.Join(sortedParams, "&")
	signature := hmacHashing(apiSecret, queryString)

	parameters["signature"] = signature

	return parameters, nil
}

func websocketAPIEdSignature(apiKey string, privateKey ed25519.PrivateKey, parameters map[string]string) (map[string]string, error) {
	if apiKey == "" || privateKey == nil {
		return nil, &WebsocketClientError{
			Message: "api_key and private_key are required for websocket API signature",
		}
	}

	parameters["timestamp"] = strconv.FormatInt(time.Now().Unix()*1000, 10)
	parameters["apiKey"] = apiKey

	// Sort parameters by key
	keys := make([]string, 0, len(parameters))
	for key := range parameters {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Build sorted query string
	var sortedParams []string
	for _, key := range keys {
		sortedParams = append(sortedParams, key+"="+parameters[key])
	}

	// Calculate signature
	queryString := strings.Join(sortedParams, "&")
	signature := ed25519Sign(privateKey, queryString)

	parameters["signature"] = signature

	return parameters, nil
}

func hmacHashing(apiSecret string, data string) string {
	mac := hmac.New(sha256.New, []byte(apiSecret))
	mac.Write([]byte(data))
	return hex.EncodeToString(mac.Sum(nil))
}

func ed25519Sign(privateKey ed25519.PrivateKey, data string) string {
	return base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(data)))
}

func getUUID() string {
	return fmt.Sprintf("%s-%s-%s-%s-%s", randomHex(8), randomHex(4), randomHex(4), randomHex(4), randomHex(12))
}

func randomHex(n int) string {
	bytes := make([]byte, n/2)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}

type WebsocketClientError struct {
	Message string
}

func (e *WebsocketClientError) Error() string {
	return e.Message
}

type TestConnectivityService struct {
	websocketAPI *WebsocketAPIClient
}

func (s *TestConnectivityService) Do(ctx context.Context) (*TestConnectivityResponse, error) {
	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "ping",
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
		var pingResponse TestConnectivityResponse
		err = Unmarshal(response, &pingResponse)
		if err != nil {
			return nil, err
		}
		return &pingResponse, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type TestConnectivityResponse struct {
	ID         string              `json:"id"`
	Status     int                 `json:"status"`
	Error      *WsAPIErrorResponse `json:"error,omitempty"`
	Result     struct{}            `json:"result,omitempty"`
	RateLimits []*WsAPIRateLimit   `json:"rateLimits,omitempty"`
}

type CheckServerTimeService struct {
	websocketAPI *WebsocketAPIClient
}

func (s *CheckServerTimeService) Do(ctx context.Context) (*CheckServerTimeResponse, error) {
	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "time",
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
		var timeResponse CheckServerTimeResponse
		err = Unmarshal(response, &timeResponse)
		if err != nil {
			return nil, err
		}
		return &timeResponse, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type CheckServerTimeResponse struct {
	ID     string              `json:"id"`
	Status int                 `json:"status"`
	Error  *WsAPIErrorResponse `json:"error,omitempty"`
	Result struct {
		ServerTime uint64 `json:"serverTime"`
	} `json:"result,omitempty"`
	RateLimits WsAPIRateLimit `json:"rateLimits,omitempty"`
}

type ExchangeInformationService struct {
	websocketAPI *WebsocketAPIClient
	symbol       *string
	symbols      *[]string
	permissions  *[]string
}

func (s *ExchangeInformationService) Symbol(symbol string) *ExchangeInformationService {
	s.symbol = &symbol
	return s
}

func (s *ExchangeInformationService) Symbols(symbols []string) *ExchangeInformationService {
	s.symbols = &symbols
	return s
}

func (s *ExchangeInformationService) Permissions(permissions []string) *ExchangeInformationService {
	s.permissions = &permissions
	return s
}

func (s *ExchangeInformationService) Do(ctx context.Context) (*ExchangeInformationResponse, error) {
	payload := map[string]interface{}{
		"id":     getUUID(),
		"method": "exchangeInfo",
	}

	// Only one of the following parameters can be set: symbol, symbols, permissions
	if s.symbol != nil {
		payload["params"] = map[string]interface{}{
			"symbol": *s.symbol,
		}
	} else if s.symbols != nil {
		payload["params"] = map[string]interface{}{
			"symbols": *s.symbols,
		}
	} else if s.permissions != nil {
		payload["params"] = map[string]interface{}{
			"permissions": *s.permissions,
		}
	}

	responseCh := make(chan *ExchangeInformationResponse)

	handler := func(message []byte) {
		var response ExchangeInformationResponse
		err := Unmarshal(message, &response)
		if err != nil {
			fmt.Println("Error unmarshaling:", err)
			return
		}
		responseCh <- &response
	}

	errHandler := func(err error) {
		fmt.Println("Error:", err)
	}

	doneCh, err := s.websocketAPI.RequestHandler(payload, handler, errHandler)
	if err != nil {
		return nil, err
	}

	select {
	case <-doneCh:
		return nil, ctx.Err()
	case response := <-responseCh:
		return response, nil
	}
}

type ExchangeInformationResponse struct {
	ID         string              `json:"id"`
	Status     int                 `json:"status"`
	Error      *WsAPIErrorResponse `json:"error,omitempty"`
	Result     json.RawMessage     `json:"result,omitempty"`
	RateLimits []*WsAPIRateLimit   `json:"rateLimits,omitempty"`
}

// General Websocket API Endpoints:
func (w *WebsocketAPIClient) NewTestConnectivityService() *TestConnectivityService {
	return &TestConnectivityService{websocketAPI: w}
}

func (w *WebsocketAPIClient) NewCheckServerTimeService() *CheckServerTimeService {
	return &CheckServerTimeService{websocketAPI: w}
}

func (w *WebsocketAPIClient) NewExchangeInformationService() *ExchangeInformationService {
	return &ExchangeInformationService{websocketAPI: w}
}

// Account Websocket API Endpoints:
// func (w *WebsocketAPIClient) NewAccountInformationService() *AccountInformationService {
// 	return &AccountInformationService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewAccountOrderRateLimitsService() *AccountOrderRateLimitsService {
// 	return &AccountOrderRateLimitsService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewAccountOrderHistoryService() *AccountOrderHistoryService {
// 	return &AccountOrderHistoryService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewAccountOCOHistoryService() *AccountOCOHistoryService {
// 	return &AccountOCOHistoryService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewAccountTradeHistoryService() *AccountTradeHistoryService {
// 	return &AccountTradeHistoryService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewAccountPreventedMatchesService() *AccountPreventedMatchesService {
// 	return &AccountPreventedMatchesService{websocketAPI: w}
// }

// // Market Websocket API Endpoints:
// func (w *WebsocketAPIClient) NewDepthService() *DepthService {
// 	return &DepthService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewRecentTradesService() *RecentTradesService {
// 	return &RecentTradesService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewHistoricalTradesService() *HistoricalTradesService {
// 	return &HistoricalTradesService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewAggTradesService() *AggregateTradesService {
// 	return &AggregateTradesService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewKlinesService() *KlinesService {
// 	return &KlinesService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewAvgPriceService() *AvgPriceService {
// 	return &AvgPriceService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewTicker24hrService() *Ticker24hrService {
// 	return &Ticker24hrService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewTickerService() *TickerService {
// 	return &TickerService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewTickerPriceService() *TickerPriceService {
// 	return &TickerPriceService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewTickerBookService() *TickerBookService {
// 	return &TickerBookService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewUiKlinesService() *UIKlinesService {
// 	return &UIKlinesService{websocketAPI: w}
// }

// Trading Websocket API Endpoints:
func (w *WebsocketAPIClient) NewPlaceNewOrderService() *OrderPlacementService {
	return &OrderPlacementService{websocketAPI: w}
}

// func (w *WebsocketAPIClient) NewTestPlaceOrderService() *TestOrderPlacementService {
// 	return &TestOrderPlacementService{websocketAPI: w}
// }

func (w *WebsocketAPIClient) NewModifyOrderService() *OrderModifyService {
	return &OrderModifyService{websocketAPI: w}
}

func (w *WebsocketAPIClient) NewCancelOrderService() *OrderCancelService {
	return &OrderCancelService{websocketAPI: w}
}

func (w *WebsocketAPIClient) NewQueryOrderService() *OrderStatusService {
	return &OrderStatusService{websocketAPI: w}
}

func (w *WebsocketAPIClient) NewAccountPositionService() *AccountPositionService {
	return &AccountPositionService{websocketAPI: w}
}

// func (w *WebsocketAPIClient) NewCancelReplaceOrderService() *OrderCancelReplaceService {
// 	return &OrderCancelReplaceService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewCurrentOpenOrdersService() *OpenOrdersStatusService {
// 	return &OpenOrdersStatusService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewCancelOpenOrdersService() *OpenOrdersCancelAllService {
// 	return &OpenOrdersCancelAllService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewPlaceOCOService() *OrderListPlaceService {
// 	return &OrderListPlaceService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewQueryOCOService() *OrderListStatusService {
// 	return &OrderListStatusService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewCurrentOpenOCOService() *OpenOrderListsStatusService {
// 	return &OpenOrderListsStatusService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewCancelOCOService() *OrderListCancelService {
// 	return &OrderListCancelService{websocketAPI: w}
// }

// User Data Websocket API Endpoints:
func (w *WebsocketAPIClient) NewStartUserDataStreamService() *StartUserDataStreamService {
	return &StartUserDataStreamService{websocketAPI: w}
}

func (w *WebsocketAPIClient) NewPingUserDataStreamService() *PingUserDataStreamService {
	return &PingUserDataStreamService{websocketAPI: w}
}

func (w *WebsocketAPIClient) NewStopUserDataStreamService() *StopUserDataStreamService {
	return &StopUserDataStreamService{websocketAPI: w}
}

func (w *WebsocketAPIClient) NewSubscribeUserDataStreamService() *SubscribeUserDataStreamService {
	return &SubscribeUserDataStreamService{websocketAPI: w}
}

// Session Websocket API Endpoints:
// func (w *WebsocketAPIClient) NewLogonSessionService() *LogonSessionService {
// 	return &LogonSessionService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewStatusSessionService() *StatusSessionService {
// 	return &StatusSessionService{websocketAPI: w}
// }

// func (w *WebsocketAPIClient) NewLogoutSessionService() *LogoutSessionService {
// 	return &LogoutSessionService{websocketAPI: w}
// }
