package binance_futures_connector

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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
	sync.Mutex

	APIKey            string
	APISecret         string
	Ed25519APIKey     string
	Ed25519PrivateKey ed25519.PrivateKey
	UseEd25519        bool
	UseSBE            bool
	Endpoint          string
	BindIP            string
	Conn              *websocket.Conn
	Dialer            *websocket.Dialer
	MessageCh         chan []byte
	ReqResponseMap    sync.Map
	Debug             bool
	Reconnect         bool
	Logger            *log.Logger
}

type WsAPIRateLimit struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   int    `json:"intervalNum"`
	Limit         int    `json:"limit"`
	Count         int    `json:"count"`
}

type WsAPIErrorResponse struct {
	Code    int64  `json:"code"`
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
		Logger: log.New(os.Stderr, Name, log.LstdFlags),
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

func (c *WebsocketAPIClient) debug(format string, v ...interface{}) {
	if c.Debug {
		c.Logger.Printf(format, v...)
	}
}

func (c *WebsocketAPIClient) error(format string, v ...interface{}) {
	c.Logger.Printf(format, v...)
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
	conn, err := c.dial()
	if err != nil {
		return err
	}

	c.Lock()
	c.Conn = conn
	c.Unlock()
	fmt.Println("Connected to Binance Websocket API")
	if c.MessageCh == nil {
		c.MessageCh = make(chan []byte, 1000)
	}

	c.startReader()
	return nil
}

func (c *WebsocketAPIClient) dial() (*websocket.Conn, error) {
	if c.Dialer == nil {
		return nil, fmt.Errorf("dialer not initialized")
	}

	headers := http.Header{}
	headers.Add("User-Agent", fmt.Sprintf("%s/%s", Name, Version))
	conn, _, err := c.Dialer.Dial(c.Endpoint, headers)
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(655350)
	return conn, nil
}

func (c *WebsocketAPIClient) startReader() {
	go func() {
		defer func() {
			if c.Reconnect {
				c.ReLogin()
			}
		}()

		for {
			_, message, err := c.Conn.ReadMessage()
			if err != nil {
				log.Println("Ws API error reading:", err)
				return
			}
			c.Handler(message)
		}
	}()
}

func (c *WebsocketAPIClient) ReLogin() {
	err := c.Connect()
	if err != nil {
		log.Println("Ws API error reconnecting:", err)
		return
	}

	log.Printf("Ws API re-login successful")
}

// Handler function to handle responses
// 支持并发执行, 但不支持用户数据流订阅, 用户数据流相应中没有 ID 字段
func (c *WebsocketAPIClient) Handler(message []byte) {
	c.debug("Receive message: %s", string(message))
	var response WsAPIErrorResponse
	err := Unmarshal(message, &response)
	if err != nil {
		log.Println("Error unmarshaling:", err)
		return
	}
	// Send the message to the corresponding request
	if val, exists := c.ReqResponseMap.Load(response.ID); exists {
		if channel, ok := val.(chan []byte); ok {
			select {
			case channel <- message:
			default:
				c.error("Ws API response channel full, drop response id: %s, message: %s", response.ID, string(message))
			}
		}
		return
	}

	if c.MessageCh == nil {
		return
	}

	select {
	case c.MessageCh <- message:
	default:
		c.error("Ws API message channel full, drop unsolicited message: %s", string(message))
	}
}

func (c *WebsocketAPIClient) WaitForCloseSignal() {
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)
	<-stopCh
}

func (c *WebsocketAPIClient) Close() error {
	c.Reconnect = false
	if c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

func (c *WebsocketAPIClient) SendMessage(msg interface{}) error {
	if c.Conn == nil {
		return fmt.Errorf("websocket connection not available")
	}
	c.debug("Send message: %s", JsonFormat(msg))

	c.Lock()
	err := c.Conn.WriteJSON(msg)
	c.Unlock()
	if err == nil {
		return nil
	}

	c.error("Send message failed: %v, %s", err, "reconnect...")
	if c.Reconnect {
		c.ReLogin()
	}
	return err

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
		return ""
	}
	return hex.EncodeToString(bytes)
}

type WebsocketClientError struct {
	Message string
}

func (e *WebsocketClientError) Error() string {
	return e.Message
}

// Account Websocket API Endpoints:
func (w *WebsocketAPIClient) NewAccountInformationService() *AccountInformationService {
	return &AccountInformationService{websocketAPI: w}
}

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

func (w *WebsocketAPIClient) NewPlaceAlgoOrderService() *AlgoOrderPlacementService {
	return &AlgoOrderPlacementService{websocketAPI: w}
}

func (w *WebsocketAPIClient) NewCancelAlgoOrderService() *AlgoOrderCancelService {
	return &AlgoOrderCancelService{websocketAPI: w}
}

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
