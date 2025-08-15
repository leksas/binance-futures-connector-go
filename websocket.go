package binance_futures_connector

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// WsHandler handle raw websocket message
type WsHandler func(message []byte)

// ErrHandler handles errors
type ErrHandler func(err error)

// WsConfig webservice configuration
type WsConfig struct {
	Endpoint string
	BindIP   string
}

type WebsocketStreamClient struct {
	Endpoint   string
	IsCombined bool
	BindIP     string
}

func NewWebsocketStreamClient(isCombined bool, baseURL ...string) *WebsocketStreamClient {
	// Set default base URL to production WS URL
	url := "wss://fstream.binance.com"

	if len(baseURL) > 0 {
		url = baseURL[0]
	}

	// Append to baseURL based on whether the client is for combined streams or not
	if isCombined {
		url += "/stream?streams="
	} else {
		url += "/ws"
	}

	return &WebsocketStreamClient{
		Endpoint:   url,
		IsCombined: isCombined,
	}
}

func (c *WebsocketStreamClient) SetBindIP(ip string) {
	c.BindIP = ip
}

func newWsConfig(endpoint string, bindIP ...string) *WsConfig {
	var ip string
	if len(bindIP) > 0 {
		ip = bindIP[0]
	}
	return &WsConfig{
		Endpoint: endpoint,
		BindIP:   ip,
	}
}

var wsServe = func(cfg *WsConfig, handler WsHandler, errHandler ErrHandler) (doneCh, stopCh chan struct{}, err error) {
	Dialer := websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  45 * time.Second,
		EnableCompression: false,
	}

	if cfg.BindIP != "" {
		Dialer.NetDial = func(network, addr string) (net.Conn, error) {
			lAddr, err := net.ResolveTCPAddr(network, cfg.BindIP+":0")
			if err != nil {
				return nil, err
			}
			dialer := net.Dialer{
				LocalAddr: lAddr,
			}
			return dialer.Dial(network, addr)
		}
	}

	headers := http.Header{}
	headers.Add("User-Agent", fmt.Sprintf("%s/%s", Name, Version))
	c, _, err := Dialer.Dial(cfg.Endpoint, headers)
	if err != nil {
		return nil, nil, err
	}
	c.SetReadLimit(655350)
	doneCh = make(chan struct{})
	stopCh = make(chan struct{})
	go func() {
		// This function will exit either on error from
		// websocket.Conn.ReadMessage or when the stopC channel is
		// closed by the client.
		defer func() {
			close(doneCh)
			c.Close()
		}()

		if WebsocketKeepalive {
			keepAlive(c, WebsocketTimeout)
		}
		// Wait for the stopC channel to be closed.  We do that in a
		// separate goroutine because ReadMessage is a blocking
		// operation.
		silent := false
		go func() {
			for {
				select {
				case <-stopCh:
					silent = true
					return
				default:
					_, message, err := c.ReadMessage()
					if err != nil {
						if !silent {
							errHandler(err)
						}
						stopCh <- struct{}{}
						return
					}
					go handler(message)
				}
			}
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

func keepAlive(c *websocket.Conn, timeout time.Duration) {
	ticker := time.NewTicker(timeout)

	lastResponse := time.Now()
	c.SetPongHandler(func(msg string) error {
		lastResponse = time.Now()
		return nil
	})

	go func() {
		defer ticker.Stop()
		for {
			deadline := time.Now().Add(10 * time.Second)
			err := c.WriteControl(websocket.PingMessage, []byte{}, deadline)
			if err != nil {
				return
			}
			<-ticker.C
			if time.Since(lastResponse) > timeout {
				return
			}
		}
	}()
}
