package binance_futures_connector

import (
	"context"
	"strconv"
)

type AccountInformationService struct {
	websocketAPI *WebsocketAPIClient
	recvWindow   *int64
}

func (s *AccountInformationService) RecvWindow(recvWindow int64) *AccountInformationService {
	s.recvWindow = &recvWindow
	return s
}

func (s *AccountInformationService) Do(ctx context.Context) (*AccountInformationResponse, error) {
	parameters := map[string]string{}

	if s.recvWindow != nil {
		parameters["recvWindow"] = strconv.FormatInt(*s.recvWindow, 10)
	}

	signedParams, err := s.websocketAPI.Sign(parameters)
	if err != nil {
		panic(err)
	}

	id := getUUID()

	payload := map[string]interface{}{
		"id":     id,
		"method": "v2/account.status",
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
		var accInfoResponse AccountInformationResponse
		err = Unmarshal(response, &accInfoResponse)
		if err != nil {
			return nil, err
		}
		return &accInfoResponse, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type AccountInformationResponse struct {
	ID         string              `json:"id"`
	Status     int                 `json:"status"`
	Error      *WsAPIErrorResponse `json:"error,omitempty"`
	Result     *Account            `json:"result,omitempty"`
	RateLimits []*WsAPIRateLimit   `json:"rateLimits"`
}
