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
	Result     *AccountInformation `json:"result,omitempty"`
	RateLimits []*WsAPIRateLimit   `json:"rateLimits"`
}

type AccountInformation struct {
	TotalInitialMargin          string     `json:"totalInitialMargin"`          // 当前所需起始保证金总额(存在逐仓请忽略), 仅计算usdt资产positions), only for USDT asset
	TotalMaintMargin            string     `json:"totalMaintMargin"`            // 维持保证金总额, 仅计算usdt资产
	TotalWalletBalance          string     `json:"totalWalletBalance"`          // 账户总余额, 仅计算usdt资产
	TotalUnrealizedProfit       string     `json:"totalUnrealizedProfit"`       // 持仓未实现盈亏总额, 仅计算usdt资产
	TotalMarginBalance          string     `json:"totalMarginBalance"`          // 保证金总余额, 仅计算usdt资产
	TotalPositionInitialMargin  string     `json:"totalPositionInitialMargin"`  // 持仓所需起始保证金(基于最新标记价格), 仅计算usdt资产
	TotalOpenOrderInitialMargin string     `json:"totalOpenOrderInitialMargin"` // 当前挂单所需起始保证金(基于最新标记价格), 仅计算usdt资产
	TotalCrossWalletBalance     string     `json:"totalCrossWalletBalance"`     // 全仓账户余额, 仅计算usdt资产
	TotalCrossUnPnl             string     `json:"totalCrossUnPnl"`             // 全仓持仓未实现盈亏总额, 仅计算usdt资产
	AvailableBalance            string     `json:"availableBalance"`            // 可用余额, 仅计算usdt资产
	MaxWithdrawAmount           string     `json:"maxWithdrawAmount"`           // 最大可转出余额, 仅计算usdt资产
	Assets                      []Asset    `json:"assets"`
	Positions                   []Position `json:"positions"`
}
