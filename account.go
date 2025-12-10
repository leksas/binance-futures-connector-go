package binance_futures_connector

import (
	"context"
	"net/http"
)

// 账户余额V3 （GET /fapi/v3/balance）
type GetBalanceService struct {
	c *Client
}

func (s *GetBalanceService) Do(ctx context.Context, opts ...RequestOption) (res []Balance, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/balance",
		secType:  secTypeSigned,
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return []Balance{}, err
	}
	err = Unmarshal(data, &res)
	return res, err
}

type Balance struct {
	AccountAlias       string `json:"accountAlias"`       // 账户唯一识别码
	Asset              string `json:"asset"`              // 资产
	Balance            string `json:"balance"`            // 总余额
	CrossWalletBalance string `json:"crossWalletBalance"` // 全仓余额
	CrossUnPnl         string `json:"crossUnPnl"`         // 全仓持仓未实现盈亏
	AvailableBalance   string `json:"availableBalance"`   // 下单可用余额
	MaxWithdrawAmount  string `json:"maxWithdrawAmount"`  // 最大可转出余额
	MarginAvailable    bool   `json:"marginAvailable"`    // 是否可用作联合保证金
	UpdateTime         int64  `json:"updateTime"`
}

// 账户信息V3 (GET /fapi/v3/account)
type GetAccountService struct {
	c *Client
}

func (s *GetAccountService) Do(ctx context.Context, opts ...RequestOption) (res *Account, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v3/account",
		secType:  secTypeSigned,
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	err = Unmarshal(data, &res)
	return res, err
}

type Account struct {
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

type Asset struct {
	Asset                  string `json:"asset"`                  // 资产
	WalletBalance          string `json:"walletBalance"`          // 余额
	UnrealizedProfit       string `json:"unrealizedProfit"`       // 未实现盈亏
	MarginBalance          string `json:"marginBalance"`          // 保证金余额
	MaintMargin            string `json:"maintMargin"`            // 维持保证金
	InitialMargin          string `json:"initialMargin"`          // 当前所需起始保证金
	PositionInitialMargin  string `json:"positionInitialMargin"`  // 持仓所需起始保证金(基于最新标记价格)
	OpenOrderInitialMargin string `json:"openOrderInitialMargin"` // 当前挂单所需起始保证金(基于最新标记价格)
	CrossWalletBalance     string `json:"crossWalletBalance"`     // 全仓账户余额
	CrossUnPnl             string `json:"crossUnPnl"`             // 全仓持仓未实现盈亏
	AvailableBalance       string `json:"availableBalance"`       // 可用余额
	MaxWithdrawAmount      string `json:"maxWithdrawAmount"`      // 最大可转出余额
	UpdateTime             int64  `json:"updateTime"`             // 更新时间
}

// 仅有仓位或挂单的交易对会被返回
// 根据用户持仓模式展示持仓方向，即单向模式下只返回BOTH持仓情况，双向模式下只返回 LONG 和 SHORT 持仓情况
type Position struct {
	Symbol           string       `json:"symbol"`           // 交易对
	PositionSide     PositionSide `json:"positionSide"`     // 持仓方向
	PositionAmt      string       `json:"positionAmt"`      // 持仓数量
	UnrealizedProfit string       `json:"unrealizedProfit"` // 持仓未实现盈亏
	IsolatedMargin   string       `json:"isolatedMargin"`
	Notional         string       `json:"notional"`
	IsolatedWallet   string       `json:"isolatedWallet"`
	InitialMargin    string       `json:"initialMargin"` // 持仓所需起始保证金(基于最新标记价格)
	MaintMargin      string       `json:"maintMargin"`   // 当前杠杆下用户可用的最大名义价值
	UpdateTime       int64        `json:"updateTime"`    // 更新时间
}

// 用户手续费率 (GET /fapi/v1/commissionRate)
type CommissionRateService struct {
	c      *Client
	symbol string
}

func (s *CommissionRateService) Symbol(symbol string) *CommissionRateService {
	s.symbol = symbol
	return s
}

func (s *CommissionRateService) Do(ctx context.Context, opts ...RequestOption) (res *CommissionRate, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/commissionRate",
		secType:  secTypeSigned,
	}
	r.setParam("symbol", s.symbol)
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(CommissionRate)
	err = Unmarshal(data, res)
	return res, err
}

type CommissionRate struct {
	Symbol    string `json:"symbol"`
	MakerRate string `json:"makerCommissionRate"`
	TakerRate string `json:"takerCommissionRate"`
}

// 账户配置 (GET /fapi/v1/accountConfig)
type AccountConfigService struct {
	c *Client
}

func (s *AccountConfigService) Do(ctx context.Context, opts ...RequestOption) (res *AccountConfig, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/accountConfig",
		secType:  secTypeSigned,
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return
	}
	res = new(AccountConfig)
	err = Unmarshal(data, res)
	return
}

type AccountConfig struct {
	FeeTier           int   `json:"feeTier"`     // 费率等级
	CanTrade          bool  `json:"canTrade"`    // 是否可以交易
	CanDeposit        bool  `json:"canDeposit"`  // 是否可以存款
	CanWithdraw       bool  `json:"canWithdraw"` // 是否可以提现
	DualSidePosition  bool  `json:"dualSidePosition"`
	UpdateTime        int64 `json:"updateTime"` // 忽略
	MultiAssetsMargin bool  `json:"multiAssetsMargin"`
	TradeGroupId      int   `json:"tradeGroupId"`
}

// 交易对配置 (GET /fapi/v1/symbolConfig)
type SymbolConfigService struct {
	c      *Client
	symbol *string
}

func (s *SymbolConfigService) Symbol(symbol string) *SymbolConfigService {
	s.symbol = &symbol
	return s
}

func (s *SymbolConfigService) Do(ctx context.Context, opts ...RequestOption) (res []SymbolConfig, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/symbolConfig",
		secType:  secTypeSigned,
	}
	if s.symbol != nil {
		r.setParam("symbol", s.symbol)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return []SymbolConfig{}, err
	}
	err = Unmarshal(data, &res)
	return res, err
}

type SymbolConfig struct {
	Symbol           string `json:"symbol"`
	MarginType       string `json:"marginType"`
	IsAutoAddMargin  bool   `json:"isAutoAddMargin"`
	Leverage         int    `json:"leverage"`
	MaxNotionalValue string `json:"maxNotionalValue"`
}

type GetIncomeService struct {
	c          *Client
	symbol     *string
	incomeType *string
	startTime  *int64
	endTime    *int64
	page       *int
	limit      *int
}

func (s *GetIncomeService) Symbol(symbol string) *GetIncomeService {
	s.symbol = &symbol
	return s
}

func (s *GetIncomeService) IncomeType(incomeType string) *GetIncomeService {
	s.incomeType = &incomeType
	return s
}

func (s *GetIncomeService) StartTime(startTime int64) *GetIncomeService {
	s.startTime = &startTime
	return s
}

func (s *GetIncomeService) EndTime(endTime int64) *GetIncomeService {
	s.endTime = &endTime
	return s
}

func (s *GetIncomeService) Page(page int) *GetIncomeService {
	s.page = &page
	return s
}

func (s *GetIncomeService) Limit(limit int) *GetIncomeService {
	s.limit = &limit
	return s
}

func (s *GetIncomeService) Do(ctx context.Context, opts ...RequestOption) (res []Income, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/income",
		secType:  secTypeSigned,
	}
	if s.symbol != nil {
		r.setParam("symbol", *s.symbol)
	}
	if s.incomeType != nil {
		r.setParam("incomeType", *s.incomeType)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	if s.page != nil {
		r.setParam("page", *s.page)
	}
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return []Income{}, err
	}
	err = Unmarshal(data, &res)
	return res, err
}

type Income struct {
	Symbol     string     `json:"symbol"`
	IncomeType IncomeType `json:"incomeType"`
	Income     string     `json:"income"`
	Asset      string     `json:"asset"`
	Info       string     `json:"info"`
	Time       int64      `json:"time"`
	TranID     int64      `json:"tranId"`
	TradeID    string     `json:"tradeId"`
}
