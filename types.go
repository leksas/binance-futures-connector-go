package binance_futures_connector

type (
	Side           = string
	PositionSide   = string
	OrderType      = string
	TimeInForce    = string
	WorkingType    = string
	OrderRespType  = string
	PriceMatch     = string
	STPMode        = string
	OrderStatus    = string
	Interval       = string
	ContractType   = string
	ContractStatus = string
	StrategyStatus = string
	OpCode         = int
	UserDataType   = string
)

var (
	Buy  Side = "BUY"
	Sell Side = "SELL"

	// PositionSide 持仓方向
	Both  PositionSide = "BOTH"  // 单一持仓方向
	Long  PositionSide = "LONG"  // 多头(双向持仓下)
	Short PositionSide = "SHORT" // 空头(双向持仓下)

	// OrderType 订单种类
	Limit              OrderType = "LIMIT"                // 限价单
	Market             OrderType = "MARKET"               // 市价单
	Stop               OrderType = "STOP"                 // 止损限价单
	TakeProfit         OrderType = "TAKE_PROFIT"          // 止损市价单
	StopMarket         OrderType = "STOP_MARKET"          // 止盈限价单
	TakeProfitMarket   OrderType = "TAKE_PROFIT_MARKET"   // 止盈市价单
	TrailingStopMarket OrderType = "TRAILING_STOP_MARKET" // 跟踪止损单

	// TimeInForce 有效方式
	GTC TimeInForce = "GTC" // Good Till Cancel 成交为止（下单后仅有1年有效期，1年后自动取消）
	IOC TimeInForce = "IOC" // Immediate or Cancel 无法立即成交(吃单)的部分就撤销
	FOK TimeInForce = "FOK" // Fill or Kill 无法全部立即成交就撤销
	GTX TimeInForce = "GTX" // Good Till Crossing 无法成为挂单方就撤销
	GTD TimeInForce = "GTD" // Good Till Date 在特定时间之前有效，到期自动撤销

	// WorkingType 条件价格触发类型
	MarkPrice     WorkingType = "MARK_PRICE"     // 标记价格
	ContractPrice WorkingType = "CONTRACT_PRICE" // 合约最新价 (默认)

	ACK    OrderRespType = "ACK"
	RESULT OrderRespType = "RESULT"
	FULL   OrderRespType = "FULL" // 已取消

	// PriceMatch 盘口价下单模式
	Opponent   PriceMatch = "OPPONENT"    // (盘口对手价)
	Opponent5  PriceMatch = "OPPONENT_5"  // (盘口对手5档价)
	Opponent10 PriceMatch = "OPPONENT_10" // (盘口对手10档价)
	Opponent20 PriceMatch = "OPPONENT_20"
	Queue      PriceMatch = "QUEUE"    // (盘口同向价)
	Queue5     PriceMatch = "QUEUE_5"  // (盘口同向排队5档价)
	Queue10    PriceMatch = "QUEUE_10" // (盘口同向排队10档价)
	Queue20    PriceMatch = "QUEUE_20" // (盘口同向排队20档价)

	// STPMode 防止自成交模式
	None        STPMode = "NONE" // 默认
	ExpireTaker STPMode = "EXPIRE_TAKER"
	ExpireMaker STPMode = "EXPIRE_MAKER"
	ExpireBoth  STPMode = "EXPIRE_BOTH"

	// OrderStatus 订单状态
	NEW              OrderStatus = "NEW"              // 新建订单
	PARTIALLY_FILLED OrderStatus = "PARTIALLY_FILLED" // 部分成交
	FILLED           OrderStatus = "FILLED"           // 全部成交
	CANCELED         OrderStatus = "CANCELED"         // 已撤销
	REJECTED         OrderStatus = "REJECTED"         // 订单被拒绝
	EXPIRED          OrderStatus = "EXPIRED"          // 订单过期(根据timeInForce参数规则)
	EXPIRED_IN_MATCH OrderStatus = "EXPIRED_IN_MATCH" // 订单被STP过期

	// Interval1m K线间隔
	Interval1m  Interval = "1m"
	Interval3m  Interval = "3m"
	Interval5m  Interval = "5m"
	Interval15m Interval = "15m"
	Interval30m Interval = "30m"
	Interval1h  Interval = "1h"
	Interval2h  Interval = "2h"
	Interval4h  Interval = "4h"
	Interval6h  Interval = "6h"
	Interval8h  Interval = "8h"
	Interval12h Interval = "12h"
	Interval1d  Interval = "1d"
	Interval3d  Interval = "3d"
	Interval1w  Interval = "1w"
	Interval1M  Interval = "1M"

	// ContractType 合约类型
	PERPETUAL            ContractType = "PERPETUAL"            // 永续合约
	CURRENT_MONTH        ContractType = "CURRENT_MONTH"        // 当月交割合约
	NEXT_MONTH           ContractType = "NEXT_MONTH"           // 次月交割合约
	CURRENT_QUARTER      ContractType = "CURRENT_QUARTER"      // 当季交割合约
	NEXT_QUARTER         ContractType = "NEXT_QUARTER"         // 次季交割合约
	PERPETUAL_DELIVERING ContractType = "PERPETUAL_DELIVERING" // 交割结算中合约

	// ContractStatus 合约状态
	PendingTrading ContractStatus = "PENDING_TRADING" // 待上市
	Trading        ContractStatus = "TRADING"         // 交易中
	PreDelivering  ContractStatus = "PRE_DELIVERING"  // 预交割
	Delivering     ContractStatus = "DELIVERING"      // 交割中
	Delivered      ContractStatus = "DELIVERED"       // 已交割
	PreSettle      ContractStatus = "PRE_SETTLE"      // 预结算
	Settling       ContractStatus = "SETTLING"        // 结算中
	Close          ContractStatus = "CLOSE"           // 已下架

	// StrategyStatus 策略状态
	New      StrategyStatus = "NEW"
	Working  StrategyStatus = "WORKING"
	Canceled StrategyStatus = "CANCELED"
	Expired  StrategyStatus = "EXPIRED"

	OpCode8001 OpCode = 8001 // 策略参数修改
	OpCode8002 OpCode = 8002 // 用户取消策略
	OpCode8003 OpCode = 8003 // 用户手动新增或取消订单
	OpCode8004 OpCode = 8004 // 达到 stop limit
	OpCode8005 OpCode = 8005 // 用户仓位爆仓
	OpCode8006 OpCode = 8006 // 已达最大可挂单数量
	OpCode8007 OpCode = 8007 // 新增网格策略
	OpCode8008 OpCode = 8008 // 保证金不足
	OpCode8009 OpCode = 8009 // 价格超出范围
	OpCode8010 OpCode = 8010 // 市场非交易状态
	OpCode8011 OpCode = 8011 // 关仓失败，平仓单无法成交
	OpCode8012 OpCode = 8012 // 超过最大可交易名目金额
	OpCode8013 OpCode = 8013 // 不符合网格交易身份
	OpCode8014 OpCode = 8014 // 不符合 Futures Trading Quantitative Rules，策略终止
	OpCode8015 OpCode = 8015 // 无仓位或是仓位已经爆仓

	ListenKeyExpired              UserDataType = "listenKeyExpired"
	AccountDataUpdate             UserDataType = "ACCOUNT_UPDATE"
	MarginCall                    UserDataType = "MARGIN_CALL"
	OrderTradeUpdate              UserDataType = "ORDER_TRADE_UPDATE"
	TradeLite                     UserDataType = "TRADE_LITE"
	AccountConfigUpdate           UserDataType = "ACCOUNT_CONFIG_UPDATE"
	StrategyUpdate                UserDataType = "STRATEGY_UPDATE"
	GridUpdate                    UserDataType = "GRID_UPDATE"
	ConditionalOrderTriggerReject UserDataType = "CONDITIONAL_ORDER_TRIGGER_REJECT"
)
