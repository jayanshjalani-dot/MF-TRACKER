// ---------- Core Domain Types ----------

export interface OptionStrike {
  strike: number;
  expiry: string;
  ce_ltp: number | null;
  ce_oi: number | null;
  ce_volume: number | null;
  ce_iv: number | null;
  ce_delta: number | null;
  ce_gamma: number | null;
  ce_theta: number | null;
  ce_vega: number | null;
  pe_ltp: number | null;
  pe_oi: number | null;
  pe_volume: number | null;
  pe_iv: number | null;
  pe_delta: number | null;
  pe_gamma: number | null;
  pe_theta: number | null;
  pe_vega: number | null;
  pcr: number | null;
}

export interface OptionChainResponse {
  underlying: string;
  max_pain: number;
  pcr: number;
  chain: OptionStrike[];
}

export interface OHLCVBar {
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface Tick {
  token: string;
  ltp: number;
  volume: number;
  oi: number;
  bid: number;
  ask: number;
  ts: string;
}

export interface SentimentResult {
  score: number;
  label: string;
  trade_blocked: boolean;
  headlines: Headline[];
}

export interface Headline {
  source: string;
  title: string;
  url: string;
  published: string;
  polarity: number;
  weight: number;
}

export type OrderSide = "BUY" | "SELL";
export type OrderType = "MARKET" | "LIMIT" | "STOPLOSS_LIMIT" | "STOPLOSS_MARKET";
export type ProductType = "INTRADAY" | "DELIVERY" | "CARRYFORWARD" | "MARGIN";

export interface PlaceOrderPayload {
  variety: string;
  tradingsymbol: string;
  symboltoken: string;
  transactiontype: OrderSide;
  exchange: string;
  ordertype: OrderType;
  producttype: ProductType;
  duration: string;
  price: number;
  quantity: number;
  stoploss?: number;
  squareoff?: number;
  triggerprice?: number;
}

export interface Order {
  orderid: string;
  tradingsymbol: string;
  transactiontype: string;
  ordertype: string;
  quantity: number;
  price: number;
  status: string;
  filledshares: number;
  averageprice: number;
  updatetime: string;
}

export interface Position {
  tradingsymbol: string;
  exchange: string;
  producttype: string;
  netqty: number;
  buyavgprice: number;
  sellavgprice: number;
  ltp: number;
  pnl: number;
  symboltoken: string;
}

export interface BacktestRequest {
  symbol: string;
  exchange: string;
  symbol_token: string;
  interval: string;
  from_date: string;
  to_date: string;
  strategy: string;
  fast_period: number;
  slow_period: number;
}

export interface BacktestTrade {
  entry_date: string;
  exit_date: string;
  entry_price: number;
  exit_price: number;
  direction: string;
  pnl: number;
  pnl_pct: number;
  bars_held: number;
}

export interface BacktestResult {
  total_trades: number;
  win_rate: number;
  total_pnl: number;
  total_pnl_pct: number;
  max_drawdown: number;
  max_drawdown_pct: number;
  sharpe_ratio: number;
  profit_factor: number;
  avg_win: number;
  avg_loss: number;
  best_trade: number;
  worst_trade: number;
  trades: BacktestTrade[];
}

export interface PayoffPoint {
  spot: number;
  pnl: number;
}
