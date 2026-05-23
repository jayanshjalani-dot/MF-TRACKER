import axios from "axios";
import type {
  OptionChainResponse,
  OHLCVBar,
  SentimentResult,
  PlaceOrderPayload,
  Order,
  Position,
  BacktestRequest,
  BacktestResult,
} from "./types";

const BASE = "/api/v1";

const client = axios.create({ baseURL: BASE, timeout: 30_000 });

// ---------- Auth ----------
export const getProfile = () => client.get("/auth/profile").then((r) => r.data);

// ---------- Instruments ----------
export const searchInstruments = (q: string, exchange?: string) =>
  client.get("/instruments/search", { params: { q, exchange } }).then((r) => r.data);

export const getNiftySpot = () =>
  client.get<{ underlying: string; spot: number }>("/instruments/nifty-spot").then((r) => r.data);

export const getBankNiftySpot = () =>
  client.get<{ underlying: string; spot: number }>("/instruments/banknifty-spot").then((r) => r.data);

// ---------- Historical ----------
export const getHistorical = (
  token: string,
  exchange: string,
  interval: string,
  from_date: string,
  to_date: string
): Promise<OHLCVBar[]> =>
  client
    .get("/historical", { params: { token, exchange, interval, from_date, to_date } })
    .then((r) => r.data);

// ---------- Option Chain ----------
export const getOptionChain = (
  underlying: string,
  expiry?: string,
  strikesAroundAtm = 10
): Promise<OptionChainResponse> =>
  client
    .get(`/option-chain/${underlying}`, {
      params: { expiry, strikes_around_atm: strikesAroundAtm },
    })
    .then((r) => r.data);

export const getPayoff = (params: {
  strategy: string;
  strike: number;
  spot: number;
  ce_premium?: number;
  pe_premium?: number;
  lots?: number;
  lot_size?: number;
}) => client.get("/strategy/payoff", { params }).then((r) => r.data);

// ---------- Orders ----------
export const placeOrder = (payload: PlaceOrderPayload) =>
  client.post("/orders/place", payload).then((r) => r.data);

export const cancelOrder = (orderid: string) =>
  client.delete("/orders/cancel", { data: { orderid } }).then((r) => r.data);

export const getOrderBook = (): Promise<Order[]> =>
  client.get("/orders/book").then((r) => r.data);

export const getTradeBook = () => client.get("/orders/trades").then((r) => r.data);

export const getPositions = (): Promise<Position[]> =>
  client.get("/portfolio/positions").then((r) => r.data);

export const getHoldings = () => client.get("/portfolio/holdings").then((r) => r.data);

// ---------- Sentiment ----------
export const getSentiment = (refresh = false): Promise<SentimentResult> =>
  client.get("/sentiment", { params: { refresh } }).then((r) => r.data);

// ---------- Backtesting ----------
export const runBacktest = (req: BacktestRequest): Promise<BacktestResult> =>
  client.post("/backtest", req).then((r) => r.data);

// ---------- Streaming ----------
export const createTickStream = (onTick: (data: object) => void): EventSource => {
  const es = new EventSource(`${BASE}/stream/ticks`);
  es.onmessage = (e) => {
    try {
      onTick(JSON.parse(e.data));
    } catch {}
  };
  return es;
};
