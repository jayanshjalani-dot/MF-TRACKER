"use client";

import { useState } from "react";
import { runBacktest, searchInstruments } from "@/lib/api";
import type { BacktestResult, BacktestRequest } from "@/lib/types";
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from "recharts";
import clsx from "clsx";

const DEFAULT_FORM: BacktestRequest = {
  symbol: "NIFTY",
  exchange: "NSE",
  symbol_token: "99926000",
  interval: "ONE_DAY",
  from_date: "2023-01-01 09:15",
  to_date: "2024-12-31 15:30",
  strategy: "MA_CROSSOVER",
  fast_period: 10,
  slow_period: 20,
};

function MetricCard({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div className="panel p-3 flex flex-col gap-1">
      <div className="text-xs text-terminal-muted">{label}</div>
      <div className={clsx("text-base font-bold", color || "text-terminal-text")}>{value}</div>
    </div>
  );
}

export default function BacktestPanel() {
  const [form, setForm] = useState<BacktestRequest>(DEFAULT_FORM);
  const [result, setResult] = useState<BacktestResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await runBacktest(form);
      setResult(res);
    } catch (e: any) {
      setError(e.response?.data?.detail || e.message);
    } finally {
      setLoading(false);
    }
  };

  // Build cumulative PnL curve for chart
  const equityCurve = result?.trades.reduce(
    (acc, t) => {
      acc.push({ date: t.exit_date.slice(0, 10), cumPnl: (acc[acc.length - 1]?.cumPnl || 0) + t.pnl });
      return acc;
    },
    [] as { date: string; cumPnl: number }[]
  ) || [];

  const update = (key: keyof BacktestRequest, val: unknown) =>
    setForm((f) => ({ ...f, [key]: val }));

  return (
    <div className="flex flex-col gap-3 p-3 overflow-auto h-full">
      {/* Config Form */}
      <div className="panel p-3 grid grid-cols-4 gap-3 text-xs">
        <div className="flex flex-col gap-1">
          <label className="text-terminal-muted">Symbol Token</label>
          <input
            value={form.symbol_token}
            onChange={(e) => update("symbol_token", e.target.value)}
            className="bg-terminal-bg border border-terminal-border px-2 py-1 rounded text-xs"
            placeholder="99926000"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-terminal-muted">Exchange</label>
          <select
            value={form.exchange}
            onChange={(e) => update("exchange", e.target.value)}
            className="bg-terminal-bg border border-terminal-border px-2 py-1 rounded text-xs"
          >
            <option>NSE</option>
            <option>BSE</option>
            <option>NFO</option>
          </select>
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-terminal-muted">Interval</label>
          <select
            value={form.interval}
            onChange={(e) => update("interval", e.target.value)}
            className="bg-terminal-bg border border-terminal-border px-2 py-1 rounded text-xs"
          >
            <option value="ONE_MINUTE">1m</option>
            <option value="FIVE_MINUTE">5m</option>
            <option value="FIFTEEN_MINUTE">15m</option>
            <option value="ONE_HOUR">1H</option>
            <option value="ONE_DAY">1D</option>
          </select>
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-terminal-muted">Strategy</label>
          <select
            value={form.strategy}
            onChange={(e) => update("strategy", e.target.value)}
            className="bg-terminal-bg border border-terminal-border px-2 py-1 rounded text-xs"
          >
            <option value="MA_CROSSOVER">MA Crossover</option>
            <option value="SUPERTREND">Supertrend</option>
          </select>
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-terminal-muted">From Date</label>
          <input
            value={form.from_date}
            onChange={(e) => update("from_date", e.target.value)}
            className="bg-terminal-bg border border-terminal-border px-2 py-1 rounded text-xs"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-terminal-muted">To Date</label>
          <input
            value={form.to_date}
            onChange={(e) => update("to_date", e.target.value)}
            className="bg-terminal-bg border border-terminal-border px-2 py-1 rounded text-xs"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-terminal-muted">Fast Period</label>
          <input
            type="number"
            value={form.fast_period}
            onChange={(e) => update("fast_period", parseInt(e.target.value))}
            className="bg-terminal-bg border border-terminal-border px-2 py-1 rounded text-xs"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-terminal-muted">Slow Period</label>
          <input
            type="number"
            value={form.slow_period}
            onChange={(e) => update("slow_period", parseInt(e.target.value))}
            className="bg-terminal-bg border border-terminal-border px-2 py-1 rounded text-xs"
          />
        </div>
        <div className="col-span-4 flex justify-end">
          <button
            onClick={handleSubmit}
            disabled={loading}
            className="bg-terminal-blue text-white px-6 py-1.5 rounded text-xs hover:bg-blue-500 disabled:opacity-50"
          >
            {loading ? "Running..." : "Run Backtest"}
          </button>
        </div>
      </div>

      {error && <div className="text-terminal-red text-xs panel p-2">{error}</div>}

      {/* Results */}
      {result && (
        <>
          <div className="grid grid-cols-4 gap-2">
            <MetricCard label="Total Trades" value={String(result.total_trades)} />
            <MetricCard
              label="Win Rate"
              value={`${result.win_rate.toFixed(1)}%`}
              color={result.win_rate >= 50 ? "text-terminal-green" : "text-terminal-red"}
            />
            <MetricCard
              label="Net P&L"
              value={`₹${result.total_pnl.toLocaleString()} (${result.total_pnl_pct.toFixed(2)}%)`}
              color={result.total_pnl >= 0 ? "text-terminal-green" : "text-terminal-red"}
            />
            <MetricCard
              label="Max Drawdown"
              value={`₹${result.max_drawdown.toLocaleString()} (${result.max_drawdown_pct.toFixed(2)}%)`}
              color="text-terminal-red"
            />
            <MetricCard label="Sharpe Ratio" value={result.sharpe_ratio.toFixed(3)} color={result.sharpe_ratio >= 1 ? "text-terminal-green" : "text-terminal-yellow"} />
            <MetricCard label="Profit Factor" value={result.profit_factor === Infinity ? "∞" : result.profit_factor.toFixed(3)} color={result.profit_factor >= 1.5 ? "text-terminal-green" : "text-terminal-red"} />
            <MetricCard label="Avg Win" value={`₹${result.avg_win.toFixed(2)}`} color="text-terminal-green" />
            <MetricCard label="Avg Loss" value={`₹${result.avg_loss.toFixed(2)}`} color="text-terminal-red" />
          </div>

          {/* Equity Curve */}
          {equityCurve.length > 0 && (
            <div className="panel p-3">
              <div className="text-xs text-terminal-muted mb-2">Equity Curve (Cumulative P&L)</div>
              <ResponsiveContainer width="100%" height={160}>
                <AreaChart data={equityCurve}>
                  <defs>
                    <linearGradient id="pnlGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#2979ff" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#2979ff" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <XAxis dataKey="date" tick={{ fill: "#4a5568", fontSize: 10 }} tickLine={false} />
                  <YAxis tick={{ fill: "#4a5568", fontSize: 10 }} tickLine={false} axisLine={false} />
                  <Tooltip
                    contentStyle={{ background: "#13161d", border: "1px solid #1e2433", fontSize: 11 }}
                    labelStyle={{ color: "#e2e8f0" }}
                  />
                  <ReferenceLine y={0} stroke="#4a5568" strokeDasharray="3 3" />
                  <Area type="monotone" dataKey="cumPnl" stroke="#2979ff" fill="url(#pnlGrad)" dot={false} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Trade Log */}
          <div className="panel">
            <div className="px-3 py-2 border-b border-terminal-border text-xs text-terminal-muted">
              Trade Log ({result.trades.length} trades)
            </div>
            <div className="overflow-auto max-h-48">
              <table className="w-full text-xs border-collapse">
                <thead className="sticky top-0 bg-terminal-panel">
                  <tr className="text-terminal-muted border-b border-terminal-border">
                    <th className="px-2 py-1 text-left">Entry</th>
                    <th className="px-2 py-1 text-left">Exit</th>
                    <th className="px-2 py-1 text-left">Dir</th>
                    <th className="px-2 py-1 text-right">Entry ₹</th>
                    <th className="px-2 py-1 text-right">Exit ₹</th>
                    <th className="px-2 py-1 text-right">P&L</th>
                    <th className="px-2 py-1 text-right">%</th>
                    <th className="px-2 py-1 text-right">Bars</th>
                  </tr>
                </thead>
                <tbody>
                  {result.trades.map((t, i) => (
                    <tr key={i} className="border-b border-terminal-border hover:bg-white/5">
                      <td className="px-2 py-1 text-terminal-muted">{t.entry_date.slice(0, 10)}</td>
                      <td className="px-2 py-1 text-terminal-muted">{t.exit_date.slice(0, 10)}</td>
                      <td className={clsx("px-2 py-1 font-semibold", t.direction === "LONG" ? "text-terminal-green" : "text-terminal-red")}>
                        {t.direction}
                      </td>
                      <td className="px-2 py-1 text-right">{t.entry_price.toFixed(2)}</td>
                      <td className="px-2 py-1 text-right">{t.exit_price.toFixed(2)}</td>
                      <td className={clsx("px-2 py-1 text-right font-semibold", t.pnl >= 0 ? "text-terminal-green" : "text-terminal-red")}>
                        {t.pnl >= 0 ? "+" : ""}{t.pnl.toFixed(2)}
                      </td>
                      <td className={clsx("px-2 py-1 text-right", t.pnl >= 0 ? "text-terminal-green" : "text-terminal-red")}>
                        {t.pnl_pct >= 0 ? "+" : ""}{t.pnl_pct.toFixed(2)}%
                      </td>
                      <td className="px-2 py-1 text-right text-terminal-muted">{t.bars_held}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
