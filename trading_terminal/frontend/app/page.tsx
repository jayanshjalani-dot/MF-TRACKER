"use client";

import { useState, useEffect, useRef } from "react";
import dynamic from "next/dynamic";
import useSWR from "swr";
import { getNiftySpot, getBankNiftySpot, createTickStream } from "@/lib/api";
import type { Tick } from "@/lib/types";
import clsx from "clsx";

// Dynamically import heavy components (chart requires browser APIs)
const TradingChart = dynamic(() => import("@/components/TradingChart"), { ssr: false });
const OptionChain = dynamic(() => import("@/components/OptionChain"), { ssr: false });
const OrderPanel = dynamic(() => import("@/components/OrderPanel"), { ssr: false });
const SentimentBar = dynamic(() => import("@/components/SentimentBar"), { ssr: false });
const Heatmap = dynamic(() => import("@/components/Heatmap"), { ssr: false });
const BacktestPanel = dynamic(() => import("@/components/BacktestPanel"), { ssr: false });

type RightPanel = "option_chain" | "heatmap" | "backtest";

function SpotTicker({ label, spot, change }: { label: string; spot: number; change: number }) {
  const isUp = change >= 0;
  return (
    <div className="flex items-baseline gap-2 px-3 border-r border-terminal-border last:border-0">
      <span className="text-terminal-muted text-xs uppercase">{label}</span>
      <span className="text-terminal-text text-sm font-bold tabular-nums">
        {spot.toLocaleString("en-IN", { minimumFractionDigits: 2 })}
      </span>
      <span className={clsx("text-xs tabular-nums", isUp ? "text-terminal-green" : "text-terminal-red")}>
        {isUp ? "▲" : "▼"} {Math.abs(change).toFixed(2)}%
      </span>
    </div>
  );
}

export default function Terminal() {
  const [underlying, setUnderlying] = useState<"NIFTY" | "BANKNIFTY">("NIFTY");
  const [rightPanel, setRightPanel] = useState<RightPanel>("option_chain");
  const [ticks, setTicks] = useState<Record<string, Tick>>({});
  const esRef = useRef<EventSource | null>(null);

  const { data: niftyData } = useSWR("nifty-spot", getNiftySpot, { refreshInterval: 5000 });
  const { data: bnData } = useSWR("bnifty-spot", getBankNiftySpot, { refreshInterval: 5000 });

  const niftySpot = niftyData?.spot || 0;
  const bnSpot = bnData?.spot || 0;
  const activeSpot = underlying === "NIFTY" ? niftySpot : bnSpot;

  // Connect to SSE tick stream
  useEffect(() => {
    esRef.current = createTickStream((tick) => {
      const t = tick as Tick;
      if (t.token) {
        setTicks((prev) => ({ ...prev, [t.token]: t }));
      }
    });
    return () => esRef.current?.close();
  }, []);

  // Chart token for the currently selected underlying
  const chartToken = underlying === "NIFTY" ? "99926000" : "99926009";
  const chartSymbol = underlying === "NIFTY" ? "NIFTY 50" : "NIFTY BANK";

  return (
    <div className="flex flex-col h-screen overflow-hidden select-none">
      {/* ── Top Bar ─────────────────────────────────────────────── */}
      <header className="flex items-center h-9 border-b border-terminal-border px-3 shrink-0">
        {/* Logo */}
        <div className="flex items-center gap-2 mr-6">
          <div className="w-2 h-2 rounded-full bg-terminal-green animate-pulse" />
          <span className="text-xs font-bold text-terminal-text tracking-widest uppercase">
            NSE/BSE Terminal
          </span>
        </div>

        {/* Spot prices */}
        {niftySpot > 0 && (
          <SpotTicker label="Nifty 50" spot={niftySpot} change={0.42} />
        )}
        {bnSpot > 0 && (
          <SpotTicker label="Bank Nifty" spot={bnSpot} change={-0.18} />
        )}

        {/* Underlying selector */}
        <div className="flex gap-1 ml-6">
          {(["NIFTY", "BANKNIFTY"] as const).map((u) => (
            <button
              key={u}
              onClick={() => setUnderlying(u)}
              className={clsx(
                "px-3 py-0.5 rounded text-xs transition-colors",
                underlying === u
                  ? "bg-terminal-blue text-white"
                  : "text-terminal-muted hover:text-terminal-text"
              )}
            >
              {u}
            </button>
          ))}
        </div>

        {/* Right panel selector */}
        <div className="flex gap-1 ml-auto">
          {(["option_chain", "heatmap", "backtest"] as RightPanel[]).map((p) => (
            <button
              key={p}
              onClick={() => setRightPanel(p)}
              className={clsx(
                "px-3 py-0.5 rounded text-xs uppercase tracking-wide transition-colors",
                rightPanel === p
                  ? "bg-terminal-panel border border-terminal-blue text-terminal-blue"
                  : "text-terminal-muted hover:text-terminal-text"
              )}
            >
              {p.replace("_", " ")}
            </button>
          ))}
        </div>
      </header>

      {/* ── Sentiment Bar ────────────────────────────────────────── */}
      <div className="shrink-0 px-2 py-1 border-b border-terminal-border">
        <SentimentBar />
      </div>

      {/* ── Main Content ─────────────────────────────────────────── */}
      <div className="flex flex-1 min-h-0 gap-0">
        {/* Left column: Chart + Orders */}
        <div className="flex flex-col flex-1 min-w-0 border-r border-terminal-border">
          {/* TradingView-style chart */}
          <div className="h-[55%] border-b border-terminal-border">
            <TradingChart token={chartToken} exchange="NSE" symbol={chartSymbol} />
          </div>

          {/* Order book / positions */}
          <div className="flex-1 min-h-0">
            <OrderPanel />
          </div>
        </div>

        {/* Right column: Option Chain / Heatmap / Backtest */}
        <div className="w-[48%] min-w-0 flex flex-col">
          {rightPanel === "option_chain" && activeSpot > 0 && (
            <OptionChain underlying={underlying} spot={activeSpot} />
          )}
          {rightPanel === "heatmap" && <Heatmap />}
          {rightPanel === "backtest" && <BacktestPanel />}
        </div>
      </div>
    </div>
  );
}
