"use client";

import useSWR from "swr";
import { searchInstruments } from "@/lib/api";
import clsx from "clsx";

// Static Nifty 50 constituents for the heatmap
// In production this would be fetched dynamically
const NIFTY50_SYMBOLS = [
  "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK",
  "HINDUNILVR", "ITC", "SBIN", "BAJFINANCE", "BHARTIARTL",
  "KOTAKBANK", "LT", "ASIANPAINT", "AXISBANK", "MARUTI",
  "SUNPHARMA", "WIPRO", "ULTRACEMCO", "NESTLEIND", "TITAN",
  "ONGC", "NTPC", "POWERGRID", "JSWSTEEL", "TATAMOTORS",
  "HCLTECH", "TECHM", "DRREDDY", "COALINDIA", "EICHERMOT",
  "GRASIM", "BAJAJFINSV", "BPCL", "BRITANNIA", "DIVISLAB",
  "ADANIPORTS", "TATACONSUM", "CIPLA", "HEROMOTOCO", "UPL",
  "HINDALCO", "INDUSINDBK", "M&M", "SBILIFE", "HDFCLIFE",
  "BAJAJ-AUTO", "APOLLOHOSP", "TATASTEEL", "ADANIENT", "LTM",
];

// Simulate heatmap data (in production, fetch from backend)
function generateMockData() {
  return NIFTY50_SYMBOLS.map((sym) => ({
    symbol: sym,
    change: (Math.random() * 6 - 3),  // -3% to +3%
    volume_change: (Math.random() * 40 - 20),  // -20% to +20% OI
  }));
}

function heatmapColor(pct: number): string {
  if (pct >= 2) return "bg-green-600";
  if (pct >= 1) return "bg-green-700/70";
  if (pct >= 0.25) return "bg-green-800/50";
  if (pct >= -0.25) return "bg-gray-700/50";
  if (pct >= -1) return "bg-red-800/50";
  if (pct >= -2) return "bg-red-700/70";
  return "bg-red-600";
}

function textColor(pct: number): string {
  return pct >= 0 ? "text-green-300" : "text-red-300";
}

export default function Heatmap() {
  // In a real app this refreshes every 30s with live data
  const data = generateMockData();
  const sorted = [...data].sort((a, b) => b.change - a.change);

  return (
    <div className="flex flex-col h-full">
      <div className="px-3 py-2 border-b border-terminal-border text-xs flex items-center gap-3">
        <span className="text-terminal-text font-semibold">Nifty 50 Heatmap</span>
        <span className="text-terminal-muted">price % change</span>
        <div className="flex items-center gap-1 ml-auto">
          <span className="w-3 h-3 rounded bg-green-600 inline-block" />
          <span className="text-terminal-muted text-xs">Bullish</span>
          <span className="w-3 h-3 rounded bg-red-600 inline-block ml-2" />
          <span className="text-terminal-muted text-xs">Bearish</span>
        </div>
      </div>

      <div className="flex-1 overflow-auto p-2">
        <div className="grid grid-cols-5 gap-1.5 auto-rows-fr">
          {sorted.map((item) => (
            <div
              key={item.symbol}
              className={clsx(
                "rounded p-1.5 flex flex-col justify-between min-h-[52px] cursor-pointer transition-opacity hover:opacity-80",
                heatmapColor(item.change)
              )}
            >
              <span className="text-white text-xs font-semibold leading-tight truncate">
                {item.symbol}
              </span>
              <div>
                <div className={clsx("text-xs font-bold", textColor(item.change))}>
                  {item.change >= 0 ? "+" : ""}{item.change.toFixed(2)}%
                </div>
                <div className="text-gray-400 text-xs">
                  OI {item.volume_change >= 0 ? "+" : ""}{item.volume_change.toFixed(1)}%
                </div>
              </div>
            </div>
          ))}
        </div>

        {/* Legend: OI change context */}
        <div className="mt-3 flex gap-4 text-xs text-terminal-muted border-t border-terminal-border pt-2">
          <div>
            <span className="text-terminal-green font-semibold">↑ Price + ↑ OI</span>
            <span className="ml-1">Long Buildup</span>
          </div>
          <div>
            <span className="text-terminal-green font-semibold">↑ Price + ↓ OI</span>
            <span className="ml-1">Short Covering</span>
          </div>
          <div>
            <span className="text-terminal-red font-semibold">↓ Price + ↑ OI</span>
            <span className="ml-1">Short Buildup</span>
          </div>
          <div>
            <span className="text-terminal-red font-semibold">↓ Price + ↓ OI</span>
            <span className="ml-1">Long Unwinding</span>
          </div>
        </div>
      </div>
    </div>
  );
}
