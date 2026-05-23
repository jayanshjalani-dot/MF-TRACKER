"use client";

import useSWR from "swr";
import { getSentiment } from "@/lib/api";
import type { SentimentResult } from "@/lib/types";
import { AlertTriangle, TrendingUp, TrendingDown, Minus } from "lucide-react";

function ScoreGauge({ score }: { score: number }) {
  // Map -1..+1 → 0%..100%
  const pct = ((score + 1) / 2) * 100;
  const color =
    score >= 0.2 ? "#00c853" : score <= -0.2 ? "#ff1744" : "#ffd600";

  return (
    <div className="flex items-center gap-2 w-full">
      <span className="text-xs text-terminal-muted w-8">-1</span>
      <div className="flex-1 h-2 rounded bg-terminal-border relative overflow-hidden">
        <div
          className="absolute top-0 left-0 h-full rounded transition-all duration-500"
          style={{ width: `${pct}%`, background: color }}
        />
        {/* Center neutral line */}
        <div className="absolute top-0 left-1/2 w-px h-full bg-terminal-muted opacity-40" />
      </div>
      <span className="text-xs text-terminal-muted w-8 text-right">+1</span>
    </div>
  );
}

function LabelIcon({ label }: { label: string }) {
  if (label.includes("Bullish")) return <TrendingUp className="w-4 h-4 text-terminal-green" />;
  if (label.includes("Bearish")) return <TrendingDown className="w-4 h-4 text-terminal-red" />;
  return <Minus className="w-4 h-4 text-terminal-yellow" />;
}

export default function SentimentBar() {
  const { data, isLoading } = useSWR<SentimentResult>(
    "sentiment",
    () => getSentiment(),
    { refreshInterval: 300_000 } // refresh every 5 minutes
  );

  if (isLoading || !data) {
    return (
      <div className="panel px-3 py-2 flex items-center gap-2 text-terminal-muted text-xs">
        Loading sentiment...
      </div>
    );
  }

  const scoreColor =
    data.score >= 0.2
      ? "text-terminal-green"
      : data.score <= -0.2
      ? "text-terminal-red"
      : "text-terminal-yellow";

  return (
    <div className="panel px-3 py-2 flex items-center gap-4">
      {/* Block warning */}
      {data.trade_blocked && (
        <div className="flex items-center gap-1 text-terminal-red text-xs font-bold animate-pulse">
          <AlertTriangle className="w-3 h-3" />
          TRADE BLOCKED
        </div>
      )}

      <div className="flex items-center gap-1">
        <LabelIcon label={data.label} />
        <span className={`text-xs font-semibold ${scoreColor}`}>{data.label}</span>
        <span className={`text-xs ml-1 ${scoreColor}`}>({data.score.toFixed(3)})</span>
      </div>

      <div className="flex-1 max-w-48">
        <ScoreGauge score={data.score} />
      </div>

      <div className="text-xs text-terminal-muted">
        {data.headlines.length} headlines
      </div>
    </div>
  );
}
