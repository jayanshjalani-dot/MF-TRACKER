"use client";

import { useEffect, useRef, useState } from "react";
import {
  createChart,
  IChartApi,
  ISeriesApi,
  CandlestickData,
  Time,
} from "lightweight-charts";
import { getHistorical } from "@/lib/api";
import { format, subMonths, subDays } from "date-fns";

interface Props {
  token: string;
  exchange: string;
  symbol: string;
}

const INTERVALS = [
  { label: "1m", value: "ONE_MINUTE", days: 1 },
  { label: "5m", value: "FIVE_MINUTE", days: 5 },
  { label: "15m", value: "FIFTEEN_MINUTE", days: 10 },
  { label: "1H", value: "ONE_HOUR", days: 30 },
  { label: "1D", value: "ONE_DAY", days: 365 },
];

export default function TradingChart({ token, exchange, symbol }: Props) {
  const chartRef = useRef<HTMLDivElement>(null);
  const chartInstance = useRef<IChartApi | null>(null);
  const candleSeries = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeries = useRef<ISeriesApi<"Histogram"> | null>(null);

  const [interval, setInterval] = useState(INTERVALS[4]); // default 1D
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Create chart on mount
  useEffect(() => {
    if (!chartRef.current) return;

    const chart = createChart(chartRef.current, {
      layout: {
        background: { color: "#13161d" },
        textColor: "#e2e8f0",
      },
      grid: {
        vertLines: { color: "#1e2433" },
        horzLines: { color: "#1e2433" },
      },
      crosshair: { mode: 1 },
      timeScale: {
        borderColor: "#1e2433",
        timeVisible: true,
        secondsVisible: false,
      },
      rightPriceScale: { borderColor: "#1e2433" },
      width: chartRef.current.clientWidth,
      height: chartRef.current.clientHeight,
    });

    const candles = chart.addCandlestickSeries({
      upColor: "#00c853",
      downColor: "#ff1744",
      borderDownColor: "#ff1744",
      borderUpColor: "#00c853",
      wickDownColor: "#ff1744",
      wickUpColor: "#00c853",
    });

    const volume = chart.addHistogramSeries({
      color: "#26a69a",
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
    });
    chart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });

    chartInstance.current = chart;
    candleSeries.current = candles;
    volumeSeries.current = volume;

    const ro = new ResizeObserver(() => {
      if (chartRef.current) {
        chart.applyOptions({
          width: chartRef.current.clientWidth,
          height: chartRef.current.clientHeight,
        });
      }
    });
    ro.observe(chartRef.current);

    return () => {
      ro.disconnect();
      chart.remove();
    };
  }, []);

  // Load data whenever token or interval changes
  useEffect(() => {
    if (!token || !candleSeries.current) return;
    setLoading(true);
    setError(null);

    const to = new Date();
    const from = subDays(to, interval.days);
    const fmt2 = (d: Date) => format(d, "yyyy-MM-dd HH:mm");

    getHistorical(token, exchange, interval.value, fmt2(from), fmt2(to))
      .then((bars) => {
        const candles: CandlestickData[] = bars.map((b) => ({
          time: (new Date(b.timestamp).getTime() / 1000) as Time,
          open: b.open,
          high: b.high,
          low: b.low,
          close: b.close,
        }));
        const vols = bars.map((b) => ({
          time: (new Date(b.timestamp).getTime() / 1000) as Time,
          value: b.volume,
          color: b.close >= b.open ? "rgba(0,200,83,0.4)" : "rgba(255,23,68,0.4)",
        }));

        candleSeries.current?.setData(candles);
        volumeSeries.current?.setData(vols);
        chartInstance.current?.timeScale().fitContent();
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [token, exchange, interval]);

  return (
    <div className="flex flex-col h-full">
      {/* Toolbar */}
      <div className="flex items-center gap-2 px-3 py-1.5 border-b border-terminal-border text-xs">
        <span className="font-bold text-terminal-text">{symbol}</span>
        <span className="text-terminal-muted">{exchange}</span>
        <div className="flex gap-1 ml-4">
          {INTERVALS.map((iv) => (
            <button
              key={iv.value}
              onClick={() => setInterval(iv)}
              className={`px-2 py-0.5 rounded text-xs transition-colors ${
                interval.value === iv.value
                  ? "bg-terminal-blue text-white"
                  : "text-terminal-muted hover:text-terminal-text"
              }`}
            >
              {iv.label}
            </button>
          ))}
        </div>
        {loading && <span className="text-terminal-muted ml-auto">Loading...</span>}
        {error && <span className="text-terminal-red ml-auto text-xs">{error}</span>}
      </div>

      {/* Chart canvas */}
      <div ref={chartRef} className="flex-1 min-h-0" />
    </div>
  );
}
