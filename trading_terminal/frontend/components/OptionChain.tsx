"use client";

import { useState, useCallback } from "react";
import useSWR from "swr";
import { getOptionChain } from "@/lib/api";
import type { OptionChainResponse, OptionStrike, PlaceOrderPayload } from "@/lib/types";
import { placeOrder } from "@/lib/api";
import clsx from "clsx";

interface Props {
  underlying: "NIFTY" | "BANKNIFTY";
  spot: number;
}

function fmt(n: number | null, decimals = 2): string {
  if (n === null || n === undefined) return "—";
  return n.toFixed(decimals);
}

function oiBar(oi: number | null, maxOi: number): string {
  if (!oi || maxOi === 0) return "0%";
  return `${Math.min(100, (oi / maxOi) * 100).toFixed(1)}%`;
}

function StrikeRow({
  s,
  isAtm,
  maxCeOi,
  maxPeOi,
  spot,
  underlying,
  onOrder,
}: {
  s: OptionStrike;
  isAtm: boolean;
  maxCeOi: number;
  maxPeOi: number;
  spot: number;
  underlying: string;
  onOrder: (payload: Partial<PlaceOrderPayload>) => void;
}) {
  const ceColor = s.ce_ltp ? "text-terminal-red" : "text-terminal-muted";
  const peColor = s.pe_ltp ? "text-terminal-green" : "text-terminal-muted";

  return (
    <tr className={clsx("border-b border-terminal-border hover:bg-white/5 transition-colors", isAtm && "atm-row")}>
      {/* CE side */}
      <td className={clsx("px-2 py-1 text-right", ceColor)}>{fmt(s.ce_iv)}%</td>
      <td className="px-2 py-1 text-right text-terminal-muted text-xs">{fmt(s.ce_delta, 3)}</td>
      <td className="px-2 py-1 text-right text-terminal-muted text-xs">{fmt(s.ce_theta, 2)}</td>
      <td className="px-2 py-1 text-right text-terminal-muted text-xs">
        <div className="relative h-3 w-16 bg-terminal-border rounded overflow-hidden ml-auto">
          <div className="absolute right-0 top-0 h-full bg-terminal-red/50 rounded" style={{ width: oiBar(s.ce_oi, maxCeOi) }} />
        </div>
      </td>
      <td className={clsx("px-2 py-1 text-right font-semibold", ceColor)}>
        <button
          className="hover:text-terminal-red hover:underline"
          onClick={() =>
            onOrder({ transactiontype: "BUY", tradingsymbol: `${underlying}${s.expiry}${s.strike}CE`, exchange: "NFO", price: s.ce_ltp || 0 })
          }
        >
          {fmt(s.ce_ltp)}
        </button>
      </td>

      {/* Strike */}
      <td className={clsx("px-3 py-1 text-center font-bold text-sm", isAtm ? "text-terminal-blue" : "text-terminal-text")}>
        {s.strike.toLocaleString()}
        {isAtm && <span className="ml-1 text-xs text-terminal-blue opacity-70">ATM</span>}
      </td>

      {/* PE side */}
      <td className={clsx("px-2 py-1 text-left font-semibold", peColor)}>
        <button
          className="hover:text-terminal-green hover:underline"
          onClick={() =>
            onOrder({ transactiontype: "BUY", tradingsymbol: `${underlying}${s.expiry}${s.strike}PE`, exchange: "NFO", price: s.pe_ltp || 0 })
          }
        >
          {fmt(s.pe_ltp)}
        </button>
      </td>
      <td className="px-2 py-1 text-left text-terminal-muted text-xs">
        <div className="relative h-3 w-16 bg-terminal-border rounded overflow-hidden">
          <div className="absolute left-0 top-0 h-full bg-terminal-green/50 rounded" style={{ width: oiBar(s.pe_oi, maxPeOi) }} />
        </div>
      </td>
      <td className="px-2 py-1 text-left text-terminal-muted text-xs">{fmt(s.pe_delta, 3)}</td>
      <td className="px-2 py-1 text-left text-terminal-muted text-xs">{fmt(s.pe_theta, 2)}</td>
      <td className={clsx("px-2 py-1 text-left", peColor)}>{fmt(s.pe_iv)}%</td>
    </tr>
  );
}

export default function OptionChain({ underlying, spot }: Props) {
  const [orderDraft, setOrderDraft] = useState<Partial<PlaceOrderPayload> | null>(null);
  const [orderMsg, setOrderMsg] = useState<string | null>(null);

  const { data, isLoading, error } = useSWR<OptionChainResponse>(
    `option-chain-${underlying}`,
    () => getOptionChain(underlying),
    { refreshInterval: 15_000 }
  );

  const handleOrder = useCallback((draft: Partial<PlaceOrderPayload>) => {
    setOrderDraft({
      ...draft,
      variety: "NORMAL",
      ordertype: "MARKET",
      producttype: "INTRADAY",
      duration: "DAY",
      quantity: 1,
      symboltoken: "",
    });
  }, []);

  const submitOrder = async () => {
    if (!orderDraft) return;
    try {
      await placeOrder(orderDraft as PlaceOrderPayload);
      setOrderMsg("✓ Order placed successfully");
      setOrderDraft(null);
    } catch (e: any) {
      setOrderMsg(`✗ ${e.response?.data?.detail || e.message}`);
    }
  };

  if (isLoading) return <div className="p-4 text-terminal-muted text-xs">Loading option chain...</div>;
  if (error) return <div className="p-4 text-terminal-red text-xs">Failed to load option chain</div>;
  if (!data) return null;

  const chain = data.chain;
  const maxCeOi = Math.max(...chain.map((s) => s.ce_oi || 0));
  const maxPeOi = Math.max(...chain.map((s) => s.pe_oi || 0));
  const atmStrike = chain.reduce((a, b) =>
    Math.abs(a.strike - spot) < Math.abs(b.strike - spot) ? a : b
  )?.strike;

  return (
    <div className="flex flex-col h-full overflow-hidden">
      {/* Header Stats */}
      <div className="flex items-center gap-4 px-3 py-2 border-b border-terminal-border text-xs">
        <span className="text-terminal-muted">Max Pain:</span>
        <span className="text-terminal-yellow font-bold">{data.max_pain.toLocaleString()}</span>
        <span className="text-terminal-muted ml-2">PCR:</span>
        <span className={clsx("font-bold", data.pcr > 1.2 ? "text-terminal-green" : data.pcr < 0.7 ? "text-terminal-red" : "text-terminal-yellow")}>
          {data.pcr.toFixed(3)}
        </span>
        <span className="text-terminal-muted ml-2">Expiry:</span>
        <span className="text-terminal-text">{chain[0]?.expiry}</span>
      </div>

      {/* Order draft modal */}
      {orderDraft && (
        <div className="flex items-center gap-3 px-3 py-2 bg-terminal-blue/10 border-b border-terminal-blue/30 text-xs">
          <span className={clsx("font-bold", orderDraft.transactiontype === "BUY" ? "text-terminal-green" : "text-terminal-red")}>
            {orderDraft.transactiontype}
          </span>
          <span>{orderDraft.tradingsymbol}</span>
          <span className="text-terminal-muted">@ MKT</span>
          <input
            type="number"
            min={1}
            value={orderDraft.quantity}
            onChange={(e) => setOrderDraft({ ...orderDraft, quantity: parseInt(e.target.value) || 1 })}
            className="w-16 bg-terminal-bg border border-terminal-border px-1 py-0.5 rounded text-xs"
          />
          <span className="text-terminal-muted">lots</span>
          <button onClick={submitOrder} className="bg-terminal-blue px-3 py-1 rounded text-white text-xs hover:bg-blue-500">
            CONFIRM
          </button>
          <button onClick={() => setOrderDraft(null)} className="text-terminal-muted hover:text-terminal-red">
            CANCEL
          </button>
        </div>
      )}
      {orderMsg && (
        <div className="px-3 py-1 text-xs text-terminal-green border-b border-terminal-border">
          {orderMsg}
        </div>
      )}

      {/* Table */}
      <div className="overflow-auto flex-1">
        <table className="w-full text-xs border-collapse">
          <thead className="sticky top-0 bg-terminal-panel z-10">
            <tr className="text-terminal-muted border-b border-terminal-border">
              <th className="px-2 py-1 text-right">IV</th>
              <th className="px-2 py-1 text-right">Δ</th>
              <th className="px-2 py-1 text-right">Θ</th>
              <th className="px-2 py-1 text-right">OI</th>
              <th className="px-2 py-1 text-right text-terminal-red">CALL</th>
              <th className="px-3 py-1 text-center">STRIKE</th>
              <th className="px-2 py-1 text-left text-terminal-green">PUT</th>
              <th className="px-2 py-1 text-left">OI</th>
              <th className="px-2 py-1 text-left">Δ</th>
              <th className="px-2 py-1 text-left">Θ</th>
              <th className="px-2 py-1 text-left">IV</th>
            </tr>
          </thead>
          <tbody>
            {chain.map((s) => (
              <StrikeRow
                key={s.strike}
                s={s}
                isAtm={s.strike === atmStrike}
                maxCeOi={maxCeOi}
                maxPeOi={maxPeOi}
                spot={spot}
                underlying={underlying}
                onOrder={handleOrder}
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
