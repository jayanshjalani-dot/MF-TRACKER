"use client";

import { useState } from "react";
import useSWR from "swr";
import { getOrderBook, getPositions, cancelOrder } from "@/lib/api";
import type { Order, Position } from "@/lib/types";
import clsx from "clsx";

type Tab = "orders" | "positions";

function StatusBadge({ status }: { status: string }) {
  const color =
    status === "complete"
      ? "text-terminal-green"
      : status === "rejected"
      ? "text-terminal-red"
      : status === "cancelled"
      ? "text-terminal-muted"
      : "text-terminal-yellow";
  return <span className={`uppercase text-xs font-semibold ${color}`}>{status}</span>;
}

function PnlCell({ pnl }: { pnl: number }) {
  return (
    <span className={pnl >= 0 ? "text-terminal-green" : "text-terminal-red"}>
      {pnl >= 0 ? "+" : ""}
      {pnl.toFixed(2)}
    </span>
  );
}

export default function OrderPanel() {
  const [tab, setTab] = useState<Tab>("orders");

  const { data: orders, mutate: mutateOrders } = useSWR<Order[]>(
    tab === "orders" ? "orders" : null,
    getOrderBook,
    { refreshInterval: 5_000 }
  );

  const { data: positions } = useSWR<Position[]>(
    tab === "positions" ? "positions" : null,
    getPositions,
    { refreshInterval: 5_000 }
  );

  const handleCancel = async (orderId: string) => {
    try {
      await cancelOrder(orderId);
      mutateOrders();
    } catch (e: any) {
      alert(`Cancel failed: ${e.response?.data?.detail || e.message}`);
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Tabs */}
      <div className="flex border-b border-terminal-border text-xs">
        {(["orders", "positions"] as Tab[]).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={clsx(
              "px-4 py-2 uppercase tracking-wider transition-colors",
              tab === t
                ? "text-terminal-blue border-b-2 border-terminal-blue"
                : "text-terminal-muted hover:text-terminal-text"
            )}
          >
            {t}
          </button>
        ))}
      </div>

      {/* Order Book */}
      {tab === "orders" && (
        <div className="overflow-auto flex-1">
          <table className="w-full text-xs border-collapse">
            <thead className="sticky top-0 bg-terminal-panel">
              <tr className="text-terminal-muted border-b border-terminal-border">
                <th className="px-2 py-1 text-left">Symbol</th>
                <th className="px-2 py-1 text-left">Type</th>
                <th className="px-2 py-1 text-right">Qty</th>
                <th className="px-2 py-1 text-right">Price</th>
                <th className="px-2 py-1 text-right">Filled</th>
                <th className="px-2 py-1 text-center">Status</th>
                <th className="px-2 py-1 text-center">Action</th>
              </tr>
            </thead>
            <tbody>
              {(orders || []).map((o) => (
                <tr key={o.orderid} className="border-b border-terminal-border hover:bg-white/5">
                  <td className="px-2 py-1 text-terminal-text">{o.tradingsymbol}</td>
                  <td
                    className={clsx(
                      "px-2 py-1 font-semibold",
                      o.transactiontype === "BUY" ? "text-terminal-green" : "text-terminal-red"
                    )}
                  >
                    {o.transactiontype}
                  </td>
                  <td className="px-2 py-1 text-right">{o.quantity}</td>
                  <td className="px-2 py-1 text-right">{o.price || "MKT"}</td>
                  <td className="px-2 py-1 text-right text-terminal-muted">{o.filledshares}</td>
                  <td className="px-2 py-1 text-center">
                    <StatusBadge status={o.status} />
                  </td>
                  <td className="px-2 py-1 text-center">
                    {o.status === "open" && (
                      <button
                        onClick={() => handleCancel(o.orderid)}
                        className="text-terminal-red text-xs hover:underline"
                      >
                        Cancel
                      </button>
                    )}
                  </td>
                </tr>
              ))}
              {(!orders || orders.length === 0) && (
                <tr>
                  <td colSpan={7} className="px-2 py-4 text-center text-terminal-muted">
                    No orders today
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Positions */}
      {tab === "positions" && (
        <div className="overflow-auto flex-1">
          <table className="w-full text-xs border-collapse">
            <thead className="sticky top-0 bg-terminal-panel">
              <tr className="text-terminal-muted border-b border-terminal-border">
                <th className="px-2 py-1 text-left">Symbol</th>
                <th className="px-2 py-1 text-right">Net Qty</th>
                <th className="px-2 py-1 text-right">Avg</th>
                <th className="px-2 py-1 text-right">LTP</th>
                <th className="px-2 py-1 text-right">P&L</th>
              </tr>
            </thead>
            <tbody>
              {(positions || []).map((p, i) => (
                <tr key={i} className="border-b border-terminal-border hover:bg-white/5">
                  <td className="px-2 py-1 text-terminal-text">{p.tradingsymbol}</td>
                  <td
                    className={clsx(
                      "px-2 py-1 text-right font-semibold",
                      p.netqty > 0 ? "text-terminal-green" : p.netqty < 0 ? "text-terminal-red" : "text-terminal-muted"
                    )}
                  >
                    {p.netqty}
                  </td>
                  <td className="px-2 py-1 text-right text-terminal-muted">
                    {p.netqty > 0 ? p.buyavgprice?.toFixed(2) : p.sellavgprice?.toFixed(2)}
                  </td>
                  <td className="px-2 py-1 text-right">{p.ltp?.toFixed(2)}</td>
                  <td className="px-2 py-1 text-right">
                    <PnlCell pnl={p.pnl} />
                  </td>
                </tr>
              ))}
              {(!positions || positions.length === 0) && (
                <tr>
                  <td colSpan={5} className="px-2 py-4 text-center text-terminal-muted">
                    No open positions
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
