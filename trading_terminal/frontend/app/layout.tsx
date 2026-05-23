import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "NSE/BSE Institutional Trading Terminal",
  description: "Real-time options trading terminal with Black-Scholes Greeks, sentiment engine, and backtesting",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="bg-terminal-bg text-terminal-text font-mono overflow-hidden">
        {children}
      </body>
    </html>
  );
}
