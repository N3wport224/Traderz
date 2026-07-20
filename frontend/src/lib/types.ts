export type EngineChannel = "momentum" | "swing";

export type SignalAction = "buy" | "sell" | "short" | "exit" | "alert";

export interface TradeSignal {
  engine: string;
  symbol: string;
  action: SignalAction;
  price: number;
  timestamp: string;
  reason: string;
  metadata: Record<string, unknown>;
}

export interface EquityPoint {
  timestamp: string;
  equity: number;
}
