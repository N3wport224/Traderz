export type EngineChannel = "momentum" | "swing";

export type SignalAction = "buy" | "sell" | "short" | "exit" | "alert" | "circuit_breaker_triggered";

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

export interface Trade {
  id: number;
  engine_type: string;
  asset_ticker: string;
  entry_timestamp: string;
  exit_timestamp: string;
  entry_price: number;
  exit_price: number;
  position_size: number;
  fees: number;
  net_profit: number;
}

export interface MomentumConfig {
  opening_range_minutes: number;
  time_stop_minutes: number;
}

export interface SwingConfig {
  min_touches: number;
  touch_tolerance_pct: number;
}

export interface StrategyConfig {
  momentum: MomentumConfig;
  swing: SwingConfig;
}

export type SystemStatus = "RUNNING" | "PAUSED" | "HALTED_BY_DRAWDOWN";

export interface RiskStatus {
  system_status: SystemStatus;
  halted: boolean;
  halted_reason: string | null;
  paused: boolean;
  daily_pnl: number;
  daily_drawdown_pct: number;
  max_daily_drawdown_pct: number;
  total_capital: number;
  allocation_pct: Record<string, number>;
  fee_rate: number;
  current_date: string | null;
}
