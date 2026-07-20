export type EngineChannel = "momentum" | "swing";

export type SignalAction =
  | "buy"
  | "sell"
  | "short"
  | "exit"
  | "alert"
  | "circuit_breaker_triggered"
  | "data_disconnected"
  | "data_reconnected";

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
  requested_price: number;
  actual_filled_price: number;
  slippage_cost: number;
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

export type SystemStatus = "RUNNING" | "PAUSED" | "HALTED_BY_DRAWDOWN" | "DATA_DISCONNECTED";

export interface RiskStatus {
  system_status: SystemStatus;
  halted: boolean;
  halted_reason: string | null;
  paused: boolean;
  data_disconnected: boolean;
  disconnected_tickers: string[];
  daily_pnl: number;
  daily_drawdown_pct: number;
  max_daily_drawdown_pct: number;
  total_capital: number;
  allocation_pct: Record<string, number>;
  fee_rate: number;
  current_date: string | null;
}

export type GatewayMode = "mock" | "live";

export type StreamState = "connected" | "disconnected" | "reconnecting" | "verifying";

export interface StreamStatus {
  state: StreamState;
  disconnect_count: number;
  reconnect_attempts: number;
}

export interface OrderFlow {
  engine_type: string;
  ticker: string;
  order_id: string;
  signal_type: string;
  signal_to_approval_ms: number;
  approval_to_fill_ms: number;
  signal_to_fill_ms: number;
  gateway_latency_ms: number;
  requested_price: number;
  filled_price: number;
  slippage_cost: number;
  status: string;
  timestamp: string;
}

export interface TelemetryStats {
  order_count: number;
  cumulative_slippage_cost: number;
  persisted_slippage_cost: number;
  avg_signal_to_approval_ms: number;
  avg_approval_to_fill_ms: number;
  avg_gateway_latency_ms: number;
  connection_latency_ms: number;
  last_flow: OrderFlow | null;
  gateway: { name: string; mode: GatewayMode };
  system_status: SystemStatus;
  data_disconnected: boolean;
  disconnected_tickers: string[];
  streams: Record<string, StreamStatus>;
  boot_reconciliation: {
    matched: string[];
    healed: string[];
    cleared: string[];
    discrepancies: number;
  };
}
