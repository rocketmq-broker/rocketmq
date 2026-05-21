// ─── AMQP Topology for Game Telemetry & Analytics Platform ─────────────────────

export const AMQP_URL =
  process.env.AMQP_URL || 'amqp://guest:guest@127.0.0.1:5672/';

// Core Exchanges
export const EXCHANGE_GAME = 'game.events'; // Topic
export const EXCHANGE_ANALYTICS = 'analytics.events'; // Direct
export const EXCHANGE_METRICS = 'metrics.exchange'; // Fanout
export const EXCHANGE_DLX = 'dlx.exchange'; // Direct

// New Exchanges for Clustered Topology Testing
export const EXCHANGE_MATCHMAKING = 'matchmaking.events'; // Direct
export const EXCHANGE_SECURITY = 'security.broadcast'; // Fanout
export const EXCHANGE_LEADERBOARD = 'leaderboard.updates'; // Topic

// Queues
export const QUEUE_SESSION_TICKS = 'game.session.ticks';
export const QUEUE_SESSION_ACTIONS = 'game.session.actions';
export const QUEUE_TELEMETRY = 'game.analytics.telemetry';
export const QUEUE_ANTI_CHEAT = 'game.anti_cheat.alerts';
export const QUEUE_MATCHMAKING = 'game.matchmaking.lobby';
export const QUEUE_METRICS_LOGS = 'game.metrics.logs';
export const QUEUE_DLQ = 'dead-letter-queue';

// New Queues for Advanced Clustered Testing
export const QUEUE_LOBBY_CREATED = 'game.lobby.created';
export const QUEUE_SECURITY_BROADCAST = 'game.security.bans';
export const QUEUE_LEADERBOARD_UPDATES = 'game.leaderboards';
export const QUEUE_LOBBY_MAINTENANCE = 'game.lobby.maintenance';

// Routing Keys / Patterns
export const RK_SESSION_TICK_PATTERN = 'game.session.*.tick';
export const RK_SESSION_ACTION_PATTERN = 'game.session.*.action';
export const RK_ALL_SESSION_EVENTS = 'game.session.*.*';
export const RK_ANTI_CHEAT_ALERT = 'anti_cheat.alert';
export const RK_MATCHMAKING_LOBBY = 'matchmaking.lobby';

// New Routing Keys / Patterns
export const RK_LOBBY_CREATED = 'lobby.created';
export const RK_LEADERBOARD_GLOBAL = 'leaderboard.global.rankings';
