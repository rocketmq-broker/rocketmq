// RabbitMQ API Types - Complete type definitions for all API responses

// Overview & Cluster
export interface Overview {
  management_version: string;
  rates_mode: string;
  sample_retention_policies: {
    global: number[];
    basic: number[];
    detailed: number[];
  };
  exchange_types: ExchangeType[];
  product_version: string;
  product_name: string;
  rabbitmq_version: string;
  cluster_name: string;
  erlang_version: string;
  erlang_full_version: string;
  release_series_support_status: string;
  disable_stats: boolean;
  enable_queue_totals: boolean;
  message_stats?: MessageStats;
  churn_rates?: ChurnRates;
  queue_totals?: QueueTotals;
  object_totals: ObjectTotals;
  statistics_db_event_queue: number;
  node: string;
  listeners: Listener[];
  contexts: Context[];
}

export interface ExchangeType {
  name: string;
  description: string;
  enabled: boolean;
}

export interface MessageStats {
  ack?: number;
  ack_details?: RateDetails;
  confirm?: number;
  confirm_details?: RateDetails;
  deliver?: number;
  deliver_details?: RateDetails;
  deliver_get?: number;
  deliver_get_details?: RateDetails;
  deliver_no_ack?: number;
  deliver_no_ack_details?: RateDetails;
  disk_reads?: number;
  disk_reads_details?: RateDetails;
  disk_writes?: number;
  disk_writes_details?: RateDetails;
  drop_unroutable?: number;
  drop_unroutable_details?: RateDetails;
  get?: number;
  get_details?: RateDetails;
  get_empty?: number;
  get_empty_details?: RateDetails;
  get_no_ack?: number;
  get_no_ack_details?: RateDetails;
  publish?: number;
  publish_details?: RateDetails;
  redeliver?: number;
  redeliver_details?: RateDetails;
  return_unroutable?: number;
  return_unroutable_details?: RateDetails;
}

export interface RateDetails {
  rate: number;
  samples?: Array<{ sample: number; timestamp: number }>;
  avg_rate?: number;
  avg?: number;
}

export interface ChurnRates {
  channel_closed: number;
  channel_closed_details: RateDetails;
  channel_created: number;
  channel_created_details: RateDetails;
  connection_closed: number;
  connection_closed_details: RateDetails;
  connection_created: number;
  connection_created_details: RateDetails;
  queue_created: number;
  queue_created_details: RateDetails;
  queue_declared: number;
  queue_declared_details: RateDetails;
  queue_deleted: number;
  queue_deleted_details: RateDetails;
}

export interface QueueTotals {
  messages: number;
  messages_details: RateDetails;
  messages_ready: number;
  messages_ready_details: RateDetails;
  messages_unacknowledged: number;
  messages_unacknowledged_details: RateDetails;
}

export interface ObjectTotals {
  channels: number;
  connections: number;
  consumers: number;
  exchanges: number;
  queues: number;
}

export interface Listener {
  node: string;
  protocol: string;
  ip_address: string;
  port: number;
  socket_opts?: {
    backlog?: number;
    nodelay?: boolean;
    linger?: [boolean, number];
    exit_on_close?: boolean;
  };
}

export interface Context {
  ssl_opts: string[];
  node: string;
  description: string;
  path: string;
  cowboy_opts: string;
  ip: string;
  port: string;
}

// Nodes
export interface Node {
  name: string;
  type: string;
  running: boolean;
  os_pid: string;
  mem_limit: number;
  mem_alarm: boolean;
  mem_used: number;
  mem_used_details?: RateDetails;
  disk_free_limit: number;
  disk_free_alarm: boolean;
  disk_free: number;
  disk_free_details?: RateDetails;
  fd_total: number;
  fd_used: number;
  fd_used_details?: RateDetails;
  sockets_total: number;
  sockets_used: number;
  sockets_used_details?: RateDetails;
  proc_total: number;
  proc_used: number;
  proc_used_details?: RateDetails;
  run_queue: number;
  processors: number;
  uptime: number;
  rates_mode: string;
  exchange_types: ExchangeType[];
  auth_mechanisms: AuthMechanism[];
  applications: Application[];
  contexts: Context[];
  log_files: string[];
  db_dir: string;
  config_files: string[];
  net_ticktime: number;
  enabled_plugins: string[];
  mem_calculation_strategy: string;
  ra_open_file_metrics?: {
    ra_log_wal: number;
    ra_log_segment_writer: number;
  };
  metrics_gc_queue_length?: {
    channel_closed: number;
    channel_consumer_deleted: number;
    connection_closed: number;
    consumer_deleted: number;
    exchange_deleted: number;
    node_node_deleted: number;
    queue_deleted: number;
    vhost_deleted: number;
  };
  cluster_links?: ClusterLink[];
}

export interface AuthMechanism {
  name: string;
  description: string;
  enabled: boolean;
}

export interface Application {
  name: string;
  description: string;
  version: string;
}

export interface ClusterLink {
  name: string;
  recv_bytes: number;
  recv_bytes_details?: RateDetails;
  send_bytes: number;
  send_bytes_details?: RateDetails;
  stats?: {
    recv_oct: number;
    recv_oct_details?: RateDetails;
    send_oct: number;
    send_oct_details?: RateDetails;
  };
}

// Queues
export interface Queue {
  name: string;
  vhost: string;
  type: 'classic' | 'quorum' | 'stream';
  durable: boolean;
  auto_delete: boolean;
  exclusive: boolean;
  arguments: Record<string, unknown>;
  node?: string;
  state?: string;
  policy?: string;
  exclusive_consumer_tag?: string;
  effective_policy_definition?: Record<string, unknown>;
  operator_policy?: string;
  consumer_capacity?: number;
  consumer_utilisation?: number;
  consumers?: number;
  memory?: number;
  messages?: number;
  messages_details?: RateDetails;
  messages_ready?: number;
  messages_ready_details?: RateDetails;
  messages_unacknowledged?: number;
  messages_unacknowledged_details?: RateDetails;
  message_bytes?: number;
  message_bytes_ready?: number;
  message_bytes_unacknowledged?: number;
  message_bytes_ram?: number;
  message_bytes_persistent?: number;
  message_bytes_paged_out?: number;
  head_message_timestamp?: number;
  disk_reads?: number;
  disk_writes?: number;
  backing_queue_status?: {
    mode: string;
    q1: number;
    q2: number;
    delta: [string, number, number, number];
    q3: number;
    q4: number;
    len: number;
    target_ram_count: string | number;
    next_seq_id: number;
    avg_ingress_rate: number;
    avg_egress_rate: number;
    avg_ack_ingress_rate: number;
    avg_ack_egress_rate: number;
    mirror_seen: number;
    mirror_senders: number;
  };
  message_stats?: MessageStats;
  reductions?: number;
  reductions_details?: RateDetails;
  garbage_collection?: {
    fullsweep_after: number;
    max_heap_size: number;
    min_bin_vheap_size: number;
    min_heap_size: number;
    minor_gcs: number;
  };
  recoverable_slaves?: string[];
  slave_nodes?: string[];
  synchronised_slave_nodes?: string[];
  leader?: string;
  members?: string[];
  online?: string[];
}

export interface QueueCreateRequest {
  auto_delete?: boolean;
  durable?: boolean;
  arguments?: Record<string, unknown>;
  node?: string;
}

export interface QueueMessage {
  payload_bytes: number;
  redelivered: boolean;
  exchange: string;
  routing_key: string;
  message_count: number;
  properties: {
    delivery_mode?: number;
    headers?: Record<string, unknown>;
    content_type?: string;
    content_encoding?: string;
    priority?: number;
    correlation_id?: string;
    reply_to?: string;
    expiration?: string;
    message_id?: string;
    timestamp?: number;
    type?: string;
    user_id?: string;
    app_id?: string;
  };
  payload: string;
  payload_encoding: string;
}

// Exchanges
export interface Exchange {
  name: string;
  vhost: string;
  type: 'direct' | 'fanout' | 'topic' | 'headers' | 'x-consistent-hash' | 'x-modulus-hash' | 'x-random' | 'x-delayed-message';
  durable: boolean;
  auto_delete: boolean;
  internal: boolean;
  arguments: Record<string, unknown>;
  policy?: string;
  message_stats?: MessageStats;
  user_who_performed_action?: string;
}

export interface ExchangeCreateRequest {
  type: string;
  auto_delete?: boolean;
  durable?: boolean;
  internal?: boolean;
  arguments?: Record<string, unknown>;
}

// Bindings
export interface Binding {
  source: string;
  vhost: string;
  destination: string;
  destination_type: 'queue' | 'exchange';
  routing_key: string;
  arguments: Record<string, unknown>;
  properties_key: string;
}

export interface BindingCreateRequest {
  routing_key?: string;
  arguments?: Record<string, unknown>;
}

// Connections
export interface Connection {
  name: string;
  node: string;
  channels: number;
  state: string;
  type: string;
  protocol: string;
  auth_mechanism: string;
  peer_cert_subject?: string;
  peer_cert_issuer?: string;
  peer_cert_validity?: string;
  ssl: boolean;
  ssl_protocol?: string;
  ssl_key_exchange?: string;
  ssl_cipher?: string;
  ssl_hash?: string;
  peer_host: string;
  peer_port: number;
  host: string;
  port: number;
  user: string;
  vhost: string;
  timeout: number;
  frame_max: number;
  channel_max: number;
  client_properties: {
    capabilities?: Record<string, boolean>;
    product?: string;
    version?: string;
    platform?: string;
    copyright?: string;
    information?: string;
    connection_name?: string;
    [key: string]: unknown;
  };
  recv_oct: number;
  recv_oct_details?: RateDetails;
  send_oct: number;
  send_oct_details?: RateDetails;
  recv_cnt: number;
  send_cnt: number;
  send_pend: number;
  connected_at: number;
  garbage_collection?: {
    fullsweep_after: number;
    max_heap_size: number;
    min_bin_vheap_size: number;
    min_heap_size: number;
    minor_gcs: number;
  };
  reductions?: number;
  reductions_details?: RateDetails;
}

// Channels
export interface Channel {
  name: string;
  node: string;
  connection_details: {
    name: string;
    peer_host: string;
    peer_port: number;
  };
  number: number;
  user: string;
  vhost: string;
  transactional: boolean;
  confirm: boolean;
  consumer_count: number;
  messages_unacknowledged: number;
  messages_unconfirmed: number;
  messages_uncommitted: number;
  acks_uncommitted: number;
  prefetch_count: number;
  global_prefetch_count: number;
  state: string;
  message_stats?: MessageStats;
  garbage_collection?: {
    fullsweep_after: number;
    max_heap_size: number;
    min_bin_vheap_size: number;
    min_heap_size: number;
    minor_gcs: number;
  };
  reductions?: number;
  reductions_details?: RateDetails;
  idle_since?: string;
}

// Virtual Hosts
export interface VHost {
  name: string;
  description?: string;
  tags?: string[];
  default_queue_type?: string;
  tracing?: boolean;
  cluster_state?: Record<string, string>;
  recv_oct?: number;
  recv_oct_details?: RateDetails;
  send_oct?: number;
  send_oct_details?: RateDetails;
  messages?: number;
  messages_details?: RateDetails;
  messages_ready?: number;
  messages_ready_details?: RateDetails;
  messages_unacknowledged?: number;
  messages_unacknowledged_details?: RateDetails;
  metadata?: {
    description?: string;
    tags?: string[];
  };
}

export interface VHostCreateRequest {
  description?: string;
  tags?: string[];
  default_queue_type?: string;
  tracing?: boolean;
}

// Users
export interface User {
  name: string;
  password_hash: string;
  hashing_algorithm: string;
  tags: string;
  limits?: Record<string, number>;
}

export interface UserCreateRequest {
  password?: string;
  password_hash?: string;
  hashing_algorithm?: string;
  tags?: string;
}

// Permissions
export interface Permission {
  user: string;
  vhost: string;
  configure: string;
  write: string;
  read: string;
}

export interface PermissionCreateRequest {
  configure: string;
  write: string;
  read: string;
}

export interface TopicPermission {
  user: string;
  vhost: string;
  exchange: string;
  write: string;
  read: string;
}

export interface TopicPermissionCreateRequest {
  exchange: string;
  write: string;
  read: string;
}

// Policies
export interface Policy {
  name: string;
  vhost: string;
  pattern: string;
  'apply-to': 'queues' | 'exchanges' | 'all' | 'classic_queues' | 'quorum_queues' | 'streams';
  priority: number;
  definition: Record<string, unknown>;
}

export interface PolicyCreateRequest {
  pattern: string;
  definition: Record<string, unknown>;
  priority?: number;
  'apply-to'?: 'queues' | 'exchanges' | 'all' | 'classic_queues' | 'quorum_queues' | 'streams';
}

export interface OperatorPolicy {
  name: string;
  vhost: string;
  pattern: string;
  'apply-to': 'queues' | 'exchanges' | 'all' | 'classic_queues' | 'quorum_queues' | 'streams';
  priority: number;
  definition: Record<string, unknown>;
}

// Parameters (Federation, Shovel)
export interface Parameter {
  name: string;
  vhost: string;
  component: string;
  value: Record<string, unknown>;
}

export interface FederationUpstream {
  name: string;
  vhost: string;
  component: 'federation-upstream';
  value: {
    uri: string;
    prefetch_count?: number;
    reconnect_delay?: number;
    ack_mode?: 'on-confirm' | 'on-publish' | 'no-ack';
    trust_user_id?: boolean;
    exchange?: string;
    max_hops?: number;
    expires?: number;
    message_ttl?: number;
    queue?: string;
    consumer_tag?: string;
  };
}

export interface FederationUpstreamSet {
  name: string;
  vhost: string;
  component: 'federation-upstream-set';
  value: Array<{
    upstream: string;
    exchange?: string;
    queue?: string;
  }>;
}

export interface ShovelDefinition {
  name: string;
  vhost: string;
  component: 'shovel';
  value: {
    'src-uri': string;
    'src-protocol'?: 'amqp091' | 'amqp10';
    'src-queue'?: string;
    'src-exchange'?: string;
    'src-exchange-key'?: string;
    'src-prefetch-count'?: number;
    'src-delete-after'?: 'never' | 'queue-length' | number;
    'dest-uri': string;
    'dest-protocol'?: 'amqp091' | 'amqp10';
    'dest-queue'?: string;
    'dest-exchange'?: string;
    'dest-exchange-key'?: string;
    'dest-add-forward-headers'?: boolean;
    'dest-add-timestamp-header'?: boolean;
    'ack-mode'?: 'on-confirm' | 'on-publish' | 'no-ack';
    'reconnect-delay'?: number;
  };
}

export interface ShovelStatus {
  name: string;
  vhost: string;
  type: 'static' | 'dynamic';
  state: 'starting' | 'running' | 'terminated';
  timestamp?: string;
  reason?: string;
  src_uri?: string;
  src_protocol?: string;
  dest_uri?: string;
  dest_protocol?: string;
}

// Federation Status
export interface FederationLink {
  node: string;
  exchange?: string;
  queue?: string;
  upstream_exchange?: string;
  upstream_queue?: string;
  type: 'exchange' | 'queue';
  vhost: string;
  upstream: string;
  id: string;
  status: 'starting' | 'running' | 'shutdown';
  local_connection?: string;
  uri?: string;
  timestamp?: string;
  error?: string;
}

// Consumers
export interface Consumer {
  consumer_tag: string;
  exclusive: boolean;
  ack_required: boolean;
  prefetch_count: number;
  active: boolean;
  activity_status: string;
  arguments: Record<string, unknown>;
  channel_details: {
    name: string;
    number: number;
    connection_name: string;
    peer_host: string;
    peer_port: number;
    user: string;
  };
  queue: {
    name: string;
    vhost: string;
  };
}

// Definitions (Import/Export)
export interface Definitions {
  rabbit_version?: string;
  rabbitmq_version?: string;
  product_name?: string;
  product_version?: string;
  users?: User[];
  vhosts?: VHost[];
  permissions?: Permission[];
  topic_permissions?: TopicPermission[];
  parameters?: Parameter[];
  global_parameters?: GlobalParameter[];
  policies?: Policy[];
  queues?: Queue[];
  exchanges?: Exchange[];
  bindings?: Binding[];
}

export interface GlobalParameter {
  name: string;
  value: unknown;
}

// Feature Flags
export interface FeatureFlag {
  name: string;
  desc: string;
  doc_url?: string;
  stability: 'stable' | 'experimental' | 'required';
  state: 'enabled' | 'disabled' | 'state_changing';
  provided_by?: string;
}

// Health Checks
export interface HealthCheck {
  status: 'ok' | 'failed';
  reason?: string;
}

export interface AlarmsCheck {
  status: 'ok' | 'failed';
  reason?: string;
  alarms?: Array<{
    node: string;
    resource: string;
    type: string;
  }>;
}

// Streams
export interface Stream extends Queue {
  type: 'stream';
  leader?: string;
  members?: string[];
  online?: string[];
}

// Limits
export interface VHostLimits {
  vhost: string;
  value: {
    'max-connections'?: number;
    'max-queues'?: number;
  };
}

export interface UserLimits {
  user: string;
  value: {
    'max-connections'?: number;
    'max-channels'?: number;
  };
}

// Tracing
export interface Trace {
  name: string;
  vhost: string;
  format: 'text' | 'json';
  pattern?: string;
  tracer_connection_username?: string;
  max_payload_bytes?: number;
}

// API Response wrapper
export interface ApiError {
  error: string;
  reason: string;
}

// Connection config
export interface RabbitMQConfig {
  url: string;
  username: string;
  password: string;
}
