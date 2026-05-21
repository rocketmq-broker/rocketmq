// SWR hooks for data fetching with auto-refresh

import useSWR from 'swr';
import { useUIStore } from './store';
import type {
  Overview,
  Node,
  Queue,
  Exchange,
  Binding,
  Connection,
  Channel,
  VHost,
  User,
  Permission,
  TopicPermission,
  Policy,
  OperatorPolicy,
  Parameter,
  FederationLink,
  ShovelStatus,
  Consumer,
  FeatureFlag,
  VHostLimits,
  UserLimits,
} from '@/types/rabbitmq';

const fetcher = async (url: string) => {
  const res = await fetch(url);
  if (!res.ok) {
    const error = await res.json().catch(() => ({ reason: `HTTP ${res.status}` }));
    throw new Error(error.reason || error.error || 'Request failed');
  }
  return res.json();
};

// Helper to get SWR config with refresh interval
function useSWRConfig() {
  const refreshInterval = useUIStore((state) => state.refreshInterval);
  return {
    refreshInterval,
    revalidateOnFocus: true,
    dedupingInterval: 2000,
  };
}

// Overview & Cluster
export function useOverview() {
  const config = useSWRConfig();
  return useSWR<Overview>('/api/rabbitmq/overview', fetcher, config);
}

export function useClusterName() {
  const config = useSWRConfig();
  return useSWR<{ name: string }>('/api/rabbitmq/cluster-name', fetcher, config);
}

// Nodes
export function useNodes() {
  const config = useSWRConfig();
  return useSWR<Node[]>('/api/rabbitmq/nodes', fetcher, config);
}

export function useNode(name: string) {
  const config = useSWRConfig();
  return useSWR<Node>(
    name ? `/api/rabbitmq/nodes/${encodeURIComponent(name)}` : null,
    fetcher,
    config
  );
}

// Queues
export function useQueues(vhost?: string) {
  const config = useSWRConfig();
  const path = vhost
    ? `/api/rabbitmq/queues/${encodeURIComponent(vhost)}`
    : '/api/rabbitmq/queues';
  return useSWR<Queue[]>(path, fetcher, config);
}

export function useQueue(vhost: string, name: string) {
  const config = useSWRConfig();
  return useSWR<Queue>(
    vhost && name
      ? `/api/rabbitmq/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`
      : null,
    fetcher,
    config
  );
}

// Exchanges
export function useExchanges(vhost?: string) {
  const config = useSWRConfig();
  const path = vhost
    ? `/api/rabbitmq/exchanges/${encodeURIComponent(vhost)}`
    : '/api/rabbitmq/exchanges';
  return useSWR<Exchange[]>(path, fetcher, config);
}

export function useExchange(vhost: string, name: string) {
  const config = useSWRConfig();
  return useSWR<Exchange>(
    vhost && name
      ? `/api/rabbitmq/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`
      : null,
    fetcher,
    config
  );
}

// Bindings
export function useBindings(vhost?: string) {
  const config = useSWRConfig();
  const path = vhost
    ? `/api/rabbitmq/bindings/${encodeURIComponent(vhost)}`
    : '/api/rabbitmq/bindings';
  return useSWR<Binding[]>(path, fetcher, config);
}

export function useQueueBindings(vhost: string, queue: string) {
  const config = useSWRConfig();
  return useSWR<Binding[]>(
    vhost && queue
      ? `/api/rabbitmq/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(queue)}/bindings`
      : null,
    fetcher,
    config
  );
}

export function useExchangeBindingsSource(vhost: string, exchange: string) {
  const config = useSWRConfig();
  return useSWR<Binding[]>(
    vhost && exchange
      ? `/api/rabbitmq/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(exchange)}/bindings/source`
      : null,
    fetcher,
    config
  );
}

export function useExchangeBindingsDestination(vhost: string, exchange: string) {
  const config = useSWRConfig();
  return useSWR<Binding[]>(
    vhost && exchange
      ? `/api/rabbitmq/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(exchange)}/bindings/destination`
      : null,
    fetcher,
    config
  );
}

// Connections
export function useConnections() {
  const config = useSWRConfig();
  return useSWR<Connection[]>('/api/rabbitmq/connections', fetcher, config);
}

export function useConnection(name: string) {
  const config = useSWRConfig();
  return useSWR<Connection>(
    name ? `/api/rabbitmq/connections/${encodeURIComponent(name)}` : null,
    fetcher,
    config
  );
}

// Channels
export function useChannels() {
  const config = useSWRConfig();
  return useSWR<Channel[]>('/api/rabbitmq/channels', fetcher, config);
}

export function useChannel(name: string) {
  const config = useSWRConfig();
  return useSWR<Channel>(
    name ? `/api/rabbitmq/channels/${encodeURIComponent(name)}` : null,
    fetcher,
    config
  );
}

// Virtual Hosts
export function useVHosts() {
  const config = useSWRConfig();
  return useSWR<VHost[]>('/api/rabbitmq/vhosts', fetcher, config);
}

export function useVHost(name: string) {
  const config = useSWRConfig();
  return useSWR<VHost>(
    name ? `/api/rabbitmq/vhosts/${encodeURIComponent(name)}` : null,
    fetcher,
    config
  );
}

// Users
export function useUsers() {
  const config = useSWRConfig();
  return useSWR<User[]>('/api/rabbitmq/users', fetcher, config);
}

export function useUser(name: string) {
  const config = useSWRConfig();
  return useSWR<User>(
    name ? `/api/rabbitmq/users/${encodeURIComponent(name)}` : null,
    fetcher,
    config
  );
}

export function useUserPermissions(name: string) {
  const config = useSWRConfig();
  return useSWR<Permission[]>(
    name ? `/api/rabbitmq/users/${encodeURIComponent(name)}/permissions` : null,
    fetcher,
    config
  );
}

export function useUserTopicPermissions(name: string) {
  const config = useSWRConfig();
  return useSWR<TopicPermission[]>(
    name ? `/api/rabbitmq/users/${encodeURIComponent(name)}/topic-permissions` : null,
    fetcher,
    config
  );
}

// Permissions
export function usePermissions() {
  const config = useSWRConfig();
  return useSWR<Permission[]>('/api/rabbitmq/permissions', fetcher, config);
}

export function useVHostPermissions(vhost: string) {
  const config = useSWRConfig();
  return useSWR<Permission[]>(
    vhost ? `/api/rabbitmq/vhosts/${encodeURIComponent(vhost)}/permissions` : null,
    fetcher,
    config
  );
}

// Topic Permissions
export function useTopicPermissions() {
  const config = useSWRConfig();
  return useSWR<TopicPermission[]>('/api/rabbitmq/topic-permissions', fetcher, config);
}

// Policies
export function usePolicies(vhost?: string) {
  const config = useSWRConfig();
  const path = vhost
    ? `/api/rabbitmq/policies/${encodeURIComponent(vhost)}`
    : '/api/rabbitmq/policies';
  return useSWR<Policy[]>(path, fetcher, config);
}

export function usePolicy(vhost: string, name: string) {
  const config = useSWRConfig();
  return useSWR<Policy>(
    vhost && name
      ? `/api/rabbitmq/policies/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`
      : null,
    fetcher,
    config
  );
}

// Operator Policies
export function useOperatorPolicies(vhost?: string) {
  const config = useSWRConfig();
  const path = vhost
    ? `/api/rabbitmq/operator-policies/${encodeURIComponent(vhost)}`
    : '/api/rabbitmq/operator-policies';
  return useSWR<OperatorPolicy[]>(path, fetcher, config);
}

// Parameters
export function useParameters(component?: string) {
  const config = useSWRConfig();
  const path = component
    ? `/api/rabbitmq/parameters/${encodeURIComponent(component)}`
    : '/api/rabbitmq/parameters';
  return useSWR<Parameter[]>(path, fetcher, config);
}

// Federation
export function useFederationLinks(vhost?: string) {
  const config = useSWRConfig();
  const path = vhost
    ? `/api/rabbitmq/federation-links/${encodeURIComponent(vhost)}`
    : '/api/rabbitmq/federation-links';
  return useSWR<FederationLink[]>(path, fetcher, config);
}

export function useFederationUpstreams() {
  const config = useSWRConfig();
  return useSWR<Parameter[]>('/api/rabbitmq/parameters/federation-upstream', fetcher, config);
}

// Shovels
export function useShovels() {
  const config = useSWRConfig();
  return useSWR<Parameter[]>('/api/rabbitmq/parameters/shovel', fetcher, config);
}

export function useShovelStatus(vhost?: string) {
  const config = useSWRConfig();
  const path = vhost
    ? `/api/rabbitmq/shovels/${encodeURIComponent(vhost)}`
    : '/api/rabbitmq/shovels';
  return useSWR<ShovelStatus[]>(path, fetcher, config);
}

// Consumers
export function useConsumers(vhost?: string) {
  const config = useSWRConfig();
  const path = vhost
    ? `/api/rabbitmq/consumers/${encodeURIComponent(vhost)}`
    : '/api/rabbitmq/consumers';
  return useSWR<Consumer[]>(path, fetcher, config);
}

// Feature Flags
export function useFeatureFlags() {
  const config = useSWRConfig();
  return useSWR<FeatureFlag[]>('/api/rabbitmq/feature-flags', fetcher, config);
}

// Limits
export function useVHostLimits() {
  const config = useSWRConfig();
  return useSWR<VHostLimits[]>('/api/rabbitmq/vhost-limits', fetcher, config);
}

export function useUserLimits() {
  const config = useSWRConfig();
  return useSWR<UserLimits[]>('/api/rabbitmq/user-limits', fetcher, config);
}

// Definitions
export function useDefinitions() {
  return useSWR<{
    rabbit_version?: string;
    rabbitmq_version?: string;
  }>('/api/rabbitmq/definitions', fetcher, { revalidateOnFocus: false });
}

// Connection status
export function useConnectionStatus() {
  return useSWR<{ connected: boolean; url?: string; username?: string; stale?: boolean; error?: boolean }>(
    '/api/rabbitmq/connect',
    fetcher,
    { refreshInterval: 30000 }
  );
}

// Health checks
export function useHealthCheck() {
  const config = useSWRConfig();
  return useSWR<{ status: 'ok' | 'failed'; reason?: string }>(
    '/api/rabbitmq/health/checks/alarms',
    fetcher,
    config
  );
}
