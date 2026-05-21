// RabbitMQ HTTP API Client - All API endpoints

import type {
  Overview,
  Node,
  Queue,
  QueueCreateRequest,
  QueueMessage,
  Exchange,
  ExchangeCreateRequest,
  Binding,
  BindingCreateRequest,
  Connection,
  Channel,
  VHost,
  VHostCreateRequest,
  User,
  UserCreateRequest,
  Permission,
  PermissionCreateRequest,
  TopicPermission,
  TopicPermissionCreateRequest,
  Policy,
  PolicyCreateRequest,
  OperatorPolicy,
  Parameter,
  FederationUpstream,
  ShovelDefinition,
  ShovelStatus,
  FederationLink,
  Consumer,
  Definitions,
  GlobalParameter,
  FeatureFlag,
  HealthCheck,
  AlarmsCheck,
  VHostLimits,
  UserLimits,
  Trace,
} from '@/types/rabbitmq';

class RabbitMQClient {
  private baseUrl = '/api/rabbitmq';

  private async request<T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<T> {
    const url = `${this.baseUrl}${endpoint}`;
    
    const response = await fetch(url, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
    });

    if (!response.ok) {
      const errorText = await response.text();
      let errorMessage: string;
      try {
        const errorJson = JSON.parse(errorText);
        errorMessage = errorJson.reason || errorJson.error || errorText;
      } catch {
        errorMessage = errorText || `HTTP ${response.status}`;
      }
      throw new Error(errorMessage);
    }

    // Handle 204 No Content
    if (response.status === 204) {
      return undefined as T;
    }

    const text = await response.text();
    if (!text) {
      return undefined as T;
    }

    return JSON.parse(text);
  }

  // Overview & Cluster
  async getOverview(): Promise<Overview> {
    return this.request<Overview>('/overview');
  }

  async getClusterName(): Promise<{ name: string }> {
    return this.request<{ name: string }>('/cluster-name');
  }

  async setClusterName(name: string): Promise<void> {
    return this.request<void>('/cluster-name', {
      method: 'PUT',
      body: JSON.stringify({ name }),
    });
  }

  // Nodes
  async getNodes(): Promise<Node[]> {
    return this.request<Node[]>('/nodes');
  }

  async getNode(name: string): Promise<Node> {
    return this.request<Node>(`/nodes/${encodeURIComponent(name)}`);
  }

  // Queues
  async getQueues(vhost?: string): Promise<Queue[]> {
    const path = vhost
      ? `/queues/${encodeURIComponent(vhost)}`
      : '/queues';
    return this.request<Queue[]>(path);
  }

  async getQueue(vhost: string, name: string): Promise<Queue> {
    return this.request<Queue>(
      `/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`
    );
  }

  async createQueue(
    vhost: string,
    name: string,
    options: QueueCreateRequest = {}
  ): Promise<void> {
    return this.request<void>(
      `/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`,
      {
        method: 'PUT',
        body: JSON.stringify(options),
      }
    );
  }

  async deleteQueue(
    vhost: string,
    name: string,
    options?: { 'if-empty'?: boolean; 'if-unused'?: boolean }
  ): Promise<void> {
    const params = new URLSearchParams();
    if (options?.['if-empty']) params.set('if-empty', 'true');
    if (options?.['if-unused']) params.set('if-unused', 'true');
    const query = params.toString() ? `?${params.toString()}` : '';
    
    return this.request<void>(
      `/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}${query}`,
      { method: 'DELETE' }
    );
  }

  async purgeQueue(vhost: string, name: string): Promise<void> {
    return this.request<void>(
      `/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}/contents`,
      { method: 'DELETE' }
    );
  }

  async getMessages(
    vhost: string,
    queue: string,
    options: {
      count?: number;
      ackmode?: 'ack_requeue_true' | 'ack_requeue_false' | 'reject_requeue_true' | 'reject_requeue_false';
      encoding?: 'auto' | 'base64';
      truncate?: number;
    } = {}
  ): Promise<QueueMessage[]> {
    return this.request<QueueMessage[]>(
      `/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(queue)}/get`,
      {
        method: 'POST',
        body: JSON.stringify({
          count: options.count || 1,
          ackmode: options.ackmode || 'ack_requeue_true',
          encoding: options.encoding || 'auto',
          truncate: options.truncate,
        }),
      }
    );
  }

  async publishMessage(
    vhost: string,
    exchange: string,
    routingKey: string,
    payload: string,
    properties: Record<string, unknown> = {}
  ): Promise<{ routed: boolean }> {
    return this.request<{ routed: boolean }>(
      `/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(exchange)}/publish`,
      {
        method: 'POST',
        body: JSON.stringify({
          routing_key: routingKey,
          payload,
          payload_encoding: 'string',
          properties: {
            delivery_mode: 2,
            ...properties,
          },
        }),
      }
    );
  }

  // Queue Actions
  async syncQueue(vhost: string, name: string): Promise<void> {
    return this.request<void>(
      `/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}/actions`,
      {
        method: 'POST',
        body: JSON.stringify({ action: 'sync' }),
      }
    );
  }

  async cancelSyncQueue(vhost: string, name: string): Promise<void> {
    return this.request<void>(
      `/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}/actions`,
      {
        method: 'POST',
        body: JSON.stringify({ action: 'cancel_sync' }),
      }
    );
  }

  // Exchanges
  async getExchanges(vhost?: string): Promise<Exchange[]> {
    const path = vhost
      ? `/exchanges/${encodeURIComponent(vhost)}`
      : '/exchanges';
    return this.request<Exchange[]>(path);
  }

  async getExchange(vhost: string, name: string): Promise<Exchange> {
    return this.request<Exchange>(
      `/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`
    );
  }

  async createExchange(
    vhost: string,
    name: string,
    options: ExchangeCreateRequest
  ): Promise<void> {
    return this.request<void>(
      `/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`,
      {
        method: 'PUT',
        body: JSON.stringify(options),
      }
    );
  }

  async deleteExchange(
    vhost: string,
    name: string,
    options?: { 'if-unused'?: boolean }
  ): Promise<void> {
    const params = new URLSearchParams();
    if (options?.['if-unused']) params.set('if-unused', 'true');
    const query = params.toString() ? `?${params.toString()}` : '';
    
    return this.request<void>(
      `/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}${query}`,
      { method: 'DELETE' }
    );
  }

  // Bindings
  async getBindings(vhost?: string): Promise<Binding[]> {
    const path = vhost
      ? `/bindings/${encodeURIComponent(vhost)}`
      : '/bindings';
    return this.request<Binding[]>(path);
  }

  async getExchangeBindingsSource(vhost: string, exchange: string): Promise<Binding[]> {
    return this.request<Binding[]>(
      `/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(exchange)}/bindings/source`
    );
  }

  async getExchangeBindingsDestination(vhost: string, exchange: string): Promise<Binding[]> {
    return this.request<Binding[]>(
      `/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(exchange)}/bindings/destination`
    );
  }

  async getQueueBindings(vhost: string, queue: string): Promise<Binding[]> {
    return this.request<Binding[]>(
      `/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(queue)}/bindings`
    );
  }

  async createBinding(
    vhost: string,
    source: string,
    destination: string,
    destinationType: 'queue' | 'exchange',
    options: BindingCreateRequest = {}
  ): Promise<void> {
    const path =
      destinationType === 'queue'
        ? `/bindings/${encodeURIComponent(vhost)}/e/${encodeURIComponent(source)}/q/${encodeURIComponent(destination)}`
        : `/bindings/${encodeURIComponent(vhost)}/e/${encodeURIComponent(source)}/e/${encodeURIComponent(destination)}`;
    
    return this.request<void>(path, {
      method: 'POST',
      body: JSON.stringify(options),
    });
  }

  async deleteBinding(
    vhost: string,
    source: string,
    destination: string,
    destinationType: 'queue' | 'exchange',
    propertiesKey: string
  ): Promise<void> {
    const path =
      destinationType === 'queue'
        ? `/bindings/${encodeURIComponent(vhost)}/e/${encodeURIComponent(source)}/q/${encodeURIComponent(destination)}/${encodeURIComponent(propertiesKey)}`
        : `/bindings/${encodeURIComponent(vhost)}/e/${encodeURIComponent(source)}/e/${encodeURIComponent(destination)}/${encodeURIComponent(propertiesKey)}`;
    
    return this.request<void>(path, { method: 'DELETE' });
  }

  // Connections
  async getConnections(): Promise<Connection[]> {
    return this.request<Connection[]>('/connections');
  }

  async getConnection(name: string): Promise<Connection> {
    return this.request<Connection>(`/connections/${encodeURIComponent(name)}`);
  }

  async closeConnection(name: string, reason?: string): Promise<void> {
    return this.request<void>(`/connections/${encodeURIComponent(name)}`, {
      method: 'DELETE',
      headers: reason ? { 'X-Reason': reason } : undefined,
    });
  }

  async getConnectionChannels(name: string): Promise<Channel[]> {
    return this.request<Channel[]>(`/connections/${encodeURIComponent(name)}/channels`);
  }

  // Channels
  async getChannels(): Promise<Channel[]> {
    return this.request<Channel[]>('/channels');
  }

  async getChannel(name: string): Promise<Channel> {
    return this.request<Channel>(`/channels/${encodeURIComponent(name)}`);
  }

  // Virtual Hosts
  async getVHosts(): Promise<VHost[]> {
    return this.request<VHost[]>('/vhosts');
  }

  async getVHost(name: string): Promise<VHost> {
    return this.request<VHost>(`/vhosts/${encodeURIComponent(name)}`);
  }

  async createVHost(name: string, options: VHostCreateRequest = {}): Promise<void> {
    return this.request<void>(`/vhosts/${encodeURIComponent(name)}`, {
      method: 'PUT',
      body: JSON.stringify(options),
    });
  }

  async deleteVHost(name: string): Promise<void> {
    return this.request<void>(`/vhosts/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    });
  }

  async startVHost(vhost: string, node: string): Promise<void> {
    return this.request<void>(
      `/vhosts/${encodeURIComponent(vhost)}/start/${encodeURIComponent(node)}`,
      { method: 'POST' }
    );
  }

  // Users
  async getUsers(): Promise<User[]> {
    return this.request<User[]>('/users');
  }

  async getUser(name: string): Promise<User> {
    return this.request<User>(`/users/${encodeURIComponent(name)}`);
  }

  async createUser(name: string, options: UserCreateRequest): Promise<void> {
    return this.request<void>(`/users/${encodeURIComponent(name)}`, {
      method: 'PUT',
      body: JSON.stringify(options),
    });
  }

  async deleteUser(name: string): Promise<void> {
    return this.request<void>(`/users/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    });
  }

  async getUserPermissions(name: string): Promise<Permission[]> {
    return this.request<Permission[]>(`/users/${encodeURIComponent(name)}/permissions`);
  }

  async getUserTopicPermissions(name: string): Promise<TopicPermission[]> {
    return this.request<TopicPermission[]>(`/users/${encodeURIComponent(name)}/topic-permissions`);
  }

  async bulkDeleteUsers(users: string[]): Promise<void> {
    return this.request<void>('/users/bulk-delete', {
      method: 'POST',
      body: JSON.stringify({ users }),
    });
  }

  // Whoami
  async whoami(): Promise<{ name: string; tags: string }> {
    return this.request<{ name: string; tags: string }>('/whoami');
  }

  // Permissions
  async getPermissions(): Promise<Permission[]> {
    return this.request<Permission[]>('/permissions');
  }

  async getVHostPermissions(vhost: string): Promise<Permission[]> {
    return this.request<Permission[]>(`/vhosts/${encodeURIComponent(vhost)}/permissions`);
  }

  async getPermission(vhost: string, user: string): Promise<Permission> {
    return this.request<Permission>(
      `/permissions/${encodeURIComponent(vhost)}/${encodeURIComponent(user)}`
    );
  }

  async setPermission(
    vhost: string,
    user: string,
    options: PermissionCreateRequest
  ): Promise<void> {
    return this.request<void>(
      `/permissions/${encodeURIComponent(vhost)}/${encodeURIComponent(user)}`,
      {
        method: 'PUT',
        body: JSON.stringify(options),
      }
    );
  }

  async deletePermission(vhost: string, user: string): Promise<void> {
    return this.request<void>(
      `/permissions/${encodeURIComponent(vhost)}/${encodeURIComponent(user)}`,
      { method: 'DELETE' }
    );
  }

  // Topic Permissions
  async getTopicPermissions(): Promise<TopicPermission[]> {
    return this.request<TopicPermission[]>('/topic-permissions');
  }

  async getVHostTopicPermissions(vhost: string): Promise<TopicPermission[]> {
    return this.request<TopicPermission[]>(
      `/vhosts/${encodeURIComponent(vhost)}/topic-permissions`
    );
  }

  async setTopicPermission(
    vhost: string,
    user: string,
    options: TopicPermissionCreateRequest
  ): Promise<void> {
    return this.request<void>(
      `/topic-permissions/${encodeURIComponent(vhost)}/${encodeURIComponent(user)}`,
      {
        method: 'PUT',
        body: JSON.stringify(options),
      }
    );
  }

  async deleteTopicPermission(
    vhost: string,
    user: string,
    exchange?: string
  ): Promise<void> {
    const path = exchange
      ? `/topic-permissions/${encodeURIComponent(vhost)}/${encodeURIComponent(user)}/${encodeURIComponent(exchange)}`
      : `/topic-permissions/${encodeURIComponent(vhost)}/${encodeURIComponent(user)}`;
    return this.request<void>(path, { method: 'DELETE' });
  }

  // Policies
  async getPolicies(vhost?: string): Promise<Policy[]> {
    const path = vhost
      ? `/policies/${encodeURIComponent(vhost)}`
      : '/policies';
    return this.request<Policy[]>(path);
  }

  async getPolicy(vhost: string, name: string): Promise<Policy> {
    return this.request<Policy>(
      `/policies/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`
    );
  }

  async setPolicy(
    vhost: string,
    name: string,
    options: PolicyCreateRequest
  ): Promise<void> {
    return this.request<void>(
      `/policies/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`,
      {
        method: 'PUT',
        body: JSON.stringify(options),
      }
    );
  }

  async deletePolicy(vhost: string, name: string): Promise<void> {
    return this.request<void>(
      `/policies/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`,
      { method: 'DELETE' }
    );
  }

  // Operator Policies
  async getOperatorPolicies(vhost?: string): Promise<OperatorPolicy[]> {
    const path = vhost
      ? `/operator-policies/${encodeURIComponent(vhost)}`
      : '/operator-policies';
    return this.request<OperatorPolicy[]>(path);
  }

  async setOperatorPolicy(
    vhost: string,
    name: string,
    options: PolicyCreateRequest
  ): Promise<void> {
    return this.request<void>(
      `/operator-policies/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`,
      {
        method: 'PUT',
        body: JSON.stringify(options),
      }
    );
  }

  async deleteOperatorPolicy(vhost: string, name: string): Promise<void> {
    return this.request<void>(
      `/operator-policies/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`,
      { method: 'DELETE' }
    );
  }

  // Parameters (Generic)
  async getParameters(component?: string): Promise<Parameter[]> {
    const path = component ? `/parameters/${encodeURIComponent(component)}` : '/parameters';
    return this.request<Parameter[]>(path);
  }

  async getVHostParameters(component: string, vhost: string): Promise<Parameter[]> {
    return this.request<Parameter[]>(
      `/parameters/${encodeURIComponent(component)}/${encodeURIComponent(vhost)}`
    );
  }

  async getParameter(component: string, vhost: string, name: string): Promise<Parameter> {
    return this.request<Parameter>(
      `/parameters/${encodeURIComponent(component)}/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`
    );
  }

  async setParameter(
    component: string,
    vhost: string,
    name: string,
    value: Record<string, unknown>
  ): Promise<void> {
    return this.request<void>(
      `/parameters/${encodeURIComponent(component)}/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`,
      {
        method: 'PUT',
        body: JSON.stringify({ vhost, component, name, value }),
      }
    );
  }

  async deleteParameter(component: string, vhost: string, name: string): Promise<void> {
    return this.request<void>(
      `/parameters/${encodeURIComponent(component)}/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`,
      { method: 'DELETE' }
    );
  }

  // Global Parameters
  async getGlobalParameters(): Promise<GlobalParameter[]> {
    return this.request<GlobalParameter[]>('/global-parameters');
  }

  async getGlobalParameter(name: string): Promise<GlobalParameter> {
    return this.request<GlobalParameter>(`/global-parameters/${encodeURIComponent(name)}`);
  }

  async setGlobalParameter(name: string, value: unknown): Promise<void> {
    return this.request<void>(`/global-parameters/${encodeURIComponent(name)}`, {
      method: 'PUT',
      body: JSON.stringify({ name, value }),
    });
  }

  async deleteGlobalParameter(name: string): Promise<void> {
    return this.request<void>(`/global-parameters/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    });
  }

  // Federation
  async getFederationLinks(): Promise<FederationLink[]> {
    return this.request<FederationLink[]>('/federation-links');
  }

  async getVHostFederationLinks(vhost: string): Promise<FederationLink[]> {
    return this.request<FederationLink[]>(
      `/federation-links/${encodeURIComponent(vhost)}`
    );
  }

  async getFederationUpstreams(vhost?: string): Promise<FederationUpstream[]> {
    return this.getParameters('federation-upstream').then((params) =>
      (params as FederationUpstream[]).filter((p) => !vhost || p.vhost === vhost)
    );
  }

  async setFederationUpstream(
    vhost: string,
    name: string,
    value: FederationUpstream['value']
  ): Promise<void> {
    return this.setParameter('federation-upstream', vhost, name, value);
  }

  async deleteFederationUpstream(vhost: string, name: string): Promise<void> {
    return this.deleteParameter('federation-upstream', vhost, name);
  }

  // Shovels
  async getShovels(): Promise<ShovelDefinition[]> {
    return this.getParameters('shovel') as Promise<ShovelDefinition[]>;
  }

  async getShovelStatus(): Promise<ShovelStatus[]> {
    return this.request<ShovelStatus[]>('/shovels');
  }

  async getVHostShovelStatus(vhost: string): Promise<ShovelStatus[]> {
    return this.request<ShovelStatus[]>(`/shovels/${encodeURIComponent(vhost)}`);
  }

  async setShovel(
    vhost: string,
    name: string,
    value: ShovelDefinition['value']
  ): Promise<void> {
    return this.setParameter('shovel', vhost, name, value);
  }

  async deleteShovel(vhost: string, name: string): Promise<void> {
    return this.deleteParameter('shovel', vhost, name);
  }

  async restartShovel(vhost: string, name: string): Promise<void> {
    return this.request<void>(
      `/shovels/vhost/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}/restart`,
      { method: 'DELETE' }
    );
  }

  // Consumers
  async getConsumers(vhost?: string): Promise<Consumer[]> {
    const path = vhost
      ? `/consumers/${encodeURIComponent(vhost)}`
      : '/consumers';
    return this.request<Consumer[]>(path);
  }

  // Definitions (Import/Export)
  async getDefinitions(): Promise<Definitions> {
    return this.request<Definitions>('/definitions');
  }

  async getVHostDefinitions(vhost: string): Promise<Definitions> {
    return this.request<Definitions>(`/definitions/${encodeURIComponent(vhost)}`);
  }

  async uploadDefinitions(definitions: Definitions): Promise<void> {
    return this.request<void>('/definitions', {
      method: 'POST',
      body: JSON.stringify(definitions),
    });
  }

  async uploadVHostDefinitions(vhost: string, definitions: Definitions): Promise<void> {
    return this.request<void>(`/definitions/${encodeURIComponent(vhost)}`, {
      method: 'POST',
      body: JSON.stringify(definitions),
    });
  }

  // Feature Flags
  async getFeatureFlags(): Promise<FeatureFlag[]> {
    return this.request<FeatureFlag[]>('/feature-flags');
  }

  async enableFeatureFlag(name: string): Promise<void> {
    return this.request<void>(`/feature-flags/${encodeURIComponent(name)}/enable`, {
      method: 'PUT',
    });
  }

  // Health Checks
  async healthCheck(): Promise<HealthCheck> {
    return this.request<HealthCheck>('/health/checks/alarms');
  }

  async healthCheckAlarms(): Promise<AlarmsCheck> {
    return this.request<AlarmsCheck>('/health/checks/alarms');
  }

  async healthCheckLocalAlarms(): Promise<AlarmsCheck> {
    return this.request<AlarmsCheck>('/health/checks/local-alarms');
  }

  async healthCheckCertificateExpiration(within: number, unit: 'days' | 'weeks' | 'months' | 'years'): Promise<HealthCheck> {
    return this.request<HealthCheck>(
      `/health/checks/certificate-expiration/${within}/${unit}`
    );
  }

  async healthCheckPortListener(port: number): Promise<HealthCheck> {
    return this.request<HealthCheck>(`/health/checks/port-listener/${port}`);
  }

  async healthCheckProtocolListener(protocol: string): Promise<HealthCheck> {
    return this.request<HealthCheck>(
      `/health/checks/protocol-listener/${encodeURIComponent(protocol)}`
    );
  }

  async healthCheckVirtualHosts(): Promise<HealthCheck> {
    return this.request<HealthCheck>('/health/checks/virtual-hosts');
  }

  async healthCheckNodeIsMirrorSyncCritical(): Promise<HealthCheck> {
    return this.request<HealthCheck>('/health/checks/node-is-mirror-sync-critical');
  }

  async healthCheckNodeIsQuorumCritical(): Promise<HealthCheck> {
    return this.request<HealthCheck>('/health/checks/node-is-quorum-critical');
  }

  // Limits
  async getVHostLimits(): Promise<VHostLimits[]> {
    return this.request<VHostLimits[]>('/vhost-limits');
  }

  async getSpecificVHostLimits(vhost: string): Promise<VHostLimits> {
    return this.request<VHostLimits>(`/vhost-limits/${encodeURIComponent(vhost)}`);
  }

  async setVHostLimit(
    vhost: string,
    limitName: 'max-connections' | 'max-queues',
    value: number
  ): Promise<void> {
    return this.request<void>(
      `/vhost-limits/${encodeURIComponent(vhost)}/${limitName}`,
      {
        method: 'PUT',
        body: JSON.stringify({ value }),
      }
    );
  }

  async deleteVHostLimit(vhost: string, limitName?: 'max-connections' | 'max-queues'): Promise<void> {
    const path = limitName
      ? `/vhost-limits/${encodeURIComponent(vhost)}/${limitName}`
      : `/vhost-limits/${encodeURIComponent(vhost)}`;
    return this.request<void>(path, { method: 'DELETE' });
  }

  async getUserLimits(): Promise<UserLimits[]> {
    return this.request<UserLimits[]>('/user-limits');
  }

  async getSpecificUserLimits(user: string): Promise<UserLimits> {
    return this.request<UserLimits>(`/user-limits/${encodeURIComponent(user)}`);
  }

  async setUserLimit(
    user: string,
    limitName: 'max-connections' | 'max-channels',
    value: number
  ): Promise<void> {
    return this.request<void>(
      `/user-limits/${encodeURIComponent(user)}/${limitName}`,
      {
        method: 'PUT',
        body: JSON.stringify({ value }),
      }
    );
  }

  async deleteUserLimit(user: string, limitName?: 'max-connections' | 'max-channels'): Promise<void> {
    const path = limitName
      ? `/user-limits/${encodeURIComponent(user)}/${limitName}`
      : `/user-limits/${encodeURIComponent(user)}`;
    return this.request<void>(path, { method: 'DELETE' });
  }

  // Tracing
  async getTraces(): Promise<Trace[]> {
    return this.getParameters('trace') as unknown as Promise<Trace[]>;
  }

  async getVHostTraces(vhost: string): Promise<Trace[]> {
    return this.getVHostParameters('trace', vhost) as unknown as Promise<Trace[]>;
  }

  async setTrace(vhost: string, name: string, options: Omit<Trace, 'name' | 'vhost'>): Promise<void> {
    return this.setParameter('trace', vhost, name, options);
  }

  async deleteTrace(vhost: string, name: string): Promise<void> {
    return this.deleteParameter('trace', vhost, name);
  }

  // Extensions
  async getExtensions(): Promise<Array<{ javascript: string }>> {
    return this.request<Array<{ javascript: string }>>('/extensions');
  }

  // Auth Attempts
  async getAuthAttempts(node: string): Promise<Array<{ remote_address: string; username: string; protocol: string; auth_mechanism: string; succeeded: boolean; timestamp: string }>> {
    return this.request(`/auth/attempts/${encodeURIComponent(node)}`);
  }

  async getAuthAttemptsSource(node: string): Promise<Array<{ remote_address: string; succeeded: boolean; failed: boolean }>> {
    return this.request(`/auth/attempts/${encodeURIComponent(node)}/source`);
  }

  // Rebalance
  async rebalanceQueues(): Promise<void> {
    return this.request<void>('/rebalance/queues', { method: 'POST' });
  }
}

export const rabbitmqClient = new RabbitMQClient();
