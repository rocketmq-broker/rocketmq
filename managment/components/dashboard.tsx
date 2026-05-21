'use client';

import { useOverview, useNodes, useConnectionStatus, useHealthCheck } from '@/lib/hooks';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { 
  Activity, 
  Inbox, 
  ArrowLeftRight, 
  PlugZap, 
  Layers, 
  Users,
  Server,
  AlertTriangle,
  CheckCircle2,
  TrendingUp,
  TrendingDown,
} from 'lucide-react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  AreaChart,
  Area,
} from 'recharts';
import { format } from 'date-fns';

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function formatNumber(num: number): string {
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
  return num.toString();
}

function formatRate(rate: number): string {
  return `${rate.toFixed(1)}/s`;
}

function StatCard({
  title,
  value,
  icon: Icon,
  rate,
  trend,
  loading,
}: {
  title: string;
  value: string | number;
  icon: React.ElementType;
  rate?: number;
  trend?: 'up' | 'down' | 'neutral';
  loading?: boolean;
}) {
  return (
    <Card>
      <CardContent className="p-6">
        <div className="flex items-center justify-between">
          <div className="space-y-1">
            <p className="text-sm text-muted-foreground">{title}</p>
            {loading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{value}</p>
            )}
            {rate !== undefined && !loading && (
              <div className="flex items-center gap-1 text-xs text-muted-foreground">
                {trend === 'up' && <TrendingUp className="h-3 w-3 text-green-500" />}
                {trend === 'down' && <TrendingDown className="h-3 w-3 text-red-500" />}
                <span>{formatRate(rate)}</span>
              </div>
            )}
          </div>
          <div className="rounded-full bg-primary/10 p-3">
            <Icon className="h-5 w-5 text-primary" />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function NodeCard({ node, loading }: { node?: { 
  name: string;
  running: boolean;
  mem_used: number;
  mem_limit: number;
  mem_alarm: boolean;
  disk_free: number;
  disk_free_limit: number;
  disk_free_alarm: boolean;
  fd_used: number;
  fd_total: number;
  sockets_used: number;
  sockets_total: number;
  uptime: number;
}; loading?: boolean }) {
  if (loading || !node) {
    return (
      <Card>
        <CardHeader className="pb-2">
          <Skeleton className="h-5 w-40" />
        </CardHeader>
        <CardContent className="space-y-4">
          <Skeleton className="h-4 w-full" />
          <Skeleton className="h-4 w-full" />
          <Skeleton className="h-4 w-full" />
        </CardContent>
      </Card>
    );
  }

  const memPercent = (node.mem_used / node.mem_limit) * 100;
  const diskPercent = ((node.disk_free_limit) / (node.disk_free + node.disk_free_limit)) * 100;
  const fdPercent = (node.fd_used / node.fd_total) * 100;
  const socketPercent = (node.sockets_used / node.sockets_total) * 100;

  const uptimeDays = Math.floor(node.uptime / (1000 * 60 * 60 * 24));
  const uptimeHours = Math.floor((node.uptime % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-medium truncate">
            {node.name}
          </CardTitle>
          <Badge variant={node.running ? 'default' : 'destructive'}>
            {node.running ? 'Running' : 'Stopped'}
          </Badge>
        </div>
        <p className="text-xs text-muted-foreground">
          Uptime: {uptimeDays}d {uptimeHours}h
        </p>
      </CardHeader>
      <CardContent className="space-y-3">
        <ResourceBar 
          label="Memory" 
          value={formatBytes(node.mem_used)} 
          max={formatBytes(node.mem_limit)} 
          percent={memPercent} 
          alarm={node.mem_alarm} 
        />
        <ResourceBar 
          label="Disk" 
          value={formatBytes(node.disk_free)} 
          max={`min: ${formatBytes(node.disk_free_limit)}`} 
          percent={100 - diskPercent} 
          alarm={node.disk_free_alarm}
          inverted 
        />
        <ResourceBar 
          label="File Descriptors" 
          value={node.fd_used.toString()} 
          max={node.fd_total.toString()} 
          percent={fdPercent} 
        />
        <ResourceBar 
          label="Sockets" 
          value={node.sockets_used.toString()} 
          max={node.sockets_total.toString()} 
          percent={socketPercent} 
        />
      </CardContent>
    </Card>
  );
}

function ResourceBar({
  label,
  value,
  max,
  percent,
  alarm,
  inverted,
}: {
  label: string;
  value: string;
  max: string;
  percent: number;
  alarm?: boolean;
  inverted?: boolean;
}) {
  const getColor = () => {
    if (alarm) return 'bg-destructive';
    if (inverted) {
      if (percent > 80) return 'bg-green-500';
      if (percent > 50) return 'bg-yellow-500';
      return 'bg-red-500';
    }
    if (percent > 80) return 'bg-destructive';
    if (percent > 60) return 'bg-yellow-500';
    return 'bg-primary';
  };

  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-xs">
        <span className="text-muted-foreground">{label}</span>
        <span>
          {value} / {max}
        </span>
      </div>
      <div className="h-1.5 w-full bg-muted rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all ${getColor()}`}
          style={{ width: `${Math.min(percent, 100)}%` }}
        />
      </div>
    </div>
  );
}

function MessageRatesChart({ overview }: { overview?: {
  message_stats?: {
    publish_details?: { rate: number; samples?: Array<{ sample: number; timestamp: number }> };
    deliver_get_details?: { rate: number; samples?: Array<{ sample: number; timestamp: number }> };
    ack_details?: { rate: number; samples?: Array<{ sample: number; timestamp: number }> };
  };
}}) {
  if (!overview?.message_stats) {
    return (
      <Card className="col-span-2">
        <CardHeader>
          <CardTitle className="text-sm font-medium">Message Rates</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-64 flex items-center justify-center text-muted-foreground">
            No message activity
          </div>
        </CardContent>
      </Card>
    );
  }

  const publishSamples = overview.message_stats.publish_details?.samples || [];
  const deliverSamples = overview.message_stats.deliver_get_details?.samples || [];
  const ackSamples = overview.message_stats.ack_details?.samples || [];

  // Combine samples into chart data
  const timestamps = new Set<number>();
  publishSamples.forEach(s => timestamps.add(s.timestamp));
  deliverSamples.forEach(s => timestamps.add(s.timestamp));
  ackSamples.forEach(s => timestamps.add(s.timestamp));

  const sortedTimestamps = Array.from(timestamps).sort();
  const chartData = sortedTimestamps.map(timestamp => ({
    timestamp,
    time: format(new Date(timestamp), 'HH:mm:ss'),
    publish: publishSamples.find(s => s.timestamp === timestamp)?.sample || 0,
    deliver: deliverSamples.find(s => s.timestamp === timestamp)?.sample || 0,
    ack: ackSamples.find(s => s.timestamp === timestamp)?.sample || 0,
  }));

  return (
    <Card className="col-span-2">
      <CardHeader>
        <CardTitle className="text-sm font-medium">Message Rates</CardTitle>
        <div className="flex gap-4 text-xs">
          <div className="flex items-center gap-1">
            <div className="h-2 w-2 rounded-full bg-chart-1" />
            <span className="text-muted-foreground">
              Publish: {formatRate(overview.message_stats.publish_details?.rate || 0)}
            </span>
          </div>
          <div className="flex items-center gap-1">
            <div className="h-2 w-2 rounded-full bg-chart-2" />
            <span className="text-muted-foreground">
              Deliver: {formatRate(overview.message_stats.deliver_get_details?.rate || 0)}
            </span>
          </div>
          <div className="flex items-center gap-1">
            <div className="h-2 w-2 rounded-full bg-chart-3" />
            <span className="text-muted-foreground">
              Ack: {formatRate(overview.message_stats.ack_details?.rate || 0)}
            </span>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="publishGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="var(--chart-1)" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="var(--chart-1)" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="deliverGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="var(--chart-2)" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="var(--chart-2)" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
              <XAxis 
                dataKey="time" 
                tick={{ fontSize: 10 }} 
                className="text-muted-foreground"
                tickLine={false}
              />
              <YAxis 
                tick={{ fontSize: 10 }} 
                className="text-muted-foreground"
                tickLine={false}
                axisLine={false}
              />
              <Tooltip 
                contentStyle={{ 
                  backgroundColor: 'var(--popover)',
                  border: '1px solid var(--border)',
                  borderRadius: '8px',
                }}
                labelStyle={{ color: 'var(--foreground)' }}
              />
              <Area
                type="monotone"
                dataKey="publish"
                stroke="var(--chart-1)"
                fill="url(#publishGradient)"
                strokeWidth={2}
              />
              <Area
                type="monotone"
                dataKey="deliver"
                stroke="var(--chart-2)"
                fill="url(#deliverGradient)"
                strokeWidth={2}
              />
              <Line
                type="monotone"
                dataKey="ack"
                stroke="var(--chart-3)"
                strokeWidth={2}
                dot={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}

function QueueTotalsChart({ overview }: { overview?: {
  queue_totals?: {
    messages: number;
    messages_details?: { rate: number; samples?: Array<{ sample: number; timestamp: number }> };
    messages_ready: number;
    messages_ready_details?: { rate: number; samples?: Array<{ sample: number; timestamp: number }> };
    messages_unacknowledged: number;
    messages_unacknowledged_details?: { rate: number; samples?: Array<{ sample: number; timestamp: number }> };
  };
}}) {
  if (!overview?.queue_totals) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium">Queued Messages</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-48 flex items-center justify-center text-muted-foreground">
            No queue data
          </div>
        </CardContent>
      </Card>
    );
  }

  const { messages, messages_ready, messages_unacknowledged } = overview.queue_totals;
  const readySamples = overview.queue_totals.messages_ready_details?.samples || [];
  const unackSamples = overview.queue_totals.messages_unacknowledged_details?.samples || [];

  const timestamps = new Set<number>();
  readySamples.forEach(s => timestamps.add(s.timestamp));
  unackSamples.forEach(s => timestamps.add(s.timestamp));

  const sortedTimestamps = Array.from(timestamps).sort();
  const chartData = sortedTimestamps.map(timestamp => ({
    timestamp,
    time: format(new Date(timestamp), 'HH:mm:ss'),
    ready: readySamples.find(s => s.timestamp === timestamp)?.sample || 0,
    unacked: unackSamples.find(s => s.timestamp === timestamp)?.sample || 0,
  }));

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm font-medium">Queued Messages</CardTitle>
        <div className="flex gap-4 text-xs">
          <span className="text-muted-foreground">Total: {formatNumber(messages)}</span>
          <span className="text-muted-foreground">Ready: {formatNumber(messages_ready)}</span>
          <span className="text-muted-foreground">Unacked: {formatNumber(messages_unacknowledged)}</span>
        </div>
      </CardHeader>
      <CardContent>
        <div className="h-48">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
              <XAxis 
                dataKey="time" 
                tick={{ fontSize: 10 }} 
                className="text-muted-foreground"
                tickLine={false}
              />
              <YAxis 
                tick={{ fontSize: 10 }} 
                className="text-muted-foreground"
                tickLine={false}
                axisLine={false}
              />
              <Tooltip 
                contentStyle={{ 
                  backgroundColor: 'var(--popover)',
                  border: '1px solid var(--border)',
                  borderRadius: '8px',
                }}
              />
              <Line
                type="monotone"
                dataKey="ready"
                stroke="var(--chart-4)"
                strokeWidth={2}
                dot={false}
              />
              <Line
                type="monotone"
                dataKey="unacked"
                stroke="var(--chart-5)"
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}

export function Dashboard() {
  const { data: overview, isLoading: overviewLoading } = useOverview();
  const { data: nodes, isLoading: nodesLoading } = useNodes();
  const { data: health } = useHealthCheck();
  const { data: connectionStatus } = useConnectionStatus();

  const isConnected = connectionStatus?.connected;

  if (!isConnected) {
    return (
      <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
        <Server className="h-16 w-16 text-muted-foreground" />
        <h2 className="text-xl font-semibold">Not Connected</h2>
        <p className="text-muted-foreground text-center max-w-md">
          Connect to a RabbitMQ server to view the dashboard. Click the Settings link in the sidebar to configure your connection.
        </p>
      </div>
    );
  }

  const objectTotals = overview?.object_totals;
  const messageStats = overview?.message_stats;

  return (
    <div className="space-y-6">
      {/* Health Status */}
      {health && (
        <div className={`flex items-center gap-2 p-3 rounded-lg ${
          health.status === 'ok' ? 'bg-green-500/10' : 'bg-destructive/10'
        }`}>
          {health.status === 'ok' ? (
            <CheckCircle2 className="h-5 w-5 text-green-500" />
          ) : (
            <AlertTriangle className="h-5 w-5 text-destructive" />
          )}
          <span className={health.status === 'ok' ? 'text-green-500' : 'text-destructive'}>
            {health.status === 'ok' ? 'All systems operational' : health.reason || 'System alert'}
          </span>
        </div>
      )}

      {/* Stats Grid */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
        <StatCard
          title="Queues"
          value={objectTotals?.queues ?? 0}
          icon={Inbox}
          loading={overviewLoading}
        />
        <StatCard
          title="Exchanges"
          value={objectTotals?.exchanges ?? 0}
          icon={ArrowLeftRight}
          loading={overviewLoading}
        />
        <StatCard
          title="Connections"
          value={objectTotals?.connections ?? 0}
          icon={PlugZap}
          loading={overviewLoading}
        />
        <StatCard
          title="Channels"
          value={objectTotals?.channels ?? 0}
          icon={Layers}
          loading={overviewLoading}
        />
        <StatCard
          title="Consumers"
          value={objectTotals?.consumers ?? 0}
          icon={Users}
          loading={overviewLoading}
        />
        <StatCard
          title="Publish Rate"
          value={formatRate(messageStats?.publish_details?.rate ?? 0)}
          icon={Activity}
          rate={messageStats?.deliver_get_details?.rate}
          trend={messageStats?.publish_details?.rate && messageStats.publish_details.rate > 0 ? 'up' : 'neutral'}
          loading={overviewLoading}
        />
      </div>

      {/* Charts */}
      <div className="grid gap-4 lg:grid-cols-3">
        <MessageRatesChart overview={overview} />
        <QueueTotalsChart overview={overview} />
      </div>

      {/* Nodes */}
      <div>
        <h3 className="text-lg font-semibold mb-4">Cluster Nodes</h3>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {nodesLoading ? (
            <>
              <NodeCard loading />
              <NodeCard loading />
            </>
          ) : (
            nodes?.map((node) => (
              <NodeCard key={node.name} node={node} />
            ))
          )}
        </div>
      </div>

      {/* Cluster Info */}
      {overview && (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium">Cluster Information</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4 text-sm">
              <div>
                <p className="text-muted-foreground">Cluster Name</p>
                <p className="font-medium">{overview.cluster_name}</p>
              </div>
              <div>
                <p className="text-muted-foreground">RabbitMQ Version</p>
                <p className="font-medium">{overview.rabbitmq_version}</p>
              </div>
              <div>
                <p className="text-muted-foreground">Erlang Version</p>
                <p className="font-medium">{overview.erlang_version}</p>
              </div>
              <div>
                <p className="text-muted-foreground">Management Version</p>
                <p className="font-medium">{overview.management_version}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
