'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useConnections, useConnectionStatus } from '@/lib/hooks';
import { rabbitmqClient } from '@/lib/rabbitmq-client';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { toast } from 'sonner';
import { mutate } from 'swr';
import {
  Search,
  MoreHorizontal,
  Trash2,
  RefreshCw,
  Server,
  PlugZap,
  Activity,
  ArrowDownLeft,
  ArrowUpRight,
} from 'lucide-react';
import type { Connection } from '@/types/rabbitmq';

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function ConnectionRow({ connection, onClose }: { connection: Connection; onClose: () => void }) {
  return (
    <TableRow>
      <TableCell className="font-mono text-sm">
        <div className="flex flex-col">
          <span className="font-medium text-foreground">{connection.name}</span>
          <span className="text-xs text-muted-foreground">
            {connection.peer_host}:{connection.peer_port} &rarr; {connection.host}:{connection.port}
          </span>
        </div>
      </TableCell>
      <TableCell>
        <Badge
          variant={connection.state === 'running' ? 'default' : 'secondary'}
          className={connection.state === 'running' ? 'bg-green-500/10 text-green-500 border-green-500/30' : ''}
        >
          {connection.state || 'unknown'}
        </Badge>
      </TableCell>
      <TableCell className="text-center font-mono">
        {connection.channels}
      </TableCell>
      <TableCell>
        <div className="flex flex-col">
          <span className="text-sm font-medium">{connection.client_properties?.product || 'AMQP Client'}</span>
          <span className="text-xs text-muted-foreground">{connection.client_properties?.version || '-'}</span>
        </div>
      </TableCell>
      <TableCell className="font-mono text-xs">
        {connection.protocol}
      </TableCell>
      <TableCell className="text-right font-mono text-xs">
        <div className="flex justify-end items-center gap-1">
          <ArrowDownLeft className="h-3 w-3 text-green-500" />
          {formatBytes(connection.recv_oct)}
        </div>
        <div className="flex justify-end items-center gap-1 mt-0.5">
          <ArrowUpRight className="h-3 w-3 text-blue-500" />
          {formatBytes(connection.send_oct)}
        </div>
      </TableCell>
      <TableCell className="font-mono text-sm">
        {connection.user}
      </TableCell>
      <TableCell>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" size="icon">
              <MoreHorizontal className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem
              onClick={onClose}
              className="text-destructive focus:text-destructive"
            >
              <Trash2 className="mr-2 h-4 w-4" />
              Force Close
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </TableCell>
    </TableRow>
  );
}

export default function ConnectionsPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const { data: connections, isLoading, mutate: mutateConnections } = useConnections();
  const [search, setSearch] = useState('');

  const isConnected = connectionStatus?.connected;

  const handleClose = async (connection: Connection) => {
    if (!confirm(`Are you sure you want to force close connection "${connection.name}"?`)) {
      return;
    }

    try {
      await rabbitmqClient.closeConnection(connection.name, 'Force closed via management UI');
      toast.success(`Connection "${connection.name}" closed`);
      mutateConnections();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to close connection');
    }
  };

  const filtered = connections?.filter((c) =>
    c.name.toLowerCase().includes(search.toLowerCase()) ||
    c.user.toLowerCase().includes(search.toLowerCase())
  );

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to manage connections.
          </p>
        </div>
      </AppShell>
    );
  }

  return (
    <AppShell>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">Connections</h1>
            <p className="text-muted-foreground">
              Monitor active client connections to the broker
            </p>
          </div>
          <Button variant="outline" size="icon" onClick={() => mutateConnections()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Stats */}
        <div className="grid gap-4 md:grid-cols-3">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-primary/10 p-2">
                  <PlugZap className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{connections?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Total Connections</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-green-500/10 p-2">
                  <Activity className="h-4 w-4 text-green-500" />
                </div>
                <div>
                  <p className="text-2xl font-bold">
                    {connections?.filter(c => c.state === 'running').length || 0}
                  </p>
                  <p className="text-xs text-muted-foreground">Active</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-blue-500/10 p-2">
                  <PlugZap className="h-4 w-4 text-blue-500" />
                </div>
                <div>
                  <p className="text-2xl font-bold">
                    {connections?.reduce((sum, c) => sum + c.channels, 0) || 0}
                  </p>
                  <p className="text-xs text-muted-foreground">Total Channels</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Filters */}
        <div className="flex items-center gap-4">
          <div className="relative flex-1 max-w-sm">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="Search connections..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-9"
            />
          </div>
        </div>

        {/* Table */}
        <Card>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name / Address</TableHead>
                  <TableHead>State</TableHead>
                  <TableHead className="text-center">Channels</TableHead>
                  <TableHead>Client Product</TableHead>
                  <TableHead>Protocol</TableHead>
                  <TableHead className="text-right">Octets (Recv/Sent)</TableHead>
                  <TableHead>User</TableHead>
                  <TableHead className="w-12"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  Array.from({ length: 3 }).map((_, i) => (
                    <TableRow key={i}>
                      {Array.from({ length: 8 }).map((_, j) => (
                        <TableCell key={j}>
                          <Skeleton className="h-4 w-full" />
                        </TableCell>
                      ))}
                    </TableRow>
                  ))
                ) : !filtered || filtered.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={8} className="text-center py-8">
                      <div className="flex flex-col items-center gap-2">
                        <PlugZap className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No active connections</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  filtered.map((conn) => (
                    <ConnectionRow
                      key={conn.name}
                      connection={conn}
                      onClose={() => handleClose(conn)}
                    />
                  ))
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </div>
    </AppShell>
  );
}
