'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useChannels, useConnectionStatus } from '@/lib/hooks';
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
import { Search, RefreshCw, Server, Layers, ArrowDownLeft, ArrowUpRight } from 'lucide-react';
import type { Channel } from '@/types/rabbitmq';

function ChannelRow({ channel }: { channel: Channel }) {
  return (
    <TableRow>
      <TableCell className="font-mono text-sm">
        <span className="font-medium text-foreground">{channel.name}</span>
      </TableCell>
      <TableCell>
        <Badge
          variant={channel.state === 'running' ? 'default' : 'secondary'}
          className={channel.state === 'running' ? 'bg-green-500/10 text-green-500 border-green-500/30' : ''}
        >
          {channel.state || 'idle'}
        </Badge>
      </TableCell>
      <TableCell className="font-mono text-sm">
        {channel.vhost}
      </TableCell>
      <TableCell className="text-center font-mono">
        {channel.consumer_count}
      </TableCell>
      <TableCell className="text-center font-mono">
        {channel.prefetch_count}
      </TableCell>
      <TableCell className="text-right font-mono text-sm text-yellow-500">
        {channel.messages_unacknowledged}
      </TableCell>
      <TableCell className="text-right font-mono text-sm text-blue-500">
        {channel.messages_unconfirmed}
      </TableCell>
      <TableCell className="font-mono text-sm">
        {channel.user}
      </TableCell>
    </TableRow>
  );
}

export default function ChannelsPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const { data: channels, isLoading, mutate: mutateChannels } = useChannels();
  const [search, setSearch] = useState('');

  const isConnected = connectionStatus?.connected;

  const filtered = channels?.filter((c) =>
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
            Connect to a RabbitMQ server to manage channels.
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
            <h1 className="text-2xl font-bold">Channels</h1>
            <p className="text-muted-foreground">
              Monitor active AMQP channels on established connections
            </p>
          </div>
          <Button variant="outline" size="icon" onClick={() => mutateChannels()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Stats */}
        <div className="grid gap-4 md:grid-cols-3">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-primary/10 p-2">
                  <Layers className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{channels?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Total Channels</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-yellow-500/10 p-2">
                  <ArrowDownLeft className="h-4 w-4 text-yellow-500" />
                </div>
                <div>
                  <p className="text-2xl font-bold">
                    {channels?.reduce((sum, c) => sum + c.messages_unacknowledged, 0) || 0}
                  </p>
                  <p className="text-xs text-muted-foreground">Unacknowledged Messages</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-blue-500/10 p-2">
                  <ArrowUpRight className="h-4 w-4 text-blue-500" />
                </div>
                <div>
                  <p className="text-2xl font-bold">
                    {channels?.reduce((sum, c) => sum + c.consumer_count, 0) || 0}
                  </p>
                  <p className="text-xs text-muted-foreground">Active Consumers</p>
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
              placeholder="Search channels..."
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
                  <TableHead>Channel Name</TableHead>
                  <TableHead>State</TableHead>
                  <TableHead>VHost</TableHead>
                  <TableHead className="text-center">Consumers</TableHead>
                  <TableHead className="text-center">Prefetch</TableHead>
                  <TableHead className="text-right">Unacked</TableHead>
                  <TableHead className="text-right">Unconfirmed</TableHead>
                  <TableHead>User</TableHead>
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
                        <Layers className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No active channels</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  filtered.map((chan) => (
                    <ChannelRow
                      key={chan.name}
                      channel={chan}
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
