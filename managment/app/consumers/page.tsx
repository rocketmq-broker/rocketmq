'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useConsumers, useConnectionStatus } from '@/lib/hooks';
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
import { Search, RefreshCw, Server, Users, ArrowRight } from 'lucide-react';
import type { Consumer } from '@/types/rabbitmq';

function ConsumerRow({ consumer }: { consumer: Consumer }) {
  return (
    <TableRow>
      <TableCell className="font-mono text-sm font-semibold text-foreground">
        {consumer.consumer_tag}
      </TableCell>
      <TableCell className="font-mono text-sm">
        {consumer.queue?.name || '-'}
      </TableCell>
      <TableCell className="font-mono text-sm">
        {consumer.channel_details?.name || '-'}
      </TableCell>
      <TableCell className="text-center font-mono">
        {consumer.prefetch_count}
      </TableCell>
      <TableCell className="text-center">
        <Badge
          variant={consumer.ack_required ? 'default' : 'secondary'}
          className={consumer.ack_required ? 'bg-blue-500/10 text-blue-500 border-blue-500/30' : ''}
        >
          {consumer.ack_required ? 'Yes' : 'No'}
        </Badge>
      </TableCell>
      <TableCell className="text-center">
        <Badge
          variant={consumer.exclusive ? 'destructive' : 'secondary'}
          className={consumer.exclusive ? 'bg-red-500/10 text-red-500 border-red-500/30' : ''}
        >
          {consumer.exclusive ? 'Exclusive' : 'Shared'}
        </Badge>
      </TableCell>
    </TableRow>
  );
}

export default function ConsumersPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const { data: consumers, isLoading, mutate: mutateConsumers } = useConsumers();
  const [search, setSearch] = useState('');

  const isConnected = connectionStatus?.connected;

  const filtered = consumers?.filter((c) =>
    c.consumer_tag.toLowerCase().includes(search.toLowerCase()) ||
    (c.queue?.name && c.queue.name.toLowerCase().includes(search.toLowerCase()))
  );

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to monitor consumers.
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
            <h1 className="text-2xl font-bold">Consumers</h1>
            <p className="text-muted-foreground">
              Monitor active message consumers subscribed to queues
            </p>
          </div>
          <Button variant="outline" size="icon" onClick={() => mutateConsumers()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Stats */}
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-primary/10 p-2">
                  <Users className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{consumers?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Active Consumers</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-blue-500/10 p-2">
                  <ArrowRight className="h-4 w-4 text-blue-500" />
                </div>
                <div>
                  <p className="text-2xl font-bold">
                    {consumers?.filter((c) => c.ack_required).length || 0}
                  </p>
                  <p className="text-xs text-muted-foreground">Ack Required</p>
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
              placeholder="Search consumers..."
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
                  <TableHead>Consumer Tag</TableHead>
                  <TableHead>Queue</TableHead>
                  <TableHead>Channel</TableHead>
                  <TableHead className="text-center">Prefetch Count</TableHead>
                  <TableHead className="text-center">Ack Required</TableHead>
                  <TableHead className="text-center">Exclusive</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  Array.from({ length: 3 }).map((_, i) => (
                    <TableRow key={i}>
                      {Array.from({ length: 6 }).map((_, j) => (
                        <TableCell key={j}>
                          <Skeleton className="h-4 w-full" />
                        </TableCell>
                      ))}
                    </TableRow>
                  ))
                ) : !filtered || filtered.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center py-8">
                      <div className="flex flex-col items-center gap-2">
                        <Users className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No active consumers</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  filtered.map((cons, idx) => (
                    <ConsumerRow
                      key={`${cons.consumer_tag}-${idx}`}
                      consumer={cons}
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
