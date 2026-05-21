'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useQueues, useConnectionStatus } from '@/lib/hooks';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
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
import { RefreshCw, Server, Blocks } from 'lucide-react';

export default function StreamsPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const { data: queues, isLoading, mutate } = useQueues();

  const isConnected = connectionStatus?.connected;
  // Filter queues that are type 'stream'
  const streams = queues?.filter(q => q.type === 'stream');

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to view streams.
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
            <h1 className="text-2xl font-bold">Append-Only Streams</h1>
            <p className="text-muted-foreground">
              Monitor high-throughput stream logs and offset tracking
            </p>
          </div>
          <Button variant="outline" size="icon" onClick={() => mutate()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Stats */}
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-primary/10 p-2">
                  <Blocks className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{streams?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Active Streams</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Streams Table */}
        <Card>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Stream Name</TableHead>
                  <TableHead>Virtual Host</TableHead>
                  <TableHead>Features</TableHead>
                  <TableHead className="text-right">Messages</TableHead>
                  <TableHead className="text-right font-mono">Size</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  Array.from({ length: 2 }).map((_, i) => (
                    <TableRow key={i}>
                      {Array.from({ length: 6 }).map((_, j) => (
                        <TableCell key={j}><Skeleton className="h-4 w-full" /></TableCell>
                      ))}
                    </TableRow>
                  ))
                ) : !streams || streams.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center py-8">
                      <div className="flex flex-col items-center gap-2">
                        <Blocks className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No streams active. Convert or declare queues as type &quot;stream&quot;.</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  streams.map((stream, idx) => (
                    <TableRow key={idx}>
                      <TableCell className="font-mono text-sm font-semibold text-foreground">
                        {stream.name}
                      </TableCell>
                      <TableCell className="font-mono text-xs">{stream.vhost}</TableCell>
                      <TableCell>
                        <Badge variant="outline" className="text-xs">Durable</Badge>
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm text-foreground">
                        {stream.messages || 0}
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm text-muted-foreground">
                        -
                      </TableCell>
                      <TableCell>
                        <Badge
                          variant={stream.state === 'running' ? 'default' : 'secondary'}
                          className={stream.state === 'running' ? 'bg-green-500/10 text-green-500 border-green-500/30' : ''}
                        >
                          {stream.state || 'running'}
                        </Badge>
                      </TableCell>
                    </TableRow>
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
