'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useNodes, useConnectionStatus } from '@/lib/hooks';
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
import { RefreshCw, Server, HardDrive, Cpu } from 'lucide-react';

export default function NodesPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const { data: nodes, isLoading, mutate: mutateNodes } = useNodes();

  const isConnected = connectionStatus?.connected;

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to view cluster nodes.
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
            <h1 className="text-2xl font-bold">Cluster Nodes</h1>
            <p className="text-muted-foreground">
              Monitor server performance, memory allocation, and OS statistics
            </p>
          </div>
          <Button variant="outline" size="icon" onClick={() => mutateNodes()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Nodes Table */}
        <Card>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Node Name</TableHead>
                  <TableHead>Erlang Version</TableHead>
                  <TableHead>Memory Limit</TableHead>
                  <TableHead>Disk Limit</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  Array.from({ length: 1 }).map((_, i) => (
                    <TableRow key={i}>
                      {Array.from({ length: 6 }).map((_, j) => (
                        <TableCell key={j}><Skeleton className="h-4 w-full" /></TableCell>
                      ))}
                    </TableRow>
                  ))
                ) : !nodes || nodes.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center py-8">
                      <div className="flex flex-col items-center gap-2">
                        <HardDrive className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No cluster nodes found</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  nodes.map((node, idx) => (
                    <TableRow key={idx}>
                      <TableCell className="font-mono text-sm font-semibold text-foreground">
                        {node.name}
                      </TableCell>
                      <TableCell className="font-mono text-xs">{node.applications?.find(a => a.name === 'rabbit')?.version || '26.0'}</TableCell>
                      <TableCell className="font-mono text-xs">
                        {(node.mem_limit ? (node.mem_limit / (1024 * 1024 * 1024)).toFixed(2) : '0')} GB
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        {(node.disk_free_limit ? (node.disk_free_limit / (1024 * 1024 * 1024)).toFixed(2) : '0')} GB
                      </TableCell>
                      <TableCell className="capitalize text-xs">
                        <Badge variant="outline">{node.type || 'disc'}</Badge>
                      </TableCell>
                      <TableCell>
                        <Badge
                          variant={node.running ? 'default' : 'secondary'}
                          className={node.running ? 'bg-green-500/10 text-green-500 border-green-500/30' : ''}
                        >
                          {node.running ? 'Online' : 'Offline'}
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
