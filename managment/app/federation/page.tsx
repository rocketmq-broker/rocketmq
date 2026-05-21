'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useFederationLinks, useFederationUpstreams, useConnectionStatus } from '@/lib/hooks';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
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
import { Search, RefreshCw, Server, Network, Link, AlertTriangle } from 'lucide-react';

export default function FederationPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const { data: links, isLoading: linksLoading, mutate: mutateLinks } = useFederationLinks();
  const { data: upstreams, isLoading: upstreamsLoading, mutate: mutateUpstreams } = useFederationUpstreams();
  const [search, setSearch] = useState('');

  const isConnected = connectionStatus?.connected;

  const handleRefresh = () => {
    mutateLinks();
    mutateUpstreams();
  };

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to manage federation.
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
            <h1 className="text-2xl font-bold">Federation</h1>
            <p className="text-muted-foreground">
              Configure and monitor federated exchanges and queues across different clusters
            </p>
          </div>
          <Button variant="outline" size="icon" onClick={handleRefresh}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Stats / Warnings */}
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-primary/10 p-2">
                  <Network className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{links?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Active Federation Links</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-blue-500/10 p-2">
                  <Link className="h-4 w-4 text-blue-500" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{upstreams?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Configured Upstreams</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Federation Links section */}
        <div>
          <h2 className="text-lg font-semibold mb-3">Federation Links</h2>
          <Card>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>VHost</TableHead>
                    <TableHead>Link Name</TableHead>
                    <TableHead>Resource</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead>Upstream</TableHead>
                    <TableHead>Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {linksLoading ? (
                    Array.from({ length: 2 }).map((_, i) => (
                      <TableRow key={i}>
                        {Array.from({ length: 6 }).map((_, j) => (
                          <TableCell key={j}><Skeleton className="h-4 w-full" /></TableCell>
                        ))}
                      </TableRow>
                    ))
                  ) : !links || links.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={6} className="text-center py-8">
                        <div className="flex flex-col items-center gap-2">
                          <Network className="h-8 w-8 text-muted-foreground" />
                          <p className="text-muted-foreground">No active federation links found</p>
                        </div>
                      </TableCell>
                    </TableRow>
                  ) : (
                    links.map((link, idx) => (
                      <TableRow key={idx}>
                        <TableCell className="font-mono text-sm">{link.vhost}</TableCell>
                        <TableCell className="font-semibold">{link.id}</TableCell>
                        <TableCell className="font-mono text-sm">{link.exchange || link.queue || '-'}</TableCell>
                        <TableCell className="capitalize">{link.type}</TableCell>
                        <TableCell className="font-mono text-sm">{link.upstream}</TableCell>
                        <TableCell>
                          <Badge
                            variant={link.status === 'running' ? 'default' : 'secondary'}
                            className={link.status === 'running' ? 'bg-green-500/10 text-green-500 border-green-500/30' : ''}
                          >
                            {link.status}
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

        {/* Upstreams section */}
        <div>
          <h2 className="text-lg font-semibold mb-3">Configured Federation Upstreams</h2>
          <Card>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>VHost</TableHead>
                    <TableHead>Upstream Name</TableHead>
                    <TableHead>URI</TableHead>
                    <TableHead>Prefetch</TableHead>
                    <TableHead>Reconnect Delay</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {upstreamsLoading ? (
                    Array.from({ length: 2 }).map((_, i) => (
                      <TableRow key={i}>
                        {Array.from({ length: 5 }).map((_, j) => (
                          <TableCell key={j}><Skeleton className="h-4 w-full" /></TableCell>
                        ))}
                      </TableRow>
                    ))
                  ) : !upstreams || upstreams.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={5} className="text-center py-8">
                        <div className="flex flex-col items-center gap-2">
                          <Network className="h-8 w-8 text-muted-foreground" />
                          <p className="text-muted-foreground">No federation upstreams configured</p>
                        </div>
                      </TableCell>
                    </TableRow>
                  ) : (
                    upstreams.map((ups, idx) => (
                      <TableRow key={idx}>
                        <TableCell className="font-mono text-sm">{ups.vhost}</TableCell>
                        <TableCell className="font-semibold">{ups.name}</TableCell>
                        <TableCell className="font-mono text-xs max-w-xs truncate">
                          {typeof ups.value === 'object' && ups.value !== null ? (ups.value as Record<string, string>).uri : '-'}
                        </TableCell>
                        <TableCell className="font-mono text-sm">
                          {typeof ups.value === 'object' && ups.value !== null ? (ups.value as Record<string, string>).prefetch_count : '-'}
                        </TableCell>
                        <TableCell className="font-mono text-sm">
                          {typeof ups.value === 'object' && ups.value !== null ? (ups.value as Record<string, string>).reconnect_delay : '-'}s
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </div>
      </div>
    </AppShell>
  );
}
