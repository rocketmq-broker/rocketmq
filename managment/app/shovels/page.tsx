'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useShovelStatus, useConnectionStatus } from '@/lib/hooks';
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
import { RefreshCw, Server, Shovel } from 'lucide-react';

export default function ShovelsPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const { data: shovels, isLoading, mutate: mutateShovels } = useShovelStatus();

  const isConnected = connectionStatus?.connected;

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to manage Shovels.
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
            <h1 className="text-2xl font-bold">Shovels</h1>
            <p className="text-muted-foreground">
              Define and monitor active dynamic Shovels that unidirectionally copy or move messages
            </p>
          </div>
          <Button variant="outline" size="icon" onClick={() => mutateShovels()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Stats */}
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-primary/10 p-2">
                  <Shovel className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{shovels?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Active Shovels</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Shovels Status Table */}
        <Card>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Virtual Host</TableHead>
                  <TableHead>Name</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Source</TableHead>
                  <TableHead>Destination</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Last Activity</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  Array.from({ length: 2 }).map((_, i) => (
                    <TableRow key={i}>
                      {Array.from({ length: 7 }).map((_, j) => (
                        <TableCell key={j}><Skeleton className="h-4 w-full" /></TableCell>
                      ))}
                    </TableRow>
                  ))
                ) : !shovels || shovels.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={7} className="text-center py-8">
                      <div className="flex flex-col items-center gap-2">
                        <Shovel className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No shovels configured or running</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  shovels.map((sh, idx) => (
                    <TableRow key={idx}>
                      <TableCell className="font-mono text-sm">{sh.vhost}</TableCell>
                      <TableCell className="font-semibold">{sh.name}</TableCell>
                      <TableCell className="capitalize">{sh.type}</TableCell>
                      <TableCell className="font-mono text-xs">{sh.src_uri || 'local'}</TableCell>
                      <TableCell className="font-mono text-xs">{sh.dest_uri || 'local'}</TableCell>
                      <TableCell>
                        <Badge
                          variant={sh.state === 'running' ? 'default' : 'secondary'}
                          className={sh.state === 'running' ? 'bg-green-500/10 text-green-500 border-green-500/30' : ''}
                        >
                          {sh.state}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">{sh.timestamp || '-'}</TableCell>
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
