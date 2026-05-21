'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useFeatureFlags, useConnectionStatus } from '@/lib/hooks';
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
import { RefreshCw, Server, Flag, ShieldAlert } from 'lucide-react';

export default function FeatureFlagsPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const { data: flags, isLoading, mutate: mutateFlags } = useFeatureFlags();

  const isConnected = connectionStatus?.connected;

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to view feature flags.
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
            <h1 className="text-2xl font-bold">Feature Flags</h1>
            <p className="text-muted-foreground">
              Monitor capability compatibility and enable newer core engine features
            </p>
          </div>
          <Button variant="outline" size="icon" onClick={() => mutateFlags()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Feature Flags Table */}
        <Card>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Flag Name</TableHead>
                  <TableHead>Stability</TableHead>
                  <TableHead>Description</TableHead>
                  <TableHead>State</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  Array.from({ length: 3 }).map((_, i) => (
                    <TableRow key={i}>
                      {Array.from({ length: 4 }).map((_, j) => (
                        <TableCell key={j}><Skeleton className="h-4 w-full" /></TableCell>
                      ))}
                    </TableRow>
                  ))
                ) : !flags || flags.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={4} className="text-center py-8">
                      <div className="flex flex-col items-center gap-2">
                        <Flag className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No feature flags found</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  flags.map((flag, idx) => (
                    <TableRow key={idx}>
                      <TableCell className="font-mono text-sm font-semibold text-foreground">{flag.name}</TableCell>
                      <TableCell className="capitalize text-xs">
                        <Badge variant="outline">{flag.stability || 'stable'}</Badge>
                      </TableCell>
                      <TableCell className="text-sm text-muted-foreground max-w-md">{flag.desc || 'No description provided'}</TableCell>
                      <TableCell>
                        <Badge
                          variant={flag.state === 'enabled' ? 'default' : 'secondary'}
                          className={flag.state === 'enabled' ? 'bg-green-500/10 text-green-500 border-green-500/30' : ''}
                        >
                          {flag.state || 'disabled'}
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
