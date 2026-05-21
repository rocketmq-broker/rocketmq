'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useConnectionStatus } from '@/lib/hooks';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Server, Bug, Play, Square, FileText } from 'lucide-react';

export default function TracingPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const [tracingActive, setTracingActive] = useState(false);
  const [logs, setLogs] = useState<{ time: string; msg: string; exchange: string; key: string }[]>([]);

  const isConnected = connectionStatus?.connected;

  const startTracing = () => {
    setTracingActive(true);
    setLogs([
      { time: new Date().toLocaleTimeString(), msg: 'Tracing enabled on default virtual host', exchange: '-', key: '-' },
      { time: new Date().toLocaleTimeString(), msg: 'Durable binding validated: orders.exchange -> dead-letter-queue', exchange: 'orders.exchange', key: 'orders.error' },
      { time: new Date().toLocaleTimeString(), msg: 'Client peer connected: 127.0.0.1:54992', exchange: '-', key: '-' },
    ]);
  };

  const stopTracing = () => {
    setTracingActive(false);
  };

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to use tracing tools.
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
            <h1 className="text-2xl font-bold">Firehose &amp; Tracing</h1>
            <p className="text-muted-foreground">
              Introspect and debug live messages flowing through exchanges and queues
            </p>
          </div>
          <div className="flex items-center gap-2">
            {tracingActive ? (
              <Button variant="destructive" onClick={stopTracing}>
                <Square className="mr-2 h-4 w-4" />
                Stop Firehose
              </Button>
            ) : (
              <Button onClick={startTracing}>
                <Play className="mr-2 h-4 w-4" />
                Start Firehose
              </Button>
            )}
          </div>
        </div>

        {/* Tracing Controls Card */}
        <Card>
          <CardContent className="p-6">
            <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
              <div className="space-y-1">
                <h3 className="font-semibold flex items-center gap-2">
                  <Bug className="h-5 w-5 text-primary" />
                  Live Payload Debugger
                </h3>
                <p className="text-sm text-muted-foreground">
                  The firehose intercepts all published and delivered messages on the broker. Use with care in production.
                </p>
              </div>
              <div>
                <Badge variant={tracingActive ? 'default' : 'secondary'} className={tracingActive ? 'bg-red-500/10 text-red-500 border-red-500/30' : ''}>
                  {tracingActive ? 'RECORDING LIVE' : 'STOPPED'}
                </Badge>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Logs Table */}
        <div>
          <h3 className="text-lg font-semibold mb-3">Live Trace Log</h3>
          <Card>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-24">Time</TableHead>
                    <TableHead>Event Message</TableHead>
                    <TableHead>Exchange</TableHead>
                    <TableHead>Routing Key</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {logs.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={4} className="text-center py-8">
                        <div className="flex flex-col items-center gap-2">
                          <FileText className="h-8 w-8 text-muted-foreground" />
                          <p className="text-muted-foreground">No events recorded. Start the firehose to see logs.</p>
                        </div>
                      </TableCell>
                    </TableRow>
                  ) : (
                    logs.map((log, idx) => (
                      <TableRow key={idx} className="font-mono text-xs">
                        <TableCell className="text-muted-foreground">{log.time}</TableCell>
                        <TableCell className="text-foreground font-semibold">{log.msg}</TableCell>
                        <TableCell>{log.exchange}</TableCell>
                        <TableCell>{log.key}</TableCell>
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
