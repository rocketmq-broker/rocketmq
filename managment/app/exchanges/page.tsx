'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useExchanges, useVHosts, useConnectionStatus } from '@/lib/hooks';
import { useUIStore } from '@/lib/store';
import { rabbitmqClient } from '@/lib/rabbitmq-client';
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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { Textarea } from '@/components/ui/textarea';
import { toast } from 'sonner';
import { mutate } from 'swr';
import {
  Plus,
  Search,
  MoreHorizontal,
  Trash2,
  FileJson,
  Send,
  RefreshCw,
  Server,
  Loader2,
  ChevronRight,
  ArrowLeftRight,
} from 'lucide-react';
import Link from 'next/link';
import type { Exchange } from '@/types/rabbitmq';

const EXCHANGE_TYPES = [
  { value: 'direct', label: 'Direct', description: 'Routes by exact routing key match' },
  { value: 'fanout', label: 'Fanout', description: 'Broadcasts to all bound queues' },
  { value: 'topic', label: 'Topic', description: 'Routes by pattern matching' },
  { value: 'headers', label: 'Headers', description: 'Routes by message headers' },
];

function ExchangeTypeIcon({ type }: { type: string }) {
  const colors: Record<string, string> = {
    direct: 'bg-blue-500/10 text-blue-500 border-blue-500/30',
    fanout: 'bg-green-500/10 text-green-500 border-green-500/30',
    topic: 'bg-purple-500/10 text-purple-500 border-purple-500/30',
    headers: 'bg-yellow-500/10 text-yellow-500 border-yellow-500/30',
  };
  
  return (
    <Badge variant="outline" className={`text-xs ${colors[type] || ''}`}>
      {type}
    </Badge>
  );
}

function CreateExchangeDialog({ vhosts }: { vhosts: string[] }) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [name, setName] = useState('');
  const [vhost, setVhost] = useState('/');
  const [type, setType] = useState('direct');
  const [durable, setDurable] = useState(true);
  const [autoDelete, setAutoDelete] = useState(false);
  const [internal, setInternal] = useState(false);
  const [alternateExchange, setAlternateExchange] = useState('');

  const handleCreate = async () => {
    if (!name) {
      toast.error('Exchange name is required');
      return;
    }

    setLoading(true);
    try {
      const args: Record<string, unknown> = {};
      if (alternateExchange) {
        args['alternate-exchange'] = alternateExchange;
      }

      await rabbitmqClient.createExchange(vhost, name, {
        type,
        durable,
        auto_delete: autoDelete,
        internal,
        arguments: Object.keys(args).length > 0 ? args : undefined,
      });

      toast.success(`Exchange "${name}" created successfully`);
      setOpen(false);
      setName('');
      mutate((key: string) => key.includes('/exchanges'));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to create exchange');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          Add Exchange
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>Create Exchange</DialogTitle>
          <DialogDescription>
            Create a new exchange with the specified configuration
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="name">Name</Label>
              <Input
                id="name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="my-exchange"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="vhost">Virtual Host</Label>
              <Select value={vhost} onValueChange={setVhost}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {vhosts.map((v) => (
                    <SelectItem key={v} value={v}>
                      {v}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="space-y-2">
            <Label>Type</Label>
            <Select value={type} onValueChange={setType}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {EXCHANGE_TYPES.map((t) => (
                  <SelectItem key={t.value} value={t.value}>
                    <div className="flex flex-col">
                      <span>{t.label}</span>
                      <span className="text-xs text-muted-foreground">{t.description}</span>
                    </div>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="flex items-center justify-between">
            <Label htmlFor="durable">Durable</Label>
            <Switch
              id="durable"
              checked={durable}
              onCheckedChange={setDurable}
            />
          </div>

          <div className="flex items-center justify-between">
            <Label htmlFor="autoDelete">Auto-delete</Label>
            <Switch
              id="autoDelete"
              checked={autoDelete}
              onCheckedChange={setAutoDelete}
            />
          </div>

          <div className="flex items-center justify-between">
            <Label htmlFor="internal">Internal</Label>
            <Switch
              id="internal"
              checked={internal}
              onCheckedChange={setInternal}
            />
          </div>

          <div className="border-t pt-4">
            <h4 className="text-sm font-medium mb-3">Arguments (Optional)</h4>
            <div className="space-y-2">
              <Label htmlFor="ae">Alternate Exchange</Label>
              <Input
                id="ae"
                value={alternateExchange}
                onChange={(e) => setAlternateExchange(e.target.value)}
                placeholder="alternate-exchange-name"
              />
            </div>
          </div>
        </div>

        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={handleCreate} disabled={loading}>
            {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Create Exchange
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function PublishMessageDialog({ exchange }: { exchange: Exchange }) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [payload, setPayload] = useState('');
  const [routingKey, setRoutingKey] = useState('');
  const [contentType, setContentType] = useState('application/json');

  const handlePublish = async () => {
    if (!payload) {
      toast.error('Message payload is required');
      return;
    }

    setLoading(true);
    try {
      const result = await rabbitmqClient.publishMessage(
        exchange.vhost,
        exchange.name,
        routingKey,
        payload,
        { content_type: contentType }
      );

      if (result.routed) {
        toast.success('Message published successfully');
        setPayload('');
      } else {
        toast.warning('Message published but was not routed to any queue');
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to publish message');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <DropdownMenuItem onSelect={(e) => e.preventDefault()}>
          <Send className="mr-2 h-4 w-4" />
          Publish Message
        </DropdownMenuItem>
      </DialogTrigger>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>Publish to {exchange.name}</DialogTitle>
          <DialogDescription>
            Publish a message through this exchange
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label htmlFor="routingKey">Routing Key</Label>
            <Input
              id="routingKey"
              value={routingKey}
              onChange={(e) => setRoutingKey(e.target.value)}
              placeholder={exchange.type === 'fanout' ? '(not used for fanout)' : 'routing.key'}
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="contentType">Content Type</Label>
            <Select value={contentType} onValueChange={setContentType}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="application/json">application/json</SelectItem>
                <SelectItem value="text/plain">text/plain</SelectItem>
                <SelectItem value="application/xml">application/xml</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label htmlFor="payload">Payload</Label>
            <Textarea
              id="payload"
              value={payload}
              onChange={(e) => setPayload(e.target.value)}
              placeholder='{"message": "Hello!"}'
              className="font-mono text-sm h-32"
            />
          </div>
        </div>

        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={handlePublish} disabled={loading}>
            {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Publish
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function ExchangeActions({ exchange, onDelete }: { exchange: Exchange; onDelete: () => void }) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="icon">
          <MoreHorizontal className="h-4 w-4" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <Link href={`/exchanges/${encodeURIComponent(exchange.vhost)}/${encodeURIComponent(exchange.name)}`}>
          <DropdownMenuItem>
            <FileJson className="mr-2 h-4 w-4" />
            View Details
          </DropdownMenuItem>
        </Link>
        {!exchange.name.startsWith('amq.') && (
          <PublishMessageDialog exchange={exchange} />
        )}
        <DropdownMenuSeparator />
        {!exchange.name.startsWith('amq.') && exchange.name !== '' && (
          <DropdownMenuItem
            onClick={onDelete}
            className="text-destructive focus:text-destructive"
          >
            <Trash2 className="mr-2 h-4 w-4" />
            Delete Exchange
          </DropdownMenuItem>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function ExchangeRow({ exchange, onDelete }: { exchange: Exchange; onDelete: () => void }) {
  const rate = exchange.message_stats?.publish_details?.rate || 0;
  const isDefault = exchange.name === '';
  const isSystem = exchange.name.startsWith('amq.');

  return (
    <TableRow>
      <TableCell>
        <div className="flex flex-col">
          <Link
            href={`/exchanges/${encodeURIComponent(exchange.vhost)}/${encodeURIComponent(exchange.name || '(AMQP default)')}`}
            className="font-medium hover:underline flex items-center gap-1"
          >
            {isDefault ? '(AMQP default)' : exchange.name}
            <ChevronRight className="h-3 w-3" />
          </Link>
          <span className="text-xs text-muted-foreground">{exchange.vhost}</span>
        </div>
      </TableCell>
      <TableCell>
        <ExchangeTypeIcon type={exchange.type} />
      </TableCell>
      <TableCell>
        <div className="flex gap-1">
          {exchange.durable && (
            <Badge variant="outline" className="text-xs">D</Badge>
          )}
          {exchange.auto_delete && (
            <Badge variant="outline" className="text-xs">AD</Badge>
          )}
          {exchange.internal && (
            <Badge variant="outline" className="text-xs">I</Badge>
          )}
          {isSystem && (
            <Badge variant="secondary" className="text-xs">System</Badge>
          )}
        </div>
      </TableCell>
      <TableCell className="text-right">
        <span className={rate > 0 ? 'text-green-500' : 'text-muted-foreground'}>
          {rate.toFixed(1)}/s
        </span>
      </TableCell>
      <TableCell>
        {exchange.policy && (
          <Badge variant="outline">{exchange.policy}</Badge>
        )}
      </TableCell>
      <TableCell>
        <ExchangeActions exchange={exchange} onDelete={onDelete} />
      </TableCell>
    </TableRow>
  );
}

export default function ExchangesPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const selectedVHost = useUIStore((state) => state.selectedVHost);
  const setSelectedVHost = useUIStore((state) => state.setSelectedVHost);
  const { data: vhosts } = useVHosts();
  const { data: exchanges, isLoading, mutate: mutateExchanges } = useExchanges(
    selectedVHost === 'all' ? undefined : selectedVHost
  );
  const [search, setSearch] = useState('');
  const [showSystem, setShowSystem] = useState(true);

  const isConnected = connectionStatus?.connected;
  const vhostList = vhosts?.map((v) => v.name) || ['/'];

  const handleDelete = async (exchange: Exchange) => {
    if (!confirm(`Are you sure you want to delete exchange "${exchange.name}"?`)) {
      return;
    }

    try {
      await rabbitmqClient.deleteExchange(exchange.vhost, exchange.name);
      toast.success(`Exchange "${exchange.name}" deleted`);
      mutateExchanges();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to delete exchange');
    }
  };

  const filteredExchanges = exchanges?.filter((e) => {
    const matchesSearch = e.name.toLowerCase().includes(search.toLowerCase());
    const matchesSystem = showSystem || (!e.name.startsWith('amq.') && e.name !== '');
    return matchesSearch && matchesSystem;
  });

  const exchangesByType = exchanges?.reduce((acc, e) => {
    acc[e.type] = (acc[e.type] || 0) + 1;
    return acc;
  }, {} as Record<string, number>) || {};

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to manage exchanges.
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
            <h1 className="text-2xl font-bold">Exchanges</h1>
            <p className="text-muted-foreground">
              Manage message routing exchanges
            </p>
          </div>
          <CreateExchangeDialog vhosts={vhostList} />
        </div>

        {/* Stats */}
        <div className="grid gap-4 md:grid-cols-5">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-primary/10 p-2">
                  <ArrowLeftRight className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{exchanges?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Total</p>
                </div>
              </div>
            </CardContent>
          </Card>
          {Object.entries(exchangesByType).map(([type, count]) => (
            <Card key={type}>
              <CardContent className="p-4">
                <div className="flex items-center gap-4">
                  <ExchangeTypeIcon type={type} />
                  <div>
                    <p className="text-2xl font-bold">{count}</p>
                    <p className="text-xs text-muted-foreground capitalize">{type}</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>

        {/* Filters */}
        <div className="flex items-center gap-4">
          <div className="relative flex-1 max-w-sm">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="Search exchanges..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-9"
            />
          </div>
          <Select value={selectedVHost} onValueChange={setSelectedVHost}>
            <SelectTrigger className="w-48">
              <SelectValue placeholder="Virtual Host" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Virtual Hosts</SelectItem>
              {vhostList.map((v) => (
                <SelectItem key={v} value={v}>
                  {v}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <div className="flex items-center gap-2">
            <Switch
              id="showSystem"
              checked={showSystem}
              onCheckedChange={setShowSystem}
            />
            <Label htmlFor="showSystem" className="text-sm">Show system</Label>
          </div>
          <Button variant="outline" size="icon" onClick={() => mutateExchanges()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Table */}
        <Card>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Features</TableHead>
                  <TableHead className="text-right">Message Rate</TableHead>
                  <TableHead>Policy</TableHead>
                  <TableHead className="w-12"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  Array.from({ length: 5 }).map((_, i) => (
                    <TableRow key={i}>
                      {Array.from({ length: 6 }).map((_, j) => (
                        <TableCell key={j}>
                          <Skeleton className="h-4 w-full" />
                        </TableCell>
                      ))}
                    </TableRow>
                  ))
                ) : filteredExchanges?.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center py-8">
                      <div className="flex flex-col items-center gap-2">
                        <ArrowLeftRight className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No exchanges found</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  filteredExchanges?.map((exchange) => (
                    <ExchangeRow
                      key={`${exchange.vhost}-${exchange.name}`}
                      exchange={exchange}
                      onDelete={() => handleDelete(exchange)}
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
