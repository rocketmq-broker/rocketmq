'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useBindings, useVHosts, useConnectionStatus, useExchanges, useQueues } from '@/lib/hooks';
import { useUIStore } from '@/lib/store';
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
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { toast } from 'sonner';
import { mutate } from 'swr';
import {
  Plus,
  Search,
  Trash2,
  RefreshCw,
  Server,
  Loader2,
  Link2,
  ArrowRight,
} from 'lucide-react';
import type { Binding } from '@/types/rabbitmq';

function CreateBindingDialog({ vhost, exchanges, queues }: { 
  vhost: string; 
  exchanges: { name: string }[];
  queues: { name: string }[];
}) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [source, setSource] = useState('');
  const [destinationType, setDestinationType] = useState<'queue' | 'exchange'>('queue');
  const [destination, setDestination] = useState('');
  const [routingKey, setRoutingKey] = useState('');
  const [argumentsJson, setArgumentsJson] = useState('');

  const handleCreate = async () => {
    if (!source || !destination) {
      toast.error('Source and destination are required');
      return;
    }

    setLoading(true);
    try {
      let args = {};
      if (argumentsJson) {
        try {
          args = JSON.parse(argumentsJson);
        } catch {
          toast.error('Invalid JSON in arguments');
          setLoading(false);
          return;
        }
      }

      await rabbitmqClient.createBinding(vhost, source, destination, destinationType, {
        routing_key: routingKey,
        arguments: Object.keys(args).length > 0 ? args : undefined,
      });

      toast.success('Binding created successfully');
      setOpen(false);
      setSource('');
      setDestination('');
      setRoutingKey('');
      setArgumentsJson('');
      mutate((key: string) => key.includes('/bindings'));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to create binding');
    } finally {
      setLoading(false);
    }
  };

  const destinations = destinationType === 'queue' 
    ? queues.map(q => q.name) 
    : exchanges.filter(e => e.name).map(e => e.name);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          Add Binding
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>Create Binding</DialogTitle>
          <DialogDescription>
            Create a new binding between an exchange and a queue/exchange
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label>Source Exchange</Label>
            <Select value={source} onValueChange={setSource}>
              <SelectTrigger>
                <SelectValue placeholder="Select source exchange" />
              </SelectTrigger>
              <SelectContent>
                {exchanges
                  .filter((e) => e.name)
                  .map((e) => (
                    <SelectItem key={e.name} value={e.name}>
                      {e.name}
                    </SelectItem>
                  ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label>Destination Type</Label>
            <Select value={destinationType} onValueChange={(v) => {
              setDestinationType(v as 'queue' | 'exchange');
              setDestination('');
            }}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="queue">Queue</SelectItem>
                <SelectItem value="exchange">Exchange</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label>Destination {destinationType === 'queue' ? 'Queue' : 'Exchange'}</Label>
            <Select value={destination} onValueChange={setDestination}>
              <SelectTrigger>
                <SelectValue placeholder={`Select destination ${destinationType}`} />
              </SelectTrigger>
              <SelectContent>
                {destinations.map((d) => (
                  <SelectItem key={d} value={d}>
                    {d}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label>Routing Key</Label>
            <Input
              value={routingKey}
              onChange={(e) => setRoutingKey(e.target.value)}
              placeholder="routing.key.pattern"
            />
          </div>

          <div className="space-y-2">
            <Label>Arguments (JSON, optional)</Label>
            <Textarea
              value={argumentsJson}
              onChange={(e) => setArgumentsJson(e.target.value)}
              placeholder='{"x-match": "all"}'
              className="font-mono text-sm h-20"
            />
          </div>
        </div>

        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={handleCreate} disabled={loading}>
            {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Create Binding
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function BindingRow({ binding, onDelete }: { binding: Binding; onDelete: () => void }) {
  const [deleting, setDeleting] = useState(false);

  const handleDelete = async () => {
    setDeleting(true);
    await onDelete();
    setDeleting(false);
  };

  // Don't show default exchange bindings or self-bindings
  if (!binding.source) return null;

  return (
    <TableRow>
      <TableCell className="font-mono text-sm">
        {binding.source}
      </TableCell>
      <TableCell>
        <div className="flex items-center gap-2 text-muted-foreground">
          <ArrowRight className="h-4 w-4" />
        </div>
      </TableCell>
      <TableCell>
        <div className="flex items-center gap-2">
          <Badge variant="outline" className="text-xs">
            {binding.destination_type}
          </Badge>
          <span className="font-mono text-sm">{binding.destination}</span>
        </div>
      </TableCell>
      <TableCell className="font-mono text-sm">
        {binding.routing_key || <span className="text-muted-foreground">(empty)</span>}
      </TableCell>
      <TableCell>
        <span className="text-muted-foreground text-xs">{binding.vhost}</span>
      </TableCell>
      <TableCell>
        {Object.keys(binding.arguments).length > 0 ? (
          <pre className="text-xs max-w-xs truncate">
            {JSON.stringify(binding.arguments)}
          </pre>
        ) : (
          <span className="text-muted-foreground">-</span>
        )}
      </TableCell>
      <TableCell>
        <Button
          variant="ghost"
          size="icon"
          onClick={handleDelete}
          disabled={deleting}
        >
          {deleting ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Trash2 className="h-4 w-4" />
          )}
        </Button>
      </TableCell>
    </TableRow>
  );
}

export default function BindingsPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const selectedVHost = useUIStore((state) => state.selectedVHost);
  const setSelectedVHost = useUIStore((state) => state.setSelectedVHost);
  const { data: vhosts } = useVHosts();
  const { data: bindings, isLoading, mutate: mutateBindings } = useBindings(
    selectedVHost === 'all' ? undefined : selectedVHost
  );
  const { data: exchanges } = useExchanges(selectedVHost === 'all' ? '/' : selectedVHost);
  const { data: queues } = useQueues(selectedVHost === 'all' ? '/' : selectedVHost);
  const [search, setSearch] = useState('');

  const isConnected = connectionStatus?.connected;
  const vhostList = vhosts?.map((v) => v.name) || ['/'];

  const handleDelete = async (binding: Binding) => {
    try {
      await rabbitmqClient.deleteBinding(
        binding.vhost,
        binding.source,
        binding.destination,
        binding.destination_type,
        binding.properties_key
      );
      toast.success('Binding deleted');
      mutateBindings();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to delete binding');
    }
  };

  const filteredBindings = bindings?.filter((b) => {
    if (!b.source) return false; // Skip default exchange bindings
    const searchLower = search.toLowerCase();
    return (
      b.source.toLowerCase().includes(searchLower) ||
      b.destination.toLowerCase().includes(searchLower) ||
      b.routing_key.toLowerCase().includes(searchLower)
    );
  });

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to manage bindings.
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
            <h1 className="text-2xl font-bold">Bindings</h1>
            <p className="text-muted-foreground">
              View and manage exchange-to-queue bindings
            </p>
          </div>
          <CreateBindingDialog 
            vhost={selectedVHost === 'all' ? '/' : selectedVHost}
            exchanges={exchanges || []}
            queues={queues || []}
          />
        </div>

        {/* Stats */}
        <div className="grid gap-4 md:grid-cols-3">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-primary/10 p-2">
                  <Link2 className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{filteredBindings?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Total Bindings</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-blue-500/10 p-2">
                  <Link2 className="h-4 w-4 text-blue-500" />
                </div>
                <div>
                  <p className="text-2xl font-bold">
                    {bindings?.filter((b) => b.destination_type === 'queue').length || 0}
                  </p>
                  <p className="text-xs text-muted-foreground">To Queues</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-purple-500/10 p-2">
                  <Link2 className="h-4 w-4 text-purple-500" />
                </div>
                <div>
                  <p className="text-2xl font-bold">
                    {bindings?.filter((b) => b.destination_type === 'exchange').length || 0}
                  </p>
                  <p className="text-xs text-muted-foreground">To Exchanges</p>
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
              placeholder="Search bindings..."
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
          <Button variant="outline" size="icon" onClick={() => mutateBindings()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Table */}
        <Card>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Source Exchange</TableHead>
                  <TableHead className="w-12"></TableHead>
                  <TableHead>Destination</TableHead>
                  <TableHead>Routing Key</TableHead>
                  <TableHead>VHost</TableHead>
                  <TableHead>Arguments</TableHead>
                  <TableHead className="w-12"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  Array.from({ length: 5 }).map((_, i) => (
                    <TableRow key={i}>
                      {Array.from({ length: 7 }).map((_, j) => (
                        <TableCell key={j}>
                          <Skeleton className="h-4 w-full" />
                        </TableCell>
                      ))}
                    </TableRow>
                  ))
                ) : !filteredBindings || filteredBindings.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={7} className="text-center py-8">
                      <div className="flex flex-col items-center gap-2">
                        <Link2 className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No bindings found</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  filteredBindings.map((binding, index) => (
                    <BindingRow
                      key={`${binding.vhost}-${binding.source}-${binding.destination}-${binding.properties_key}-${index}`}
                      binding={binding}
                      onDelete={() => handleDelete(binding)}
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
