'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useQueues, useVHosts, useConnectionStatus } from '@/lib/hooks';
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
import { ScrollArea } from '@/components/ui/scroll-area';
import { toast } from 'sonner';
import { mutate } from 'swr';
import {
  Plus,
  Search,
  MoreHorizontal,
  Trash2,
  Eye,
  MessageSquare,
  Send,
  RefreshCw,
  Server,
  Loader2,
  ChevronRight,
  Inbox,
  FileJson,
} from 'lucide-react';
import Link from 'next/link';
import type { Queue, QueueMessage } from '@/types/rabbitmq';

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function formatNumber(num: number): string {
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
  return num.toString();
}

function QueueTypeIcon({ type }: { type: string }) {
  switch (type) {
    case 'quorum':
      return <Badge variant="outline" className="text-xs">Quorum</Badge>;
    case 'stream':
      return <Badge variant="outline" className="text-xs bg-blue-500/10 text-blue-500 border-blue-500/30">Stream</Badge>;
    default:
      return <Badge variant="outline" className="text-xs">Classic</Badge>;
  }
}

function CreateQueueDialog({ vhosts }: { vhosts: string[] }) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [name, setName] = useState('');
  const [vhost, setVhost] = useState('/');
  const [type, setType] = useState<'classic' | 'quorum' | 'stream'>('classic');
  const [durable, setDurable] = useState(true);
  const [autoDelete, setAutoDelete] = useState(false);
  const [messageTtl, setMessageTtl] = useState('');
  const [maxLength, setMaxLength] = useState('');
  const [deadLetterExchange, setDeadLetterExchange] = useState('');
  const [deadLetterRoutingKey, setDeadLetterRoutingKey] = useState('');

  const handleCreate = async () => {
    if (!name) {
      toast.error('Queue name is required');
      return;
    }

    setLoading(true);
    try {
      const args: Record<string, unknown> = {};
      
      if (type !== 'classic') {
        args['x-queue-type'] = type;
      }
      if (messageTtl) {
        args['x-message-ttl'] = parseInt(messageTtl);
      }
      if (maxLength) {
        args['x-max-length'] = parseInt(maxLength);
      }
      if (deadLetterExchange) {
        args['x-dead-letter-exchange'] = deadLetterExchange;
      }
      if (deadLetterRoutingKey) {
        args['x-dead-letter-routing-key'] = deadLetterRoutingKey;
      }

      await rabbitmqClient.createQueue(vhost, name, {
        durable,
        auto_delete: autoDelete,
        arguments: Object.keys(args).length > 0 ? args : undefined,
      });

      toast.success(`Queue "${name}" created successfully`);
      setOpen(false);
      setName('');
      mutate((key: string) => key.includes('/queues'));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to create queue');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          Add Queue
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>Create Queue</DialogTitle>
          <DialogDescription>
            Create a new queue with the specified configuration
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
                placeholder="my-queue"
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
            <Label>Queue Type</Label>
            <Select value={type} onValueChange={(v) => setType(v as typeof type)}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="classic">Classic</SelectItem>
                <SelectItem value="quorum">Quorum</SelectItem>
                <SelectItem value="stream">Stream</SelectItem>
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

          <div className="border-t pt-4">
            <h4 className="text-sm font-medium mb-3">Arguments (Optional)</h4>
            <div className="space-y-3">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="ttl">Message TTL (ms)</Label>
                  <Input
                    id="ttl"
                    type="number"
                    value={messageTtl}
                    onChange={(e) => setMessageTtl(e.target.value)}
                    placeholder="60000"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="maxLength">Max Length</Label>
                  <Input
                    id="maxLength"
                    type="number"
                    value={maxLength}
                    onChange={(e) => setMaxLength(e.target.value)}
                    placeholder="1000"
                  />
                </div>
              </div>
              <div className="space-y-2">
                <Label htmlFor="dlx">Dead Letter Exchange</Label>
                <Input
                  id="dlx"
                  value={deadLetterExchange}
                  onChange={(e) => setDeadLetterExchange(e.target.value)}
                  placeholder="dlx.exchange"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="dlrk">Dead Letter Routing Key</Label>
                <Input
                  id="dlrk"
                  value={deadLetterRoutingKey}
                  onChange={(e) => setDeadLetterRoutingKey(e.target.value)}
                  placeholder="dlx.routing.key"
                />
              </div>
            </div>
          </div>
        </div>

        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={handleCreate} disabled={loading}>
            {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Create Queue
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function GetMessagesDialog({ queue }: { queue: Queue }) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [messages, setMessages] = useState<QueueMessage[]>([]);
  const [count, setCount] = useState('10');
  const [ackMode, setAckMode] = useState<'ack_requeue_true' | 'ack_requeue_false'>('ack_requeue_true');

  const fetchMessages = async () => {
    setLoading(true);
    try {
      const result = await rabbitmqClient.getMessages(queue.vhost, queue.name, {
        count: parseInt(count),
        ackmode: ackMode,
      });
      setMessages(result);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to get messages');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <DropdownMenuItem onSelect={(e) => e.preventDefault()}>
          <Eye className="mr-2 h-4 w-4" />
          Get Messages
        </DropdownMenuItem>
      </DialogTrigger>
      <DialogContent className="max-w-3xl max-h-[80vh]">
        <DialogHeader>
          <DialogTitle>Messages in {queue.name}</DialogTitle>
          <DialogDescription>
            Retrieve and inspect messages from the queue
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4">
          <div className="flex items-end gap-4">
            <div className="space-y-2">
              <Label>Count</Label>
              <Input
                type="number"
                value={count}
                onChange={(e) => setCount(e.target.value)}
                className="w-24"
                min="1"
                max="50000"
              />
            </div>
            <div className="space-y-2">
              <Label>Ack Mode</Label>
              <Select value={ackMode} onValueChange={(v) => setAckMode(v as typeof ackMode)}>
                <SelectTrigger className="w-48">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="ack_requeue_true">Nack, Requeue</SelectItem>
                  <SelectItem value="ack_requeue_false">Ack (Remove)</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <Button onClick={fetchMessages} disabled={loading}>
              {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Get Messages
            </Button>
          </div>

          <ScrollArea className="h-96 border rounded-md">
            {messages.length === 0 ? (
              <div className="flex flex-col items-center justify-center h-full text-muted-foreground">
                <MessageSquare className="h-12 w-12 mb-2" />
                <p>No messages retrieved</p>
              </div>
            ) : (
              <div className="p-4 space-y-4">
                {messages.map((msg, index) => (
                  <Card key={index}>
                    <CardHeader className="pb-2">
                      <div className="flex items-center justify-between">
                        <CardTitle className="text-sm font-mono">
                          {msg.routing_key || '(no routing key)'}
                        </CardTitle>
                        <Badge variant="outline">
                          {formatBytes(msg.payload_bytes)}
                        </Badge>
                      </div>
                      <div className="flex gap-4 text-xs text-muted-foreground">
                        <span>Exchange: {msg.exchange || '(default)'}</span>
                        {msg.redelivered && (
                          <Badge variant="secondary" className="text-xs">
                            Redelivered
                          </Badge>
                        )}
                      </div>
                    </CardHeader>
                    <CardContent>
                      <pre className="text-xs bg-muted p-3 rounded-md overflow-x-auto whitespace-pre-wrap break-all">
                        {msg.payload_encoding === 'base64'
                          ? atob(msg.payload)
                          : msg.payload}
                      </pre>
                      {msg.properties && Object.keys(msg.properties).length > 0 && (
                        <details className="mt-2">
                          <summary className="text-xs text-muted-foreground cursor-pointer">
                            Properties
                          </summary>
                          <pre className="text-xs bg-muted p-2 rounded-md mt-1 overflow-x-auto">
                            {JSON.stringify(msg.properties, null, 2)}
                          </pre>
                        </details>
                      )}
                    </CardContent>
                  </Card>
                ))}
              </div>
            )}
          </ScrollArea>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function PublishMessageDialog({ queue }: { queue: Queue }) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [payload, setPayload] = useState('');
  const [routingKey, setRoutingKey] = useState(queue.name);
  const [contentType, setContentType] = useState('application/json');

  const handlePublish = async () => {
    if (!payload) {
      toast.error('Message payload is required');
      return;
    }

    setLoading(true);
    try {
      const result = await rabbitmqClient.publishMessage(
        queue.vhost,
        '', // default exchange
        routingKey,
        payload,
        { content_type: contentType }
      );

      if (result.routed) {
        toast.success('Message published successfully');
        setPayload('');
        mutate((key: string) => key.includes('/queues'));
      } else {
        toast.error('Message was not routed to any queue');
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
          <DialogTitle>Publish Message to {queue.name}</DialogTitle>
          <DialogDescription>
            Publish a message directly to this queue via default exchange
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label htmlFor="routingKey">Routing Key</Label>
            <Input
              id="routingKey"
              value={routingKey}
              onChange={(e) => setRoutingKey(e.target.value)}
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
              placeholder='{"message": "Hello, RabbitMQ!"}'
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

function QueueActions({ queue, onDelete }: { queue: Queue; onDelete: () => void }) {
  const [purging, setPurging] = useState(false);

  const handlePurge = async () => {
    setPurging(true);
    try {
      await rabbitmqClient.purgeQueue(queue.vhost, queue.name);
      toast.success(`Queue "${queue.name}" purged`);
      mutate((key: string) => key.includes('/queues'));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to purge queue');
    } finally {
      setPurging(false);
    }
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="icon">
          <MoreHorizontal className="h-4 w-4" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <Link href={`/queues/${encodeURIComponent(queue.vhost)}/${encodeURIComponent(queue.name)}`}>
          <DropdownMenuItem>
            <FileJson className="mr-2 h-4 w-4" />
            View Details
          </DropdownMenuItem>
        </Link>
        <GetMessagesDialog queue={queue} />
        <PublishMessageDialog queue={queue} />
        <DropdownMenuSeparator />
        <DropdownMenuItem onClick={handlePurge} disabled={purging}>
          <RefreshCw className={`mr-2 h-4 w-4 ${purging ? 'animate-spin' : ''}`} />
          Purge Messages
        </DropdownMenuItem>
        <DropdownMenuItem
          onClick={onDelete}
          className="text-destructive focus:text-destructive"
        >
          <Trash2 className="mr-2 h-4 w-4" />
          Delete Queue
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function QueueRow({ queue, onDelete }: { queue: Queue; onDelete: () => void }) {
  const rate = queue.messages_details?.rate || 0;

  return (
    <TableRow>
      <TableCell>
        <div className="flex flex-col">
          <Link
            href={`/queues/${encodeURIComponent(queue.vhost)}/${encodeURIComponent(queue.name)}`}
            className="font-medium hover:underline flex items-center gap-1"
          >
            {queue.name}
            <ChevronRight className="h-3 w-3" />
          </Link>
          <span className="text-xs text-muted-foreground">{queue.vhost}</span>
        </div>
      </TableCell>
      <TableCell>
        <QueueTypeIcon type={queue.type} />
      </TableCell>
      <TableCell>
        <Badge
          variant={queue.state === 'running' ? 'default' : 'secondary'}
          className={queue.state === 'running' ? 'bg-green-500/10 text-green-500 border-green-500/30' : ''}
        >
          {queue.state || 'unknown'}
        </Badge>
      </TableCell>
      <TableCell className="text-right font-mono">
        {formatNumber(queue.messages_ready || 0)}
      </TableCell>
      <TableCell className="text-right font-mono">
        {formatNumber(queue.messages_unacknowledged || 0)}
      </TableCell>
      <TableCell className="text-right font-mono">
        {formatNumber(queue.messages || 0)}
      </TableCell>
      <TableCell className="text-right">
        <span className={rate > 0 ? 'text-green-500' : 'text-muted-foreground'}>
          {rate.toFixed(1)}/s
        </span>
      </TableCell>
      <TableCell className="text-right font-mono">
        {queue.consumers || 0}
      </TableCell>
      <TableCell className="text-right font-mono">
        {formatBytes(queue.memory || 0)}
      </TableCell>
      <TableCell>
        <QueueActions queue={queue} onDelete={onDelete} />
      </TableCell>
    </TableRow>
  );
}

export default function QueuesPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const selectedVHost = useUIStore((state) => state.selectedVHost);
  const setSelectedVHost = useUIStore((state) => state.setSelectedVHost);
  const { data: vhosts } = useVHosts();
  const { data: queues, isLoading, mutate: mutateQueues } = useQueues(
    selectedVHost === 'all' ? undefined : selectedVHost
  );
  const [search, setSearch] = useState('');

  const isConnected = connectionStatus?.connected;
  const vhostList = vhosts?.map((v) => v.name) || ['/'];

  const handleDelete = async (queue: Queue) => {
    if (!confirm(`Are you sure you want to delete queue "${queue.name}"?`)) {
      return;
    }

    try {
      await rabbitmqClient.deleteQueue(queue.vhost, queue.name);
      toast.success(`Queue "${queue.name}" deleted`);
      mutateQueues();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to delete queue');
    }
  };

  const filteredQueues = queues?.filter((q) =>
    q.name.toLowerCase().includes(search.toLowerCase())
  );

  const totalMessages = queues?.reduce((sum, q) => sum + (q.messages || 0), 0) || 0;
  const totalConsumers = queues?.reduce((sum, q) => sum + (q.consumers || 0), 0) || 0;

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to manage queues.
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
            <h1 className="text-2xl font-bold">Queues</h1>
            <p className="text-muted-foreground">
              Manage message queues across your cluster
            </p>
          </div>
          <CreateQueueDialog vhosts={vhostList} />
        </div>

        {/* Stats */}
        <div className="grid gap-4 md:grid-cols-4">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-primary/10 p-2">
                  <Inbox className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{queues?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Total Queues</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-blue-500/10 p-2">
                  <MessageSquare className="h-4 w-4 text-blue-500" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{formatNumber(totalMessages)}</p>
                  <p className="text-xs text-muted-foreground">Total Messages</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-green-500/10 p-2">
                  <Server className="h-4 w-4 text-green-500" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{totalConsumers}</p>
                  <p className="text-xs text-muted-foreground">Total Consumers</p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-yellow-500/10 p-2">
                  <RefreshCw className="h-4 w-4 text-yellow-500" />
                </div>
                <div>
                  <p className="text-2xl font-bold">
                    {queues?.filter((q) => q.type === 'quorum').length || 0}
                  </p>
                  <p className="text-xs text-muted-foreground">Quorum Queues</p>
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
              placeholder="Search queues..."
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
          <Button variant="outline" size="icon" onClick={() => mutateQueues()}>
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
                  <TableHead>State</TableHead>
                  <TableHead className="text-right">Ready</TableHead>
                  <TableHead className="text-right">Unacked</TableHead>
                  <TableHead className="text-right">Total</TableHead>
                  <TableHead className="text-right">Rate</TableHead>
                  <TableHead className="text-right">Consumers</TableHead>
                  <TableHead className="text-right">Memory</TableHead>
                  <TableHead className="w-12"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  Array.from({ length: 5 }).map((_, i) => (
                    <TableRow key={i}>
                      {Array.from({ length: 10 }).map((_, j) => (
                        <TableCell key={j}>
                          <Skeleton className="h-4 w-full" />
                        </TableCell>
                      ))}
                    </TableRow>
                  ))
                ) : filteredQueues?.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={10} className="text-center py-8">
                      <div className="flex flex-col items-center gap-2">
                        <Inbox className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No queues found</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  filteredQueues?.map((queue) => (
                    <QueueRow
                      key={`${queue.vhost}-${queue.name}`}
                      queue={queue}
                      onDelete={() => handleDelete(queue)}
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
