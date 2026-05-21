'use client';

import { use, useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useQueue, useQueueBindings, useConnectionStatus, useExchanges } from '@/lib/hooks';
import { rabbitmqClient } from '@/lib/rabbitmq-client';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
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
import { ScrollArea } from '@/components/ui/scroll-area';
import { toast } from 'sonner';
import { mutate } from 'swr';
import {
  ArrowLeft,
  Trash2,
  Link2,
  Plus,
  Send,
  Eye,
  RefreshCw,
  Loader2,
  MessageSquare,
  Server,
} from 'lucide-react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import type { QueueMessage } from '@/types/rabbitmq';

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function formatUptime(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  
  if (days > 0) return `${days}d ${hours % 24}h`;
  if (hours > 0) return `${hours}h ${minutes % 60}m`;
  if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
  return `${seconds}s`;
}

export default function QueueDetailPage({ 
  params 
}: { 
  params: Promise<{ vhost: string; name: string }> 
}) {
  const { vhost, name } = use(params);
  const decodedVhost = decodeURIComponent(vhost);
  const decodedName = decodeURIComponent(name);
  
  const router = useRouter();
  const { data: connectionStatus } = useConnectionStatus();
  const { data: queue, isLoading, mutate: mutateQueue } = useQueue(decodedVhost, decodedName);
  const { data: bindings, mutate: mutateBindings } = useQueueBindings(decodedVhost, decodedName);
  const { data: exchanges } = useExchanges(decodedVhost);

  const [messages, setMessages] = useState<QueueMessage[]>([]);
  const [loadingMessages, setLoadingMessages] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [purging, setPurging] = useState(false);

  const isConnected = connectionStatus?.connected;

  const handleDelete = async () => {
    if (!confirm(`Are you sure you want to delete queue "${decodedName}"?`)) return;
    
    setDeleting(true);
    try {
      await rabbitmqClient.deleteQueue(decodedVhost, decodedName);
      toast.success(`Queue "${decodedName}" deleted`);
      router.push('/queues');
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to delete queue');
    } finally {
      setDeleting(false);
    }
  };

  const handlePurge = async () => {
    if (!confirm('Are you sure you want to purge all messages from this queue?')) return;
    
    setPurging(true);
    try {
      await rabbitmqClient.purgeQueue(decodedVhost, decodedName);
      toast.success('Queue purged');
      mutateQueue();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to purge queue');
    } finally {
      setPurging(false);
    }
  };

  const fetchMessages = async (count: number = 10) => {
    setLoadingMessages(true);
    try {
      const result = await rabbitmqClient.getMessages(decodedVhost, decodedName, {
        count,
        ackmode: 'ack_requeue_true',
      });
      setMessages(result);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to get messages');
    } finally {
      setLoadingMessages(false);
    }
  };

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
        </div>
      </AppShell>
    );
  }

  if (isLoading) {
    return (
      <AppShell>
        <div className="space-y-6">
          <Skeleton className="h-8 w-64" />
          <div className="grid gap-4 md:grid-cols-4">
            {[1, 2, 3, 4].map((i) => (
              <Skeleton key={i} className="h-24" />
            ))}
          </div>
        </div>
      </AppShell>
    );
  }

  if (!queue) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <h2 className="text-xl font-semibold">Queue not found</h2>
          <Link href="/queues">
            <Button variant="outline">
              <ArrowLeft className="mr-2 h-4 w-4" />
              Back to Queues
            </Button>
          </Link>
        </div>
      </AppShell>
    );
  }

  return (
    <AppShell>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-start justify-between">
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <Link href="/queues">
                <Button variant="ghost" size="icon">
                  <ArrowLeft className="h-4 w-4" />
                </Button>
              </Link>
              <h1 className="text-2xl font-bold">{queue.name}</h1>
              <Badge variant="outline">{queue.type}</Badge>
              <Badge
                variant={queue.state === 'running' ? 'default' : 'secondary'}
                className={queue.state === 'running' ? 'bg-green-500/10 text-green-500' : ''}
              >
                {queue.state}
              </Badge>
            </div>
            <p className="text-muted-foreground">Virtual Host: {queue.vhost}</p>
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={() => mutateQueue()}>
              <RefreshCw className="mr-2 h-4 w-4" />
              Refresh
            </Button>
            <Button variant="outline" onClick={handlePurge} disabled={purging}>
              {purging && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Purge
            </Button>
            <Button variant="destructive" onClick={handleDelete} disabled={deleting}>
              {deleting && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              <Trash2 className="mr-2 h-4 w-4" />
              Delete
            </Button>
          </div>
        </div>

        {/* Stats */}
        <div className="grid gap-4 md:grid-cols-4">
          <Card>
            <CardContent className="p-4">
              <p className="text-sm text-muted-foreground">Ready</p>
              <p className="text-2xl font-bold">{queue.messages_ready || 0}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <p className="text-sm text-muted-foreground">Unacknowledged</p>
              <p className="text-2xl font-bold">{queue.messages_unacknowledged || 0}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <p className="text-sm text-muted-foreground">Total Messages</p>
              <p className="text-2xl font-bold">{queue.messages || 0}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <p className="text-sm text-muted-foreground">Consumers</p>
              <p className="text-2xl font-bold">{queue.consumers || 0}</p>
            </CardContent>
          </Card>
        </div>

        {/* Tabs */}
        <Tabs defaultValue="overview">
          <TabsList>
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="messages">Messages</TabsTrigger>
            <TabsTrigger value="bindings">Bindings ({bindings?.length || 0})</TabsTrigger>
            <TabsTrigger value="publish">Publish</TabsTrigger>
          </TabsList>

          <TabsContent value="overview" className="space-y-4">
            <Card>
              <CardHeader>
                <CardTitle className="text-sm">Queue Details</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                  <div>
                    <p className="text-sm text-muted-foreground">Type</p>
                    <p className="font-medium">{queue.type}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Durable</p>
                    <p className="font-medium">{queue.durable ? 'Yes' : 'No'}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Auto-delete</p>
                    <p className="font-medium">{queue.auto_delete ? 'Yes' : 'No'}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Exclusive</p>
                    <p className="font-medium">{queue.exclusive ? 'Yes' : 'No'}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Node</p>
                    <p className="font-medium font-mono text-xs">{queue.node}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Memory</p>
                    <p className="font-medium">{formatBytes(queue.memory || 0)}</p>
                  </div>
                  {queue.policy && (
                    <div>
                      <p className="text-sm text-muted-foreground">Policy</p>
                      <p className="font-medium">{queue.policy}</p>
                    </div>
                  )}
                  {queue.consumer_utilisation !== undefined && (
                    <div>
                      <p className="text-sm text-muted-foreground">Consumer Utilisation</p>
                      <p className="font-medium">{(queue.consumer_utilisation * 100).toFixed(1)}%</p>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>

            {queue.arguments && Object.keys(queue.arguments).length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Arguments</CardTitle>
                </CardHeader>
                <CardContent>
                  <pre className="text-xs bg-muted p-3 rounded-md overflow-x-auto">
                    {JSON.stringify(queue.arguments, null, 2)}
                  </pre>
                </CardContent>
              </Card>
            )}

            {queue.type === 'quorum' && queue.members && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Quorum Members</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    <div>
                      <p className="text-sm text-muted-foreground">Leader</p>
                      <p className="font-mono text-sm">{queue.leader}</p>
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Members</p>
                      <div className="flex flex-wrap gap-2 mt-1">
                        {queue.members.map((member) => (
                          <Badge 
                            key={member} 
                            variant={queue.online?.includes(member) ? 'default' : 'secondary'}
                          >
                            {member}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            )}
          </TabsContent>

          <TabsContent value="messages" className="space-y-4">
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-sm">Get Messages</CardTitle>
                  <div className="flex gap-2">
                    <Button onClick={() => fetchMessages(10)} disabled={loadingMessages}>
                      {loadingMessages && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                      <Eye className="mr-2 h-4 w-4" />
                      Get 10 Messages
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {messages.length === 0 ? (
                  <div className="flex flex-col items-center justify-center py-8 text-muted-foreground">
                    <MessageSquare className="h-12 w-12 mb-2" />
                    <p>No messages retrieved. Click the button above to fetch messages.</p>
                  </div>
                ) : (
                  <ScrollArea className="h-96">
                    <div className="space-y-4">
                      {messages.map((msg, index) => (
                        <Card key={index}>
                          <CardHeader className="pb-2">
                            <div className="flex items-center justify-between">
                              <CardTitle className="text-sm font-mono">
                                {msg.routing_key || '(no routing key)'}
                              </CardTitle>
                              <div className="flex gap-2">
                                <Badge variant="outline">{formatBytes(msg.payload_bytes)}</Badge>
                                {msg.redelivered && <Badge variant="secondary">Redelivered</Badge>}
                              </div>
                            </div>
                          </CardHeader>
                          <CardContent>
                            <pre className="text-xs bg-muted p-3 rounded-md overflow-x-auto whitespace-pre-wrap">
                              {msg.payload_encoding === 'base64' ? atob(msg.payload) : msg.payload}
                            </pre>
                          </CardContent>
                        </Card>
                      ))}
                    </div>
                  </ScrollArea>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="bindings" className="space-y-4">
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-sm">Bindings</CardTitle>
                  <AddBindingDialog 
                    vhost={decodedVhost} 
                    queue={decodedName}
                    exchanges={exchanges || []}
                    onSuccess={() => mutateBindings()}
                  />
                </div>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Source Exchange</TableHead>
                      <TableHead>Routing Key</TableHead>
                      <TableHead>Arguments</TableHead>
                      <TableHead className="w-12"></TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {bindings?.map((binding, index) => (
                      <TableRow key={index}>
                        <TableCell className="font-mono">
                          {binding.source || '(default)'}
                        </TableCell>
                        <TableCell className="font-mono">
                          {binding.routing_key || '(empty)'}
                        </TableCell>
                        <TableCell>
                          {Object.keys(binding.arguments).length > 0 ? (
                            <pre className="text-xs">
                              {JSON.stringify(binding.arguments)}
                            </pre>
                          ) : '-'}
                        </TableCell>
                        <TableCell>
                          {binding.source && (
                            <Button
                              variant="ghost"
                              size="icon"
                              onClick={async () => {
                                try {
                                  await rabbitmqClient.deleteBinding(
                                    decodedVhost,
                                    binding.source,
                                    decodedName,
                                    'queue',
                                    binding.properties_key
                                  );
                                  toast.success('Binding removed');
                                  mutateBindings();
                                } catch (error) {
                                  toast.error(error instanceof Error ? error.message : 'Failed to delete binding');
                                }
                              }}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                    {(!bindings || bindings.length === 0) && (
                      <TableRow>
                        <TableCell colSpan={4} className="text-center py-8 text-muted-foreground">
                          No bindings found
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="publish" className="space-y-4">
            <PublishForm vhost={decodedVhost} queue={decodedName} onSuccess={() => mutateQueue()} />
          </TabsContent>
        </Tabs>
      </div>
    </AppShell>
  );
}

function AddBindingDialog({ 
  vhost, 
  queue, 
  exchanges,
  onSuccess 
}: { 
  vhost: string; 
  queue: string;
  exchanges: { name: string }[];
  onSuccess: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [exchange, setExchange] = useState('');
  const [routingKey, setRoutingKey] = useState('');

  const handleCreate = async () => {
    if (!exchange) {
      toast.error('Please select an exchange');
      return;
    }

    setLoading(true);
    try {
      await rabbitmqClient.createBinding(vhost, exchange, queue, 'queue', {
        routing_key: routingKey,
      });
      toast.success('Binding created');
      setOpen(false);
      setExchange('');
      setRoutingKey('');
      onSuccess();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to create binding');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          Add Binding
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Add Binding</DialogTitle>
          <DialogDescription>
            Bind this queue to an exchange
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label>Source Exchange</Label>
            <Select value={exchange} onValueChange={setExchange}>
              <SelectTrigger>
                <SelectValue placeholder="Select exchange" />
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
            <Label>Routing Key</Label>
            <Input
              value={routingKey}
              onChange={(e) => setRoutingKey(e.target.value)}
              placeholder="routing.key"
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

function PublishForm({ vhost, queue, onSuccess }: { vhost: string; queue: string; onSuccess: () => void }) {
  const [loading, setLoading] = useState(false);
  const [payload, setPayload] = useState('');
  const [routingKey, setRoutingKey] = useState(queue);
  const [contentType, setContentType] = useState('application/json');
  const [deliveryMode, setDeliveryMode] = useState('2');

  const handlePublish = async () => {
    if (!payload) {
      toast.error('Payload is required');
      return;
    }

    setLoading(true);
    try {
      const result = await rabbitmqClient.publishMessage(
        vhost,
        '',
        routingKey,
        payload,
        { 
          content_type: contentType,
          delivery_mode: parseInt(deliveryMode),
        }
      );

      if (result.routed) {
        toast.success('Message published');
        onSuccess();
      } else {
        toast.error('Message was not routed');
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to publish');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Publish Message</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-4 md:grid-cols-3">
          <div className="space-y-2">
            <Label>Routing Key</Label>
            <Input
              value={routingKey}
              onChange={(e) => setRoutingKey(e.target.value)}
            />
          </div>
          <div className="space-y-2">
            <Label>Content Type</Label>
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
            <Label>Delivery Mode</Label>
            <Select value={deliveryMode} onValueChange={setDeliveryMode}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="1">Non-persistent (1)</SelectItem>
                <SelectItem value="2">Persistent (2)</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
        <div className="space-y-2">
          <Label>Payload</Label>
          <Textarea
            value={payload}
            onChange={(e) => setPayload(e.target.value)}
            placeholder='{"message": "Hello, RabbitMQ!"}'
            className="font-mono text-sm h-32"
          />
        </div>
        <Button onClick={handlePublish} disabled={loading}>
          {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
          <Send className="mr-2 h-4 w-4" />
          Publish Message
        </Button>
      </CardContent>
    </Card>
  );
}
