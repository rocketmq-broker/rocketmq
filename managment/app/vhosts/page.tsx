'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useVHosts, useConnectionStatus } from '@/lib/hooks';
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
import { Label } from '@/components/ui/label';
import { toast } from 'sonner';
import { mutate } from 'swr';
import { Search, RefreshCw, Server, Plus, Trash2, Loader2, Home } from 'lucide-react';
import type { VHost } from '@/types/rabbitmq';

function CreateVHostDialog() {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [name, setName] = useState('');

  const handleCreate = async () => {
    if (!name) {
      toast.error('Virtual host name is required');
      return;
    }

    setLoading(true);
    try {
      await rabbitmqClient.createVHost(name);
      toast.success(`Virtual host "${name}" created successfully`);
      setOpen(false);
      setName('');
      mutate((key: string) => key.includes('/vhosts'));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to create virtual host');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          Add Virtual Host
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Create Virtual Host</DialogTitle>
          <DialogDescription>
            Add a new namespace to isolate queues and exchanges
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label htmlFor="name">Name</Label>
            <Input
              id="name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. /production"
            />
          </div>
        </div>
        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={handleCreate} disabled={loading}>
            {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Create
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

export default function VHostsPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const { data: vhosts, isLoading, mutate: mutateVHosts } = useVHosts();
  const [search, setSearch] = useState('');

  const isConnected = connectionStatus?.connected;

  const handleDelete = async (name: string) => {
    if (name === '/') {
      toast.error('Cannot delete the default virtual host "/"');
      return;
    }

    if (!confirm(`Are you sure you want to delete virtual host "${name}"? This deletes all exchanges, queues, and messages inside it.`)) {
      return;
    }

    try {
      await rabbitmqClient.deleteVHost(name);
      toast.success(`Virtual host "${name}" deleted`);
      mutateVHosts();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to delete virtual host');
    }
  };

  const filtered = vhosts?.filter((v) =>
    v.name.toLowerCase().includes(search.toLowerCase())
  );

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to manage virtual hosts.
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
            <h1 className="text-2xl font-bold">Virtual Hosts</h1>
            <p className="text-muted-foreground">
              Manage logical grouping and isolation namespaces
            </p>
          </div>
          <CreateVHostDialog />
        </div>

        {/* Stats */}
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-primary/10 p-2">
                  <Home className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{vhosts?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Total Virtual Hosts</p>
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
              placeholder="Search virtual hosts..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-9"
            />
          </div>
          <Button variant="outline" size="icon" onClick={() => mutateVHosts()}>
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
                  <TableHead>Features</TableHead>
                  <TableHead className="w-12"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  Array.from({ length: 2 }).map((_, i) => (
                    <TableRow key={i}>
                      <TableCell><Skeleton className="h-4 w-full" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-full" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-full" /></TableCell>
                    </TableRow>
                  ))
                ) : !filtered || filtered.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={3} className="text-center py-8">
                      <div className="flex flex-col items-center gap-2">
                        <Home className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No virtual hosts found</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  filtered.map((vh) => (
                    <TableRow key={vh.name}>
                      <TableCell className="font-mono text-sm font-semibold text-foreground">
                        {vh.name}
                      </TableCell>
                      <TableCell>
                        {vh.name === '/' && (
                          <Badge variant="outline" className="text-xs bg-green-500/10 text-green-500 border-green-500/30">
                            Default
                          </Badge>
                        )}
                      </TableCell>
                      <TableCell>
                        {vh.name !== '/' && (
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => handleDelete(vh.name)}
                          >
                            <Trash2 className="h-4 w-4 text-destructive" />
                          </Button>
                        )}
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
