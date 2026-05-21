'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { usePermissions, useVHosts, useUsers, useConnectionStatus } from '@/lib/hooks';
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
import { toast } from 'sonner';
import { mutate } from 'swr';
import { Search, RefreshCw, Server, Plus, Trash2, Loader2, Shield } from 'lucide-react';
import type { Permission } from '@/types/rabbitmq';

function SetPermissionDialog({ vhosts, users }: { vhosts: string[]; users: string[] }) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [user, setUser] = useState('');
  const [vhost, setVhost] = useState('/');
  const [configure, setConfigure] = useState('.*');
  const [write, setWrite] = useState('.*');
  const [read, setRead] = useState('.*');

  const handleSet = async () => {
    if (!user || !vhost) {
      toast.error('User and virtual host are required');
      return;
    }

    setLoading(true);
    try {
      await rabbitmqClient.setPermission(vhost, user, {
        configure,
        write,
        read,
      });
      toast.success(`Permissions set successfully for user "${user}" on "${vhost}"`);
      setOpen(false);
      setUser('');
      setConfigure('.*');
      setWrite('.*');
      setRead('.*');
      mutate((key: string) => key.includes('/permissions'));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to set permissions');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          Set Permission
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Set User Permissions</DialogTitle>
          <DialogDescription>
            Configure virtual host access levels using regular expressions
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label>User</Label>
            <Select value={user} onValueChange={setUser}>
              <SelectTrigger>
                <SelectValue placeholder="Select user" />
              </SelectTrigger>
              <SelectContent>
                {users.map((u) => (
                  <SelectItem key={u} value={u}>
                    {u}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label>Virtual Host</Label>
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

          <div className="space-y-2">
            <Label htmlFor="configure">Configure regex</Label>
            <Input
              id="configure"
              value={configure}
              onChange={(e) => setConfigure(e.target.value)}
              placeholder=".*"
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="write">Write regex</Label>
            <Input
              id="write"
              value={write}
              onChange={(e) => setWrite(e.target.value)}
              placeholder=".*"
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="read">Read regex</Label>
            <Input
              id="read"
              value={read}
              onChange={(e) => setRead(e.target.value)}
              placeholder=".*"
            />
          </div>
        </div>
        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={handleSet} disabled={loading}>
            {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Set Permission
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

export default function PermissionsPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const { data: permissions, isLoading, mutate: mutatePermissions } = usePermissions();
  const { data: vhosts } = useVHosts();
  const { data: users } = useUsers();
  const [search, setSearch] = useState('');

  const isConnected = connectionStatus?.connected;
  const vhostList = vhosts?.map((v) => v.name) || ['/'];
  const userList = users?.map((u) => u.name) || ['guest'];

  const handleDelete = async (vhost: string, user: string) => {
    if (!confirm(`Are you sure you want to revoke permissions for user "${user}" on "${vhost}"?`)) {
      return;
    }

    try {
      await rabbitmqClient.deletePermission(vhost, user);
      toast.success(`Revoked permissions for "${user}" on "${vhost}"`);
      mutatePermissions();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to revoke permissions');
    }
  };

  const filtered = permissions?.filter((p) =>
    p.user.toLowerCase().includes(search.toLowerCase()) ||
    p.vhost.toLowerCase().includes(search.toLowerCase())
  );

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to manage permissions.
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
            <h1 className="text-2xl font-bold">Permissions</h1>
            <p className="text-muted-foreground">
              Manage resource configuration, read, and write permissions scoped to Virtual Hosts
            </p>
          </div>
          <SetPermissionDialog vhosts={vhostList} users={userList} />
        </div>

        {/* Stats */}
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-primary/10 p-2">
                  <Shield className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{permissions?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Active Permission Rules</p>
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
              placeholder="Search permissions..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-9"
            />
          </div>
          <Button variant="outline" size="icon" onClick={() => mutatePermissions()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Table */}
        <Card>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>User</TableHead>
                  <TableHead>Virtual Host</TableHead>
                  <TableHead>Configure Regex</TableHead>
                  <TableHead>Write Regex</TableHead>
                  <TableHead>Read Regex</TableHead>
                  <TableHead className="w-12"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  Array.from({ length: 2 }).map((_, i) => (
                    <TableRow key={i}>
                      {Array.from({ length: 6 }).map((_, j) => (
                        <TableCell key={j}><Skeleton className="h-4 w-full" /></TableCell>
                      ))}
                    </TableRow>
                  ))
                ) : !filtered || filtered.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center py-8">
                      <div className="flex flex-col items-center gap-2">
                        <Shield className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No permissions configured</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  filtered.map((p, idx) => (
                    <TableRow key={`${p.vhost}-${p.user}-${idx}`}>
                      <TableCell className="font-mono text-sm font-semibold text-foreground">
                        {p.user}
                      </TableCell>
                      <TableCell className="font-mono text-sm">
                        {p.vhost}
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        <Badge variant="outline">{p.configure || '.*'}</Badge>
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        <Badge variant="outline">{p.write || '.*'}</Badge>
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        <Badge variant="outline">{p.read || '.*'}</Badge>
                      </TableCell>
                      <TableCell>
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleDelete(p.vhost, p.user)}
                        >
                          <Trash2 className="h-4 w-4 text-destructive" />
                        </Button>
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
