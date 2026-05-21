'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { useUsers, useConnectionStatus } from '@/lib/hooks';
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
import { Search, RefreshCw, Server, Plus, Trash2, Loader2, Users } from 'lucide-react';
import type { User } from '@/types/rabbitmq';

function CreateUserDialog() {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [name, setName] = useState('');
  const [password, setPassword] = useState('');
  const [tags, setTags] = useState('administrator');

  const handleCreate = async () => {
    if (!name || !password) {
      toast.error('Username and password are required');
      return;
    }

    setLoading(true);
    try {
      await rabbitmqClient.createUser(name, {
        password,
        tags,
      });
      toast.success(`User "${name}" created successfully`);
      setOpen(false);
      setName('');
      setPassword('');
      mutate((key: string) => key.includes('/users'));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to create user');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          Add User
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Create User</DialogTitle>
          <DialogDescription>
            Add a new user credential to access the broker
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label htmlFor="name">Username</Label>
            <Input
              id="name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. app-service"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="password">Password</Label>
            <Input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Enter password"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="tags">Tags (Roles)</Label>
            <Input
              id="tags"
              value={tags}
              onChange={(e) => setTags(e.target.value)}
              placeholder="e.g. administrator, monitoring"
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

export default function UsersPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const { data: users, isLoading, mutate: mutateUsers } = useUsers();
  const [search, setSearch] = useState('');

  const isConnected = connectionStatus?.connected;

  const handleDelete = async (name: string) => {
    if (name === 'guest') {
      toast.error('Cannot delete the default guest user');
      return;
    }

    if (!confirm(`Are you sure you want to delete user "${name}"?`)) {
      return;
    }

    try {
      await rabbitmqClient.deleteUser(name);
      toast.success(`User "${name}" deleted`);
      mutateUsers();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to delete user');
    }
  };

  const filtered = users?.filter((u) =>
    u.name.toLowerCase().includes(search.toLowerCase())
  );

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to manage users.
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
            <h1 className="text-2xl font-bold">Users</h1>
            <p className="text-muted-foreground">
              Configure user authentication credentials and system role tags
            </p>
          </div>
          <CreateUserDialog />
        </div>

        {/* Stats */}
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-primary/10 p-2">
                  <Users className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{users?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Total Users</p>
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
              placeholder="Search users..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-9"
            />
          </div>
          <Button variant="outline" size="icon" onClick={() => mutateUsers()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Table */}
        <Card>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Username</TableHead>
                  <TableHead>Tags</TableHead>
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
                        <Users className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No users found</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  filtered.map((u) => (
                    <TableRow key={u.name}>
                      <TableCell className="font-mono text-sm font-semibold text-foreground">
                        {u.name}
                      </TableCell>
                      <TableCell>
                        {u.tags.split(',').map((tag) => (
                          <Badge key={tag} variant="outline" className="mr-1 text-xs bg-primary/10 text-primary border-primary/30">
                            {tag.trim()}
                          </Badge>
                        ))}
                      </TableCell>
                      <TableCell>
                        {u.name !== 'guest' && (
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => handleDelete(u.name)}
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
