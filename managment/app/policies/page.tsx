'use client';

import { useState } from 'react';
import { AppShell } from '@/components/app-shell';
import { usePolicies, useVHosts, useConnectionStatus } from '@/lib/hooks';
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
import { Search, RefreshCw, Server, Plus, Trash2, Loader2, FileText } from 'lucide-react';
import type { Policy } from '@/types/rabbitmq';

function CreatePolicyDialog({ vhosts }: { vhosts: string[] }) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [name, setName] = useState('');
  const [vhost, setVhost] = useState('/');
  const [pattern, setPattern] = useState('.*');
  const [applyTo, setApplyTo] = useState<'all' | 'queues' | 'exchanges'>('all');
  const [priority, setPriority] = useState('0');
  const [definitionJson, setDefinitionJson] = useState('{"ha-mode": "all"}');

  const handleCreate = async () => {
    if (!name || !pattern) {
      toast.error('Name and pattern are required');
      return;
    }

    setLoading(true);
    try {
      let definition = {};
      if (definitionJson) {
        try {
          definition = JSON.parse(definitionJson);
        } catch {
          toast.error('Invalid JSON in definition');
          setLoading(false);
          return;
        }
      }

      await rabbitmqClient.setPolicy(vhost, name, {
        pattern,
        'apply-to': applyTo,
        priority: parseInt(priority) || 0,
        definition,
      });

      toast.success(`Policy "${name}" set successfully`);
      setOpen(false);
      setName('');
      setPattern('.*');
      setDefinitionJson('{"ha-mode": "all"}');
      mutate((key: string) => key.includes('/policies'));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to set policy');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          Add Policy
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Add / Edit Policy</DialogTitle>
          <DialogDescription>
            Configure runtime policies applied automatically matching regular expressions
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
                placeholder="ha-all"
              />
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
          </div>

          <div className="space-y-2">
            <Label htmlFor="pattern">Pattern regex</Label>
            <Input
              id="pattern"
              value={pattern}
              onChange={(e) => setPattern(e.target.value)}
              placeholder=".*"
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>Apply To</Label>
              <Select value={applyTo} onValueChange={(v) => setApplyTo(v as typeof applyTo)}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All</SelectItem>
                  <SelectItem value="queues">Queues</SelectItem>
                  <SelectItem value="exchanges">Exchanges</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="priority">Priority</Label>
              <Input
                id="priority"
                type="number"
                value={priority}
                onChange={(e) => setPriority(e.target.value)}
              />
            </div>
          </div>

          <div className="space-y-2">
            <Label>Definition (JSON)</Label>
            <Textarea
              value={definitionJson}
              onChange={(e) => setDefinitionJson(e.target.value)}
              placeholder='{"ha-mode": "all"}'
              className="font-mono text-sm h-24"
            />
          </div>
        </div>
        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={handleCreate} disabled={loading}>
            {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Set Policy
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

export default function PoliciesPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const { data: policies, isLoading, mutate: mutatePolicies } = usePolicies();
  const { data: vhosts } = useVHosts();
  const [search, setSearch] = useState('');

  const isConnected = connectionStatus?.connected;
  const vhostList = vhosts?.map((v) => v.name) || ['/'];

  const handleDelete = async (vhost: string, name: string) => {
    if (!confirm(`Are you sure you want to delete policy "${name}" on "${vhost}"?`)) {
      return;
    }

    try {
      await rabbitmqClient.deletePolicy(vhost, name);
      toast.success(`Policy "${name}" deleted`);
      mutatePolicies();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to delete policy');
    }
  };

  const filtered = policies?.filter((p) =>
    p.name.toLowerCase().includes(search.toLowerCase()) ||
    p.vhost.toLowerCase().includes(search.toLowerCase())
  );

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to manage policies.
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
            <h1 className="text-2xl font-bold">Runtime Policies</h1>
            <p className="text-muted-foreground">
              Define matching configuration patterns applied dynamically to queues and exchanges
            </p>
          </div>
          <CreatePolicyDialog vhosts={vhostList} />
        </div>

        {/* Stats */}
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-4">
                <div className="rounded-full bg-primary/10 p-2">
                  <FileText className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="text-2xl font-bold">{policies?.length || 0}</p>
                  <p className="text-xs text-muted-foreground">Total Policies</p>
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
              placeholder="Search policies..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-9"
            />
          </div>
          <Button variant="outline" size="icon" onClick={() => mutatePolicies()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Table */}
        <Card>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Virtual Host</TableHead>
                  <TableHead>Name</TableHead>
                  <TableHead>Pattern</TableHead>
                  <TableHead>Apply To</TableHead>
                  <TableHead>Priority</TableHead>
                  <TableHead>Definition</TableHead>
                  <TableHead className="w-12"></TableHead>
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
                ) : !filtered || filtered.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={7} className="text-center py-8">
                      <div className="flex flex-col items-center gap-2">
                        <FileText className="h-8 w-8 text-muted-foreground" />
                        <p className="text-muted-foreground">No runtime policies configured</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  filtered.map((p, idx) => (
                    <TableRow key={`${p.vhost}-${p.name}-${idx}`}>
                      <TableCell className="font-mono text-sm">
                        {p.vhost}
                      </TableCell>
                      <TableCell className="font-mono text-sm font-semibold text-foreground">
                        {p.name}
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        <Badge variant="outline">{p.pattern}</Badge>
                      </TableCell>
                      <TableCell className="capitalize text-xs">
                        {p['apply-to'] || 'all'}
                      </TableCell>
                      <TableCell className="font-mono text-sm text-center">
                        {p.priority || 0}
                      </TableCell>
                      <TableCell>
                        <pre className="text-xs bg-muted/50 p-2 rounded max-w-sm truncate overflow-x-auto">
                          {JSON.stringify(p.definition)}
                        </pre>
                      </TableCell>
                      <TableCell>
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleDelete(p.vhost, p.name)}
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
