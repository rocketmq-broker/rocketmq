'use client';

import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Loader2, AlertCircle } from 'lucide-react';
import { useConnectionStore } from '@/lib/store';
import { mutate } from 'swr';

interface ConnectDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function ConnectDialog({ open, onOpenChange }: ConnectDialogProps) {
  const [url, setUrl] = useState('http://localhost:15672');
  const [username, setUsername] = useState('guest');
  const [password, setPassword] = useState('guest');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  const { setConfig, setConnected, setConnecting } = useConnectionStore();

  const handleConnect = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setConnecting(true);

    try {
      const response = await fetch('/api/rabbitmq/connect', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url, username, password }),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.reason || data.error || 'Connection failed');
      }

      setConfig({ url, username, password });
      setConnected(true);
      
      // Revalidate all SWR caches
      mutate(() => true);
      
      onOpenChange(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to connect');
      setConnected(false);
    } finally {
      setLoading(false);
      setConnecting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Connect to RabbitMQ</DialogTitle>
          <DialogDescription>
            Enter your RabbitMQ Management API credentials
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={handleConnect} className="space-y-4">
          {error && (
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
          
          <div className="space-y-2">
            <Label htmlFor="url">Management API URL</Label>
            <Input
              id="url"
              type="url"
              placeholder="http://localhost:15672"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              required
            />
            <p className="text-xs text-muted-foreground">
              The URL of the RabbitMQ Management HTTP API (usually port 15672)
            </p>
          </div>

          <div className="space-y-2">
            <Label htmlFor="username">Username</Label>
            <Input
              id="username"
              type="text"
              placeholder="guest"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              required
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="password">Password</Label>
            <Input
              id="password"
              type="password"
              placeholder="guest"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
          </div>

          <div className="flex justify-end gap-2">
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
            >
              Cancel
            </Button>
            <Button type="submit" disabled={loading}>
              {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Connect
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}
