'use client';

import { AppShell } from '@/components/app-shell';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useConnectionStatus } from '@/lib/hooks';
import { Server, Settings, LogOut, RefreshCw, Moon } from 'lucide-react';
import { useRouter } from 'next/navigation';
import { toast } from 'sonner';

export default function SettingsPage() {
  const { data: connectionStatus } = useConnectionStatus();
  const router = useRouter();

  const isConnected = connectionStatus?.connected;

  const handleLogout = async () => {
    // Clear connection cookie
    document.cookie = 'rabbitmq-config=; path=/; expires=Thu, 01 Jan 1970 00:00:01 GMT;';
    toast.success('Logged out successfully');
    router.push('/login');
  };

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to view settings.
          </p>
        </div>
      </AppShell>
    );
  }

  return (
    <AppShell>
      <div className="space-y-6">
        {/* Header */}
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Settings className="h-7 w-7 text-primary" />
            Interface Settings
          </h1>
          <p className="text-muted-foreground">
            Customize dashboard behavior, automatic polling frequency, and session preferences
          </p>
        </div>

        <div className="grid gap-6 max-w-2xl">
          {/* Preferences Card */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Dashboard Preferences</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex items-center justify-between">
                <div className="space-y-0.5">
                  <Label>Auto-refresh Interval</Label>
                  <p className="text-xs text-muted-foreground">Adjust statistical polling background speed</p>
                </div>
                <Select defaultValue="5">
                  <SelectTrigger className="w-28">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="2">2 seconds</SelectItem>
                    <SelectItem value="5">5 seconds</SelectItem>
                    <SelectItem value="10">10 seconds</SelectItem>
                    <SelectItem value="30">30 seconds</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="flex items-center justify-between border-t pt-4">
                <div className="space-y-0.5">
                  <Label className="flex items-center gap-2">
                    <Moon className="h-4 w-4" />
                    Dark Mode Theme
                  </Label>
                  <p className="text-xs text-muted-foreground">Enforce sleek midnight premium aesthetics</p>
                </div>
                <Switch checked disabled />
              </div>
            </CardContent>
          </Card>

          {/* Session Card */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg text-destructive">Connection Session</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex items-center justify-between">
                <div className="space-y-0.5">
                  <Label>Broker Link</Label>
                  <p className="text-xs text-muted-foreground">Connected to http://localhost:15672</p>
                </div>
                <Button variant="destructive" onClick={handleLogout} className="flex items-center gap-2">
                  <LogOut className="h-4 w-4" />
                  Disconnect
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </AppShell>
  );
}
