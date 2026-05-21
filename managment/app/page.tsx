'use client';

import { AppShell } from '@/components/app-shell';
import { Dashboard } from '@/components/dashboard';
import { ConnectDialog } from '@/components/connect-dialog';
import { useConnectionStatus } from '@/lib/hooks';
import { useState, useEffect } from 'react';

export default function HomePage() {
  const { data: connectionStatus, isLoading } = useConnectionStatus();
  const [showConnect, setShowConnect] = useState(false);

  useEffect(() => {
    if (!isLoading && !connectionStatus?.connected) {
      setShowConnect(true);
    }
  }, [connectionStatus, isLoading]);

  return (
    <AppShell>
      <Dashboard />
      <ConnectDialog open={showConnect} onOpenChange={setShowConnect} />
    </AppShell>
  );
}
