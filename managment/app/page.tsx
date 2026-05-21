'use client';

import { AppShell } from '@/components/app-shell';
import { Dashboard } from '@/components/dashboard';
import { useConnectionStatus } from '@/lib/hooks';
import { useRouter } from 'next/navigation';
import { useEffect } from 'react';

export default function HomePage() {
  const { data: connectionStatus, isLoading } = useConnectionStatus();
  const router = useRouter();

  useEffect(() => {
    if (!isLoading && !connectionStatus?.connected) {
      router.push('/login');
    }
  }, [connectionStatus, isLoading, router]);

  if (isLoading || !connectionStatus?.connected) {
    return (
      <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'hsl(222 47% 11%)' }}>
        <div style={{ color: 'hsl(215 20% 55%)', fontSize: '0.875rem' }}>Loading…</div>
      </div>
    );
  }

  return (
    <AppShell>
      <Dashboard />
    </AppShell>
  );
}
