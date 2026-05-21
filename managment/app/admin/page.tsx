'use client';

import { AppShell } from '@/components/app-shell';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { useConnectionStatus } from '@/lib/hooks';
import { Server, Users, Shield, Home, FileText, Database, ArrowRight } from 'lucide-react';
import Link from 'next/link';

export default function AdminPage() {
  const { data: connectionStatus } = useConnectionStatus();

  const isConnected = connectionStatus?.connected;

  if (!isConnected) {
    return (
      <AppShell>
        <div className="flex flex-col items-center justify-center h-[60vh] space-y-4">
          <Server className="h-16 w-16 text-muted-foreground" />
          <h2 className="text-xl font-semibold">Not Connected</h2>
          <p className="text-muted-foreground">
            Connect to a RabbitMQ server to access admin panel.
          </p>
        </div>
      </AppShell>
    );
  }

  const sections = [
    {
      title: 'Users',
      desc: 'Add, update, or remove users and configure administrative role tags.',
      icon: <Users className="h-6 w-6 text-blue-500" />,
      href: '/users',
    },
    {
      title: 'Virtual Hosts',
      desc: 'Create logical namespaces to isolate resources, exchanges, and queues.',
      icon: <Home className="h-6 w-6 text-green-500" />,
      href: '/vhosts',
    },
    {
      title: 'Permissions',
      desc: 'Define resource configuration, write, and read regex filters for users.',
      icon: <Shield className="h-6 w-6 text-purple-500" />,
      href: '/permissions',
    },
    {
      title: 'Policies',
      desc: 'Configure HA, dead lettering, and queue properties automatically using regex matching.',
      icon: <FileText className="h-6 w-6 text-yellow-500" />,
      href: '/policies',
    },
  ];

  return (
    <AppShell>
      <div className="space-y-6">
        {/* Header */}
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Database className="h-7 w-7 text-primary" />
            Broker Administration
          </h1>
          <p className="text-muted-foreground">
            Manage cluster security policies, user credentials, namespaces, and runtime policies
          </p>
        </div>

        {/* Sections Grid */}
        <div className="grid gap-6 md:grid-cols-2">
          {sections.map((section, idx) => (
            <Card key={idx} className="hover:shadow-md transition-shadow">
              <CardHeader className="flex flex-row items-center gap-4 pb-2">
                <div className="rounded-lg bg-muted p-3">
                  {section.icon}
                </div>
                <CardTitle className="text-lg font-semibold">{section.title}</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <p className="text-sm text-muted-foreground">{section.desc}</p>
                <Link href={section.href}>
                  <Button variant="outline" className="w-full justify-between">
                    Open Settings
                    <ArrowRight className="h-4 w-4" />
                  </Button>
                </Link>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    </AppShell>
  );
}
