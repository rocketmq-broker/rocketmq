'use client';

import { ReactNode, useState, useEffect } from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { cn } from '@/lib/utils';
import { useConnectionStatus, useOverview } from '@/lib/hooks';
import { useUIStore } from '@/lib/store';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  LayoutDashboard,
  Inbox,
  ArrowLeftRight,
  Link2,
  PlugZap,
  Layers,
  Server,
  Users,
  Shield,
  FileText,
  Network,
  Shovel,
  Bug,
  Flag,
  HardDrive,
  Settings,
  ChevronLeft,
  ChevronRight,
  RefreshCw,
  Database,
  Activity,
  Blocks,
} from 'lucide-react';

interface NavItem {
  title: string;
  href: string;
  icon: ReactNode;
  badge?: string;
}

const mainNavItems: NavItem[] = [
  { title: 'Dashboard', href: '/', icon: <LayoutDashboard className="h-4 w-4" /> },
  { title: 'Queues', href: '/queues', icon: <Inbox className="h-4 w-4" /> },
  { title: 'Exchanges', href: '/exchanges', icon: <ArrowLeftRight className="h-4 w-4" /> },
  { title: 'Bindings', href: '/bindings', icon: <Link2 className="h-4 w-4" /> },
];

const connectivityNavItems: NavItem[] = [
  { title: 'Connections', href: '/connections', icon: <PlugZap className="h-4 w-4" /> },
  { title: 'Channels', href: '/channels', icon: <Layers className="h-4 w-4" /> },
  { title: 'Consumers', href: '/consumers', icon: <Activity className="h-4 w-4" /> },
];

const adminNavItems: NavItem[] = [
  { title: 'Virtual Hosts', href: '/vhosts', icon: <Server className="h-4 w-4" /> },
  { title: 'Users', href: '/users', icon: <Users className="h-4 w-4" /> },
  { title: 'Permissions', href: '/permissions', icon: <Shield className="h-4 w-4" /> },
  { title: 'Policies', href: '/policies', icon: <FileText className="h-4 w-4" /> },
];

const advancedNavItems: NavItem[] = [
  { title: 'Federation', href: '/federation', icon: <Network className="h-4 w-4" /> },
  { title: 'Shovels', href: '/shovels', icon: <Shovel className="h-4 w-4" /> },
  { title: 'Tracing', href: '/tracing', icon: <Bug className="h-4 w-4" /> },
  { title: 'Feature Flags', href: '/feature-flags', icon: <Flag className="h-4 w-4" /> },
];

const systemNavItems: NavItem[] = [
  { title: 'Nodes', href: '/nodes', icon: <HardDrive className="h-4 w-4" /> },
  { title: 'Streams', href: '/streams', icon: <Blocks className="h-4 w-4" /> },
  { title: 'Admin', href: '/admin', icon: <Database className="h-4 w-4" /> },
  { title: 'Settings', href: '/settings', icon: <Settings className="h-4 w-4" /> },
];

function NavSection({ 
  title, 
  items, 
  collapsed 
}: { 
  title: string; 
  items: NavItem[]; 
  collapsed: boolean;
}) {
  const pathname = usePathname();

  return (
    <div className="mb-4">
      {!collapsed && (
        <h3 className="mb-2 px-3 text-xs font-semibold text-muted-foreground uppercase tracking-wider">
          {title}
        </h3>
      )}
      <nav className="space-y-1">
        {items.map((item) => {
          const isActive = pathname === item.href || 
            (item.href !== '/' && pathname.startsWith(item.href));
          
          return collapsed ? (
            <Tooltip key={item.href} delayDuration={0}>
              <TooltipTrigger asChild>
                <Link
                  href={item.href}
                  className={cn(
                    'flex items-center justify-center rounded-md px-3 py-2 text-sm font-medium transition-colors',
                    isActive
                      ? 'bg-primary/10 text-primary'
                      : 'text-muted-foreground hover:bg-muted hover:text-foreground'
                  )}
                >
                  {item.icon}
                </Link>
              </TooltipTrigger>
              <TooltipContent side="right">
                {item.title}
                {item.badge && (
                  <Badge variant="secondary" className="ml-2">
                    {item.badge}
                  </Badge>
                )}
              </TooltipContent>
            </Tooltip>
          ) : (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                'flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors',
                isActive
                  ? 'bg-primary/10 text-primary'
                  : 'text-muted-foreground hover:bg-muted hover:text-foreground'
              )}
            >
              {item.icon}
              <span className="flex-1">{item.title}</span>
              {item.badge && (
                <Badge variant="secondary" className="ml-auto">
                  {item.badge}
                </Badge>
              )}
            </Link>
          );
        })}
      </nav>
    </div>
  );
}

export function AppShell({ children }: { children: ReactNode }) {
  const { data: connectionStatus } = useConnectionStatus();
  const { data: overview } = useOverview();
  const { sidebarCollapsed, setSidebarCollapsed, refreshInterval, setRefreshInterval } = useUIStore();
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  if (!mounted) {
    return <div className="min-h-screen bg-background" />;
  }

  const isConnected = connectionStatus?.connected;

  return (
    <TooltipProvider>
      <div className="flex min-h-screen bg-background">
        {/* Sidebar */}
        <aside
          className={cn(
            'flex flex-col border-r border-border bg-sidebar transition-all duration-300',
            sidebarCollapsed ? 'w-16' : 'w-64'
          )}
        >
          {/* Logo */}
          <div className="flex h-14 items-center border-b border-border px-4">
            {sidebarCollapsed ? (
              <div className="mx-auto flex h-8 w-8 items-center justify-center rounded-md bg-primary text-primary-foreground font-bold">
                R
              </div>
            ) : (
              <div className="flex items-center gap-2">
                <div className="flex h-8 w-8 items-center justify-center rounded-md bg-primary text-primary-foreground font-bold">
                  R
                </div>
                <span className="font-semibold text-foreground">RabbitMQ</span>
              </div>
            )}
          </div>

          {/* Connection Status */}
          {!sidebarCollapsed && (
            <div className="border-b border-border p-4">
              <div className="flex items-center gap-2 text-xs">
                <div
                  className={cn(
                    'h-2 w-2 rounded-full',
                    isConnected ? 'bg-green-500' : 'bg-red-500'
                  )}
                />
                <span className="text-muted-foreground">
                  {isConnected ? 'Connected' : 'Disconnected'}
                </span>
              </div>
              {isConnected && overview && (
                <div className="mt-2 text-xs text-muted-foreground truncate">
                  {overview.cluster_name || overview.node}
                </div>
              )}
            </div>
          )}

          {/* Navigation */}
          <ScrollArea className="flex-1 px-2 py-4">
            <NavSection title="Overview" items={mainNavItems} collapsed={sidebarCollapsed} />
            <NavSection title="Connectivity" items={connectivityNavItems} collapsed={sidebarCollapsed} />
            <NavSection title="Administration" items={adminNavItems} collapsed={sidebarCollapsed} />
            <NavSection title="Advanced" items={advancedNavItems} collapsed={sidebarCollapsed} />
            <NavSection title="System" items={systemNavItems} collapsed={sidebarCollapsed} />
          </ScrollArea>

          {/* Collapse Toggle */}
          <div className="border-t border-border p-2">
            <Button
              variant="ghost"
              size="sm"
              className="w-full justify-center"
              onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
            >
              {sidebarCollapsed ? (
                <ChevronRight className="h-4 w-4" />
              ) : (
                <ChevronLeft className="h-4 w-4" />
              )}
            </Button>
          </div>
        </aside>

        {/* Main Content */}
        <div className="flex flex-1 flex-col overflow-hidden">
          {/* Top Bar */}
          <header className="flex h-14 items-center justify-between border-b border-border bg-card px-6">
            <div className="flex items-center gap-4">
              {overview && (
                <Badge variant="outline" className="text-xs">
                  v{overview.rabbitmq_version}
                </Badge>
              )}
            </div>
            <div className="flex items-center gap-4">
              <div className="flex items-center gap-2">
                <RefreshCw className="h-4 w-4 text-muted-foreground" />
                <Select
                  value={refreshInterval.toString()}
                  onValueChange={(val) => setRefreshInterval(parseInt(val))}
                >
                  <SelectTrigger className="w-32 h-8 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="0">Manual</SelectItem>
                    <SelectItem value="2000">2 seconds</SelectItem>
                    <SelectItem value="5000">5 seconds</SelectItem>
                    <SelectItem value="10000">10 seconds</SelectItem>
                    <SelectItem value="30000">30 seconds</SelectItem>
                    <SelectItem value="60000">1 minute</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </header>

          {/* Page Content */}
          <main className="flex-1 overflow-auto p-6">
            {children}
          </main>
        </div>
      </div>
    </TooltipProvider>
  );
}
