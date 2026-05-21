'use client';

import { ReactNode, useState, useEffect } from 'react';
import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { cn } from '@/lib/utils';
import { useConnectionStatus, useOverview, useVHosts } from '@/lib/hooks';
import { useUIStore } from '@/lib/store';
import { useTheme } from 'next-themes';
import { toast } from 'sonner';

export function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const { data: connectionStatus } = useConnectionStatus();
  const { data: overview } = useOverview();
  const { data: vhosts } = useVHosts();
  const { selectedVHost, setSelectedVHost, refreshInterval, setRefreshInterval } = useUIStore();
  const { theme, setTheme } = useTheme();
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  if (!mounted) {
    return <div className="min-h-screen bg-[#F0F4F7] dark:bg-[#1A202C]" />;
  }

  const isConnected = connectionStatus?.connected;
  const vhostList = vhosts?.map((v) => v.name) || ['/'];

  // Determine active main tab
  const getActiveTab = () => {
    if (pathname === '/') return 'overview';
    if (pathname.startsWith('/connections')) return 'connections';
    if (pathname.startsWith('/channels')) return 'channels';
    if (pathname.startsWith('/exchanges')) return 'exchanges';
    if (pathname.startsWith('/queues')) return 'queues';
    return 'admin';
  };

  const activeTab = getActiveTab();

  const handleLogout = () => {
    document.cookie = 'rabbitmq-config=; path=/; expires=Thu, 01 Jan 1970 00:00:01 GMT;';
    toast.success('Logged out successfully');
    router.push('/login');
  };

  return (
    <div className="min-h-screen bg-[#F0F4F7] dark:bg-[#1A202C] text-[#333333] dark:text-[#E2E8F0] font-sans antialiased text-sm transition-colors duration-200">
      {/* Top Header Bar */}
      <header className="bg-[#4C5B66] dark:bg-[#111827] text-white flex flex-col md:flex-row items-stretch justify-between px-4">
        {/* Brand / Logo */}
        <div className="flex items-center gap-2 py-3">
          <div className="bg-[#FF6600] text-white font-black px-2 py-1 text-base tracking-tighter uppercase rounded-sm">
            RabbitMQ
          </div>
          <span className="text-white font-medium text-base">Management</span>
          {overview && (
            <span className="text-[#A2B1BC] text-xs font-mono ml-2">
              v{overview.rabbitmq_version}
            </span>
          )}
        </div>

        {/* Navigation Tabs */}
        <nav className="flex items-end flex-wrap">
          <Link
            href="/"
            className={cn(
              "px-4 py-2 text-xs font-bold uppercase border-t-2 border-transparent transition-all",
              activeTab === 'overview'
                ? "bg-[#F0F4F7] dark:bg-[#1A202C] text-[#333333] dark:text-[#E2E8F0] border-t-[#FF6600] rounded-t-sm"
                : "text-white hover:bg-[#5C6E7C] dark:hover:bg-[#1F2937]"
            )}
          >
            Overview
          </Link>
          <Link
            href="/connections"
            className={cn(
              "px-4 py-2 text-xs font-bold uppercase border-t-2 border-transparent transition-all",
              activeTab === 'connections'
                ? "bg-[#F0F4F7] dark:bg-[#1A202C] text-[#333333] dark:text-[#E2E8F0] border-t-[#FF6600] rounded-t-sm"
                : "text-white hover:bg-[#5C6E7C] dark:hover:bg-[#1F2937]"
            )}
          >
            Connections
          </Link>
          <Link
            href="/channels"
            className={cn(
              "px-4 py-2 text-xs font-bold uppercase border-t-2 border-transparent transition-all",
              activeTab === 'channels'
                ? "bg-[#F0F4F7] dark:bg-[#1A202C] text-[#333333] dark:text-[#E2E8F0] border-t-[#FF6600] rounded-t-sm"
                : "text-white hover:bg-[#5C6E7C] dark:hover:bg-[#1F2937]"
            )}
          >
            Channels
          </Link>
          <Link
            href="/exchanges"
            className={cn(
              "px-4 py-2 text-xs font-bold uppercase border-t-2 border-transparent transition-all",
              activeTab === 'exchanges'
                ? "bg-[#F0F4F7] dark:bg-[#1A202C] text-[#333333] dark:text-[#E2E8F0] border-t-[#FF6600] rounded-t-sm"
                : "text-white hover:bg-[#5C6E7C] dark:hover:bg-[#1F2937]"
            )}
          >
            Exchanges
          </Link>
          <Link
            href="/queues"
            className={cn(
              "px-4 py-2 text-xs font-bold uppercase border-t-2 border-transparent transition-all",
              activeTab === 'queues'
                ? "bg-[#F0F4F7] dark:bg-[#1A202C] text-[#333333] dark:text-[#E2E8F0] border-t-[#FF6600] rounded-t-sm"
                : "text-white hover:bg-[#5C6E7C] dark:hover:bg-[#1F2937]"
            )}
          >
            Queues
          </Link>
          <Link
            href="/admin"
            className={cn(
              "px-4 py-2 text-xs font-bold uppercase border-t-2 border-transparent transition-all",
              activeTab === 'admin'
                ? "bg-[#F0F4F7] dark:bg-[#1A202C] text-[#333333] dark:text-[#E2E8F0] border-t-[#FF6600] rounded-t-sm"
                : "text-white hover:bg-[#5C6E7C] dark:hover:bg-[#1F2937]"
            )}
          >
            Admin
          </Link>
        </nav>
      </header>

      {/* Secondary Top Control Bar */}
      <section className="bg-[#D5E1E8] dark:bg-[#2D3748] border-b border-[#B1C3CD] dark:border-[#4A5568] px-4 py-2 flex flex-col md:flex-row justify-between items-center text-xs gap-3">
        {/* Left: Virtual Host Switcher */}
        <div className="flex items-center gap-2">
          <span className="font-semibold text-[#4C5B66] dark:text-[#A2B1BC]">Virtual host:</span>
          <select
            value={selectedVHost}
            onChange={(e) => setSelectedVHost(e.target.value)}
            className="bg-white dark:bg-[#1A202C] border border-[#B1C3CD] dark:border-[#4A5568] rounded px-2 py-0.5 font-sans outline-none text-[#333333] dark:text-[#E2E8F0]"
          >
            <option value="all">All virtual hosts</option>
            {vhostList.map((v) => (
              <option key={v} value={v}>
                {v}
              </option>
            ))}
          </select>
        </div>

        {/* Right: Refresh interval & active user */}
        <div className="flex flex-wrap items-center gap-4 text-[#4C5B66] dark:text-[#A2B1BC]">
          {isConnected ? (
            <div className="flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-full bg-[#5BC0BE] border border-[#489997]" title="Connected" />
              <span>
                Connected to: <strong>{overview?.node || 'rocketmq'}</strong>
              </span>
            </div>
          ) : (
            <div className="flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-full bg-[#D9534F] border border-[#B53F3B]" title="Disconnected" />
              <span className="text-[#D9534F] font-bold">Disconnected</span>
            </div>
          )}

          <div>
            Logged in as: <strong className="text-[#333333] dark:text-white">guest</strong>
          </div>

          <div className="flex items-center gap-1.5">
            <span>Update every:</span>
            <select
              value={refreshInterval.toString()}
              onChange={(e) => setRefreshInterval(parseInt(e.target.value))}
              className="bg-white dark:bg-[#1A202C] border border-[#B1C3CD] dark:border-[#4A5568] rounded px-1 py-0.5 outline-none font-sans text-[#333333] dark:text-[#E2E8F0]"
            >
              <option value="0">Manual</option>
              <option value="2000">2 seconds</option>
              <option value="5000">5 seconds</option>
              <option value="10000">10 seconds</option>
              <option value="30000">30 seconds</option>
            </select>
          </div>

          {/* Theme Switcher Button */}
          <button
            onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
            className="bg-[#EAEAEA] dark:bg-[#4A5568] border border-[#CCCCCC] dark:border-[#5A697F] hover:bg-[#DCDCDC] dark:hover:bg-[#5A697F] text-[#333333] dark:text-[#E2E8F0] px-2 py-0.5 rounded font-medium transition-colors"
          >
            {theme === 'dark' ? '☀️ Light' : '🌙 Dark'}
          </button>

          <button
            onClick={handleLogout}
            className="bg-[#EAEAEA] dark:bg-[#4A5568] border border-[#CCCCCC] dark:border-[#5A697F] hover:bg-[#DCDCDC] dark:hover:bg-[#5A697F] text-[#333333] dark:text-[#E2E8F0] px-2 py-0.5 rounded font-medium transition-colors"
          >
            Log Out
          </button>
        </div>
      </section>

      {/* Main Content Area */}
      <main className="max-w-[1400px] mx-auto p-4 md:p-6 min-h-[calc(100vh-80px)]">
        {/* Admin Left-Sidebar / Tabs Split if active tab is admin */}
        {activeTab === 'admin' ? (
          <div className="grid grid-cols-1 md:grid-cols-[200px_1fr] gap-6">
            <aside className="bg-white dark:bg-[#2D3748] border border-[#CCCCCC] dark:border-[#4A5568] p-4 rounded shadow-sm space-y-1">
              <div className="font-semibold text-xs text-[#4C5B66] dark:text-[#A2B1BC] uppercase tracking-wider mb-2 px-2">
                Administration
              </div>
              <Link
                href="/admin"
                className={cn(
                  "block px-3 py-1.5 text-xs font-semibold rounded transition-colors",
                  pathname === '/admin'
                    ? "bg-[#FF6600] text-white"
                    : "text-[#4C5B66] dark:text-[#A2B1BC] hover:bg-[#D5E1E8] dark:hover:bg-[#4A5568]"
                )}
              >
                Overview
              </Link>
              <Link
                href="/users"
                className={cn(
                  "block px-3 py-1.5 text-xs font-semibold rounded transition-colors",
                  pathname === '/users'
                    ? "bg-[#FF6600] text-white"
                    : "text-[#4C5B66] dark:text-[#A2B1BC] hover:bg-[#D5E1E8] dark:hover:bg-[#4A5568]"
                )}
              >
                Users
              </Link>
              <Link
                href="/vhosts"
                className={cn(
                  "block px-3 py-1.5 text-xs font-semibold rounded transition-colors",
                  pathname === '/vhosts'
                    ? "bg-[#FF6600] text-white"
                    : "text-[#4C5B66] dark:text-[#A2B1BC] hover:bg-[#D5E1E8] dark:hover:bg-[#4A5568]"
                )}
              >
                Virtual Hosts
              </Link>
              <Link
                href="/permissions"
                className={cn(
                  "block px-3 py-1.5 text-xs font-semibold rounded transition-colors",
                  pathname === '/permissions'
                    ? "bg-[#FF6600] text-white"
                    : "text-[#4C5B66] dark:text-[#A2B1BC] hover:bg-[#D5E1E8] dark:hover:bg-[#4A5568]"
                )}
              >
                Permissions
              </Link>
              <Link
                href="/policies"
                className={cn(
                  "block px-3 py-1.5 text-xs font-semibold rounded transition-colors",
                  pathname === '/policies'
                    ? "bg-[#FF6600] text-white"
                    : "text-[#4C5B66] dark:text-[#A2B1BC] hover:bg-[#D5E1E8] dark:hover:bg-[#4A5568]"
                )}
              >
                Policies
              </Link>
              <div className="h-px bg-[#B1C3CD] dark:bg-[#4A5568] my-4" />
              <div className="font-semibold text-xs text-[#4C5B66] dark:text-[#A2B1BC] uppercase tracking-wider mb-2 px-2">
                System &amp; Features
              </div>
              <Link
                href="/nodes"
                className={cn(
                  "block px-3 py-1.5 text-xs font-semibold rounded transition-colors",
                  pathname === '/nodes'
                    ? "bg-[#FF6600] text-white"
                    : "text-[#4C5B66] dark:text-[#A2B1BC] hover:bg-[#D5E1E8] dark:hover:bg-[#4A5568]"
                )}
              >
                Nodes
              </Link>
              <Link
                href="/federation"
                className={cn(
                  "block px-3 py-1.5 text-xs font-semibold rounded transition-colors",
                  pathname === '/federation'
                    ? "bg-[#FF6600] text-white"
                    : "text-[#4C5B66] dark:text-[#A2B1BC] hover:bg-[#D5E1E8] dark:hover:bg-[#4A5568]"
                )}
              >
                Federation
              </Link>
              <Link
                href="/shovels"
                className={cn(
                  "block px-3 py-1.5 text-xs font-semibold rounded transition-colors",
                  pathname === '/shovels'
                    ? "bg-[#FF6600] text-white"
                    : "text-[#4C5B66] dark:text-[#A2B1BC] hover:bg-[#D5E1E8] dark:hover:bg-[#4A5568]"
                )}
              >
                Shovels
              </Link>
              <Link
                href="/tracing"
                className={cn(
                  "block px-3 py-1.5 text-xs font-semibold rounded transition-colors",
                  pathname === '/tracing'
                    ? "bg-[#FF6600] text-white"
                    : "text-[#4C5B66] dark:text-[#A2B1BC] hover:bg-[#D5E1E8] dark:hover:bg-[#4A5568]"
                )}
              >
                Tracing
              </Link>
              <Link
                href="/feature-flags"
                className={cn(
                  "block px-3 py-1.5 text-xs font-semibold rounded transition-colors",
                  pathname === '/feature-flags'
                    ? "bg-[#FF6600] text-white"
                    : "text-[#4C5B66] dark:text-[#A2B1BC] hover:bg-[#D5E1E8] dark:hover:bg-[#4A5568]"
                )}
              >
                Feature Flags
              </Link>
              <Link
                href="/streams"
                className={cn(
                  "block px-3 py-1.5 text-xs font-semibold rounded transition-colors",
                  pathname === '/streams'
                    ? "bg-[#FF6600] text-white"
                    : "text-[#4C5B66] dark:text-[#A2B1BC] hover:bg-[#D5E1E8] dark:hover:bg-[#4A5568]"
                )}
              >
                Streams
              </Link>
              <Link
                href="/settings"
                className={cn(
                  "block px-3 py-1.5 text-xs font-semibold rounded transition-colors",
                  pathname === '/settings'
                    ? "bg-[#FF6600] text-white"
                    : "text-[#4C5B66] dark:text-[#A2B1BC] hover:bg-[#D5E1E8] dark:hover:bg-[#4A5568]"
                )}
              >
                Settings
              </Link>
            </aside>
            <div className="bg-white dark:bg-[#2D3748] border border-[#CCCCCC] dark:border-[#4A5568] p-4 rounded shadow-sm">
              {children}
            </div>
          </div>
        ) : (
          <div className="bg-white dark:bg-[#2D3748] border border-[#CCCCCC] dark:border-[#4A5568] p-4 rounded shadow-sm">
            {children}
          </div>
        )}
      </main>
    </div>
  );
}
