'use client';

import { useOverview, useNodes, useConnectionStatus } from '@/lib/hooks';

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

export function Dashboard() {
  const { data: overview, isLoading: overviewLoading } = useOverview();
  const { data: nodes, isLoading: nodesLoading } = useNodes();
  const { data: connectionStatus } = useConnectionStatus();

  const isConnected = connectionStatus?.connected;

  const totalReady = overview?.queue_totals?.messages_ready || 0;
  const totalUnacked = overview?.queue_totals?.messages_unacknowledged || 0;
  const totalMessages = overview?.queue_totals?.messages || 0;

  const publishRate = overview?.message_stats?.publish_details?.rate || 0;
  const deliverRate = overview?.message_stats?.deliver_get_details?.rate || 0;
  const ackRate = overview?.message_stats?.ack_details?.rate || 0;

  return (
    <div className="space-y-6">
      {/* Overview Title */}
      <div>
        <h1 className="text-xl font-bold text-[#4C5B66] dark:text-[#E2E8F0] border-b border-[#CCCCCC] dark:border-[#4A5568] pb-1">
          Overview
        </h1>
      </div>

      {/* Main Totals / Rates Grid */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Queued Messages Totals */}
        <div className="bg-[#F6F6F6] dark:bg-[#1E293B] border border-[#CCCCCC] dark:border-[#4A5568] p-4 rounded">
          <h2 className="font-bold text-xs text-[#4C5B66] dark:text-[#A2B1BC] uppercase tracking-wider mb-3 pb-1 border-b border-[#EAEAEA] dark:border-[#4A5568]">
            Queued Messages
          </h2>
          <div className="space-y-2">
            <div className="flex justify-between items-center text-xs">
              <span className="text-gray-600 dark:text-[#94A3B8]">Ready:</span>
              <span className="font-mono font-bold text-[#5CB85C] bg-[#5CB85C]/10 px-2 py-0.5 rounded border border-[#5CB85C]/20">
                {totalReady}
              </span>
            </div>
            <div className="flex justify-between items-center text-xs">
              <span className="text-gray-600 dark:text-[#94A3B8]">Unacknowledged:</span>
              <span className="font-mono font-bold text-[#F0AD4E] bg-[#F0AD4E]/10 px-2 py-0.5 rounded border border-[#F0AD4E]/20">
                {totalUnacked}
              </span>
            </div>
            <div className="flex justify-between items-center text-xs pt-1.5 border-t border-[#EAEAEA] dark:border-[#4A5568]">
              <span className="font-bold text-gray-800 dark:text-[#E2E8F0]">Total:</span>
              <span className="font-mono font-bold text-[#D9534F] bg-[#D9534F]/10 px-2 py-0.5 rounded border border-[#D9534F]/20">
                {totalMessages}
              </span>
            </div>
          </div>
        </div>

        {/* Message Rates */}
        <div className="bg-[#F6F6F6] dark:bg-[#1E293B] border border-[#CCCCCC] dark:border-[#4A5568] p-4 rounded">
          <h2 className="font-bold text-xs text-[#4C5B66] dark:text-[#A2B1BC] uppercase tracking-wider mb-3 pb-1 border-b border-[#EAEAEA] dark:border-[#4A5568]">
            Message Rates
          </h2>
          <div className="space-y-2">
            <div className="flex justify-between items-center text-xs">
              <span className="text-gray-600 dark:text-[#94A3B8]">Publish:</span>
              <span className="font-mono font-bold text-blue-600 bg-blue-50 dark:bg-blue-950/30 px-2 py-0.5 rounded border border-blue-100 dark:border-blue-900/50">
                {publishRate.toFixed(1)}/s
              </span>
            </div>
            <div className="flex justify-between items-center text-xs">
              <span className="text-gray-600 dark:text-[#94A3B8]">Deliver / Get:</span>
              <span className="font-mono font-bold text-purple-600 bg-purple-50 dark:bg-purple-950/30 px-2 py-0.5 rounded border border-purple-100 dark:border-purple-900/50">
                {deliverRate.toFixed(1)}/s
              </span>
            </div>
            <div className="flex justify-between items-center text-xs pt-1.5 border-t border-[#EAEAEA] dark:border-[#4A5568]">
              <span className="font-bold text-gray-800 dark:text-[#E2E8F0]">Acknowledge:</span>
              <span className="font-mono font-bold text-green-600 bg-green-50 dark:bg-green-950/30 px-2 py-0.5 rounded border border-green-100 dark:border-green-900/50">
                {ackRate.toFixed(1)}/s
              </span>
            </div>
          </div>
        </div>

        {/* Global Counts */}
        <div className="bg-[#F6F6F6] dark:bg-[#1E293B] border border-[#CCCCCC] dark:border-[#4A5568] p-4 rounded">
          <h2 className="font-bold text-xs text-[#4C5B66] dark:text-[#A2B1BC] uppercase tracking-wider mb-3 pb-1 border-b border-[#EAEAEA] dark:border-[#4A5568]">
            Global Counts
          </h2>
          <div className="grid grid-cols-2 gap-2 text-xs">
            <div className="flex flex-col bg-white dark:bg-[#1A202C] border border-[#EAEAEA] dark:border-[#4A5568] p-1.5 rounded">
              <span className="text-gray-500 dark:text-gray-400 text-[10px] uppercase font-semibold">Connections</span>
              <span className="font-mono font-bold text-sm text-[#4C5B66] dark:text-[#E2E8F0]">{overview?.object_totals?.connections || 0}</span>
            </div>
            <div className="flex flex-col bg-white dark:bg-[#1A202C] border border-[#EAEAEA] dark:border-[#4A5568] p-1.5 rounded">
              <span className="text-gray-500 dark:text-gray-400 text-[10px] uppercase font-semibold">Channels</span>
              <span className="font-mono font-bold text-sm text-[#4C5B66] dark:text-[#E2E8F0]">{overview?.object_totals?.channels || 0}</span>
            </div>
            <div className="flex flex-col bg-white dark:bg-[#1A202C] border border-[#EAEAEA] dark:border-[#4A5568] p-1.5 rounded">
              <span className="text-gray-500 dark:text-gray-400 text-[10px] uppercase font-semibold">Exchanges</span>
              <span className="font-mono font-bold text-sm text-[#4C5B66] dark:text-[#E2E8F0]">{overview?.object_totals?.exchanges || 0}</span>
            </div>
            <div className="flex flex-col bg-white dark:bg-[#1A202C] border border-[#EAEAEA] dark:border-[#4A5568] p-1.5 rounded">
              <span className="text-gray-500 dark:text-gray-400 text-[10px] uppercase font-semibold">Queues</span>
              <span className="font-mono font-bold text-sm text-[#4C5B66] dark:text-[#E2E8F0]">{overview?.object_totals?.queues || 0}</span>
            </div>
          </div>
        </div>
      </div>

      {/* Nodes Section */}
      <div className="space-y-3">
        <h2 className="text-sm font-bold text-[#4C5B66] dark:text-[#E2E8F0] border-b border-[#CCCCCC] dark:border-[#4A5568] pb-0.5">
          Nodes
        </h2>
        <div className="overflow-x-auto border border-[#CCCCCC] dark:border-[#4A5568] rounded">
          <table className="w-full text-xs font-sans">
            <thead>
              <tr className="bg-[#EAEAEA] dark:bg-[#2D3748] border-b border-[#CCCCCC] dark:border-[#4A5568] text-[#333333] dark:text-[#E2E8F0]">
                <th className="text-left p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-bold">Name</th>
                <th className="text-left p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-bold">File Descriptors</th>
                <th className="text-left p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-bold">Socket Descriptors</th>
                <th className="text-left p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-bold">Erlang Processes</th>
                <th className="text-left p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-bold">Memory Limit</th>
                <th className="text-left p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-bold">Disk Free Limit</th>
                <th className="text-left p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-bold">Uptime</th>
                <th className="text-left p-2 font-bold">Status</th>
              </tr>
            </thead>
            <tbody>
              {nodesLoading ? (
                <tr>
                  <td colSpan={8} className="p-4 text-center text-gray-500 dark:text-gray-400">
                    Loading nodes details...
                  </td>
                </tr>
              ) : !nodes || nodes.length === 0 ? (
                <tr>
                  <td colSpan={8} className="p-4 text-center text-gray-500 dark:text-gray-400">
                    No cluster nodes running.
                  </td>
                </tr>
              ) : (
                nodes.map((node) => {
                  const fdPercent = ((node.fd_used || 0) / (node.fd_total || 1024)) * 100;
                  const socketPercent = ((node.sockets_used || 0) / (node.sockets_total || 8192)) * 100;
                  const uptimeDays = Math.floor((node.uptime || 0) / (1000 * 60 * 60 * 24));
                  const uptimeHours = Math.floor(((node.uptime || 0) % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));

                  return (
                    <tr key={node.name} className="bg-white dark:bg-[#1A202C] border-b border-[#EAEAEA] dark:border-[#4A5568] hover:bg-gray-50 dark:hover:bg-[#2D3748] font-mono text-xs">
                      <td className="p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-sans font-bold text-gray-800 dark:text-[#E2E8F0]">
                        {node.name}
                      </td>
                      <td className="p-2 border-r border-[#CCCCCC] dark:border-[#4A5568]">
                        <div className="flex justify-between mb-1">
                          <span>{node.fd_used || 0}</span>
                          <span className="text-gray-400 dark:text-gray-500">/ {node.fd_total || 1024}</span>
                        </div>
                        <div className="w-full bg-gray-100 dark:bg-gray-800 h-1.5 rounded overflow-hidden">
                          <div className="bg-[#5CB85C] h-full" style={{ width: `${Math.min(fdPercent, 100)}%` }} />
                        </div>
                      </td>
                      <td className="p-2 border-r border-[#CCCCCC] dark:border-[#4A5568]">
                        <div className="flex justify-between mb-1">
                          <span>{node.sockets_used || 0}</span>
                          <span className="text-gray-400 dark:text-gray-500">/ {node.sockets_total || 8192}</span>
                        </div>
                        <div className="w-full bg-gray-100 dark:bg-gray-800 h-1.5 rounded overflow-hidden">
                          <div className="bg-[#5CB85C] h-full" style={{ width: `${Math.min(socketPercent, 100)}%` }} />
                        </div>
                      </td>
                      <td className="p-2 border-r border-[#CCCCCC] dark:border-[#4A5568]">
                        <div className="flex justify-between mb-1">
                          <span>{node.proc_used || 230}</span>
                          <span className="text-gray-400 dark:text-gray-500">/ {node.proc_total || 1048576}</span>
                        </div>
                      </td>
                      <td className="p-2 border-r border-[#CCCCCC] dark:border-[#4A5568]">
                        <div className="flex justify-between mb-1">
                          <span>{formatBytes(node.mem_used || 120 * 1024 * 1024)}</span>
                          <span className="text-gray-400 dark:text-gray-500">/ {formatBytes(node.mem_limit || 8 * 1024 * 1024 * 1024)}</span>
                        </div>
                      </td>
                      <td className="p-2 border-r border-[#CCCCCC] dark:border-[#4A5568]">
                        <div className="flex justify-between mb-1">
                          <span>{formatBytes(node.disk_free || 45 * 1024 * 1024 * 1024)}</span>
                          <span className="text-gray-400 dark:text-gray-500">/ {formatBytes(node.disk_free_limit || 2 * 1024 * 1024 * 1024)}</span>
                        </div>
                      </td>
                      <td className="p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-sans">
                        {uptimeDays}d {uptimeHours}h
                      </td>
                      <td className="p-2 font-sans font-bold">
                        <span className="bg-green-100 dark:bg-green-950 text-green-800 dark:text-green-200 border border-green-200 dark:border-green-900/50 px-2 py-0.5 rounded text-[10px]">
                          Running
                        </span>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Port Bindings Section */}
      <div className="space-y-3">
        <h2 className="text-sm font-bold text-[#4C5B66] dark:text-[#E2E8F0] border-b border-[#CCCCCC] dark:border-[#4A5568] pb-0.5">
          Listening Ports
        </h2>
        <div className="overflow-x-auto border border-[#CCCCCC] dark:border-[#4A5568] rounded">
          <table className="w-full text-xs font-sans">
            <thead>
              <tr className="bg-[#EAEAEA] dark:bg-[#2D3748] border-b border-[#CCCCCC] dark:border-[#4A5568] text-[#333333] dark:text-[#E2E8F0]">
                <th className="text-left p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-bold">Protocol</th>
                <th className="text-left p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-bold">Bound Address</th>
                <th className="text-left p-2 font-bold">Port</th>
              </tr>
            </thead>
            <tbody>
              <tr className="bg-white dark:bg-[#1A202C] border-b border-[#EAEAEA] dark:border-[#4A5568] hover:bg-gray-50 dark:hover:bg-[#2D3748] font-mono text-xs">
                <td className="p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-sans font-bold">amqp</td>
                <td className="p-2 border-r border-[#CCCCCC] dark:border-[#4A5568]">0.0.0.0</td>
                <td className="p-2">5672</td>
              </tr>
              <tr className="bg-white dark:bg-[#1A202C] border-b border-[#EAEAEA] dark:border-[#4A5568] hover:bg-gray-50 dark:hover:bg-[#2D3748] font-mono text-xs">
                <td className="p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-sans font-bold">clustering</td>
                <td className="p-2 border-r border-[#CCCCCC] dark:border-[#4A5568]">0.0.0.0</td>
                <td className="p-2">5680</td>
              </tr>
              <tr className="bg-white dark:bg-[#1A202C] border-b border-[#EAEAEA] dark:border-[#4A5568] hover:bg-gray-50 dark:hover:bg-[#2D3748] font-mono text-xs">
                <td className="p-2 border-r border-[#CCCCCC] dark:border-[#4A5568] font-sans font-bold">http</td>
                <td className="p-2 border-r border-[#CCCCCC] dark:border-[#4A5568]">0.0.0.0</td>
                <td className="p-2">15672</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
