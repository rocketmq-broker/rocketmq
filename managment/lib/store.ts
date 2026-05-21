// Global state management with Zustand

import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export interface ConnectionConfig {
  url: string;
  username: string;
  password: string;
}

interface ConnectionState {
  config: ConnectionConfig | null;
  isConnected: boolean;
  isConnecting: boolean;
  error: string | null;
  lastConnected: number | null;
  setConfig: (config: ConnectionConfig) => void;
  setConnected: (connected: boolean) => void;
  setConnecting: (connecting: boolean) => void;
  setError: (error: string | null) => void;
  disconnect: () => void;
}

export const useConnectionStore = create<ConnectionState>()(
  persist(
    (set) => ({
      config: null,
      isConnected: false,
      isConnecting: false,
      error: null,
      lastConnected: null,
      setConfig: (config) => set({ config, lastConnected: Date.now() }),
      setConnected: (isConnected) => set({ isConnected, error: isConnected ? null : undefined }),
      setConnecting: (isConnecting) => set({ isConnecting }),
      setError: (error) => set({ error, isConnected: false }),
      disconnect: () => set({ isConnected: false, config: null, error: null }),
    }),
    {
      name: 'rabbitmq-connection',
      partialize: (state) => ({ config: state.config }),
    }
  )
);

// UI State
interface UIState {
  selectedVHost: string;
  refreshInterval: number;
  sidebarCollapsed: boolean;
  theme: 'light' | 'dark' | 'system';
  setSelectedVHost: (vhost: string) => void;
  setRefreshInterval: (interval: number) => void;
  setSidebarCollapsed: (collapsed: boolean) => void;
  setTheme: (theme: 'light' | 'dark' | 'system') => void;
}

export const useUIStore = create<UIState>()(
  persist(
    (set) => ({
      selectedVHost: '/',
      refreshInterval: 5000,
      sidebarCollapsed: false,
      theme: 'dark',
      setSelectedVHost: (selectedVHost) => set({ selectedVHost }),
      setRefreshInterval: (refreshInterval) => set({ refreshInterval }),
      setSidebarCollapsed: (sidebarCollapsed) => set({ sidebarCollapsed }),
      setTheme: (theme) => set({ theme }),
    }),
    {
      name: 'rabbitmq-ui',
    }
  )
);
