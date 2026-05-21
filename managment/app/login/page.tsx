"use client";

import { useConnectionStore } from "@/lib/store";
import { useRouter } from "next/navigation";
import { useState } from "react";
import { mutate } from "swr";

export default function LoginPage() {
  const [username, setUsername] = useState("guest");
  const [password, setPassword] = useState("guest");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const router = useRouter();
  const { setConfig, setConnected } = useConnectionStore();

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);

    try {
      const response = await fetch("/api/rabbitmq/connect", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          url: "http://localhost:15672",
          username,
          password,
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.reason || data.error || "Authentication failed");
      }

      setConfig({ url: "http://localhost:15672", username, password });
      setConnected(true);
      mutate(() => true);
      router.push("/");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed");
      setConnected(false);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="login-page">
      <div className="login-card">
        <div className="login-header">
          <div className="login-logo">
            <svg width="48" height="48" viewBox="0 0 48 48" fill="none">
              <rect width="48" height="48" rx="12" fill="hsl(var(--primary))" />
              <text
                x="50%"
                y="55%"
                dominantBaseline="middle"
                textAnchor="middle"
                fill="white"
                fontSize="24"
                fontWeight="700"
                fontFamily="system-ui"
              >
                R
              </text>
            </svg>
          </div>
          <h1>RocketMQ Management</h1>
          <p className="login-subtitle">
            Sign in to manage your message broker
          </p>
        </div>

        <form onSubmit={handleLogin} className="login-form">
          {error && (
            <div className="login-error">
              <svg
                width="16"
                height="16"
                viewBox="0 0 16 16"
                fill="currentColor"
              >
                <path d="M8 1a7 7 0 110 14A7 7 0 018 1zm0 10.5a.75.75 0 100 1.5.75.75 0 000-1.5zM8 4a.75.75 0 00-.75.75v4.5a.75.75 0 001.5 0v-4.5A.75.75 0 008 4z" />
              </svg>
              <span>{error}</span>
            </div>
          )}

          <div className="login-field">
            <label htmlFor="username">Username</label>
            <input
              id="username"
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="guest"
              autoComplete="username"
              autoFocus
              required
            />
          </div>

          <div className="login-field">
            <label htmlFor="password">Password</label>
            <input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="guest"
              autoComplete="current-password"
              required
            />
          </div>

          <button type="submit" className="login-button" disabled={loading}>
            {loading ? (
              <>
                <svg
                  className="login-spinner"
                  width="16"
                  height="16"
                  viewBox="0 0 16 16"
                >
                  <circle
                    cx="8"
                    cy="8"
                    r="6"
                    stroke="currentColor"
                    strokeWidth="2"
                    fill="none"
                    strokeDasharray="28"
                    strokeDashoffset="8"
                  />
                </svg>
                Signing in…
              </>
            ) : (
              "Sign in"
            )}
          </button>
        </form>

        <div className="login-footer">
          <p>
            Default credentials: <code>guest</code> / <code>guest</code>
          </p>
        </div>
      </div>

      <style jsx>{`
        .login-page {
          min-height: 100vh;
          display: flex;
          align-items: center;
          justify-content: center;
          background: linear-gradient(
            135deg,
            hsl(222 47% 8%) 0%,
            hsl(222 47% 14%) 50%,
            hsl(240 30% 12%) 100%
          );
          padding: 1rem;
        }
        .login-card {
          width: 100%;
          max-width: 400px;
          background: hsl(222 47% 11%);
          border: 1px solid hsl(215 20% 20%);
          border-radius: 16px;
          padding: 2.5rem;
          box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.6);
        }
        .login-header {
          text-align: center;
          margin-bottom: 2rem;
        }
        .login-logo {
          display: flex;
          justify-content: center;
          margin-bottom: 1.25rem;
        }
        .login-header h1 {
          font-size: 1.5rem;
          font-weight: 700;
          color: hsl(210 40% 98%);
          margin: 0 0 0.5rem;
          letter-spacing: -0.025em;
        }
        .login-subtitle {
          color: hsl(215 20% 55%);
          font-size: 0.875rem;
          margin: 0;
        }
        .login-form {
          display: flex;
          flex-direction: column;
          gap: 1.25rem;
        }
        .login-error {
          display: flex;
          align-items: center;
          gap: 0.5rem;
          padding: 0.75rem 1rem;
          background: hsl(0 62% 15%);
          border: 1px solid hsl(0 62% 25%);
          border-radius: 8px;
          color: hsl(0 62% 70%);
          font-size: 0.875rem;
        }
        .login-field {
          display: flex;
          flex-direction: column;
          gap: 0.375rem;
        }
        .login-field label {
          font-size: 0.8125rem;
          font-weight: 500;
          color: hsl(215 20% 65%);
        }
        .login-field input {
          height: 2.75rem;
          padding: 0 0.875rem;
          background: hsl(222 47% 8%);
          border: 1px solid hsl(215 20% 22%);
          border-radius: 8px;
          color: hsl(210 40% 98%);
          font-size: 0.9375rem;
          outline: none;
          transition:
            border-color 0.15s,
            box-shadow 0.15s;
        }
        .login-field input:focus {
          border-color: hsl(var(--primary));
          box-shadow: 0 0 0 3px hsl(var(--primary) / 0.15);
        }
        .login-field input::placeholder {
          color: hsl(215 20% 35%);
        }
        .login-button {
          height: 2.75rem;
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 0.5rem;
          background: hsl(var(--primary));
          color: hsl(var(--primary-foreground));
          border: none;
          border-radius: 8px;
          font-size: 0.9375rem;
          font-weight: 600;
          cursor: pointer;
          transition:
            opacity 0.15s,
            transform 0.1s;
          margin-top: 0.5rem;
        }
        .login-button:hover:not(:disabled) {
          opacity: 0.9;
        }
        .login-button:active:not(:disabled) {
          transform: scale(0.98);
        }
        .login-button:disabled {
          opacity: 0.6;
          cursor: not-allowed;
        }
        @keyframes spin {
          to {
            transform: rotate(360deg);
          }
        }
        .login-spinner {
          animation: spin 0.8s linear infinite;
        }
        .login-footer {
          text-align: center;
          margin-top: 1.5rem;
          padding-top: 1.5rem;
          border-top: 1px solid hsl(215 20% 18%);
        }
        .login-footer p {
          color: hsl(215 20% 45%);
          font-size: 0.8125rem;
          margin: 0;
        }
        .login-footer code {
          background: hsl(222 47% 15%);
          padding: 0.125rem 0.375rem;
          border-radius: 4px;
          font-family: "JetBrains Mono", monospace;
          font-size: 0.75rem;
          color: hsl(215 20% 65%);
        }
      `}</style>
    </div>
  );
}
