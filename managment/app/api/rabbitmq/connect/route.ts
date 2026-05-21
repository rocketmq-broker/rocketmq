// Connection management API routes

import { NextRequest, NextResponse } from 'next/server';
import { cookies } from 'next/headers';

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { url, username, password } = body;

    if (!url || !username || !password) {
      return NextResponse.json(
        { error: 'Missing required fields', reason: 'URL, username, and password are required' },
        { status: 400 }
      );
    }

    // Test the connection
    const testUrl = `${url}/api/overview`;
    const response = await fetch(testUrl, {
      headers: {
        'Authorization': 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64'),
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      const errorText = await response.text();
      let errorMessage = `HTTP ${response.status}`;
      try {
        const errorJson = JSON.parse(errorText);
        errorMessage = errorJson.reason || errorJson.error || errorMessage;
      } catch {
        if (errorText) errorMessage = errorText;
      }
      return NextResponse.json(
        { error: 'Connection failed', reason: errorMessage },
        { status: response.status }
      );
    }

    // Store the config in a cookie
    const cookieStore = await cookies();
    cookieStore.set('rabbitmq-config', JSON.stringify({ url, username, password }), {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'strict',
      maxAge: 60 * 60 * 24 * 7, // 1 week
    });

    const overview = await response.json();
    return NextResponse.json({ 
      success: true, 
      overview: {
        cluster_name: overview.cluster_name,
        rabbitmq_version: overview.rabbitmq_version,
        erlang_version: overview.erlang_version,
        node: overview.node,
      }
    });
  } catch (error) {
    console.error('Connection error:', error);
    return NextResponse.json(
      { 
        error: 'Connection error', 
        reason: error instanceof Error ? error.message : 'Failed to connect' 
      },
      { status: 502 }
    );
  }
}

export async function DELETE() {
  const cookieStore = await cookies();
  cookieStore.delete('rabbitmq-config');
  return NextResponse.json({ success: true });
}

export async function GET() {
  const cookieStore = await cookies();
  const configCookie = cookieStore.get('rabbitmq-config');
  
  if (!configCookie) {
    return NextResponse.json({ connected: false });
  }

  try {
    const config = JSON.parse(configCookie.value);
    // Test if still connected
    const testUrl = `${config.url}/api/overview`;
    const response = await fetch(testUrl, {
      headers: {
        'Authorization': 'Basic ' + Buffer.from(`${config.username}:${config.password}`).toString('base64'),
      },
    });

    if (!response.ok) {
      // Cookie exists but connection is stale
      return NextResponse.json({ connected: false, stale: true });
    }

    return NextResponse.json({ 
      connected: true, 
      url: config.url,
      username: config.username,
    });
  } catch {
    return NextResponse.json({ connected: false, error: true });
  }
}
