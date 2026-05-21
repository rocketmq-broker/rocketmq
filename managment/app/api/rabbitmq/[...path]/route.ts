// API proxy route to forward requests to RabbitMQ Management API
// This handles CORS and credential storage

import { NextRequest, NextResponse } from 'next/server';
import { cookies } from 'next/headers';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> }
) {
  return handleRequest(request, params, 'GET');
}

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> }
) {
  return handleRequest(request, params, 'POST');
}

export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> }
) {
  return handleRequest(request, params, 'PUT');
}

export async function DELETE(
  request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> }
) {
  return handleRequest(request, params, 'DELETE');
}

async function handleRequest(
  request: NextRequest,
  paramsPromise: Promise<{ path: string[] }>,
  method: string
) {
  try {
    const { path } = await paramsPromise;
    const cookieStore = await cookies();
    
    // Get connection config from cookies
    const configCookie = cookieStore.get('rabbitmq-config');
    if (!configCookie) {
      return NextResponse.json(
        { error: 'Not connected', reason: 'No connection configuration found' },
        { status: 401 }
      );
    }

    let config: { url: string; username: string; password: string };
    try {
      config = JSON.parse(configCookie.value);
    } catch {
      return NextResponse.json(
        { error: 'Invalid config', reason: 'Failed to parse connection configuration' },
        { status: 400 }
      );
    }

    const { url, username, password } = config;

    // Build the target URL — extract raw pathname from request.url to preserve %2F for vhost "/"
    let apiPath = '';
    const prefix = '/api/rabbitmq';
    const urlString = request.url;
    const prefixIdx = urlString.indexOf(prefix);
    if (prefixIdx === -1) {
      apiPath = '/api/' + path.map(encodeURIComponent).join('/');
    } else {
      let rawPath = urlString.substring(prefixIdx + prefix.length);
      const qIdx = rawPath.indexOf('?');
      if (qIdx !== -1) {
        rawPath = rawPath.substring(0, qIdx);
      }
      apiPath = '/api' + rawPath;
    }

    const searchParams = request.nextUrl.searchParams.toString();
    const targetUrl = `${url}${apiPath}${searchParams ? '?' + searchParams : ''}`;

    // Prepare headers
    const headers: HeadersInit = {
      'Authorization': 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64'),
      'Content-Type': 'application/json',
    };

    // Forward X-Reason header if present (for connection close)
    const xReason = request.headers.get('X-Reason');
    if (xReason) {
      headers['X-Reason'] = xReason;
    }

    // Get request body if applicable
    let body: string | undefined;
    if (method !== 'GET' && method !== 'DELETE') {
      try {
        body = await request.text();
      } catch {
        // No body
      }
    } else if (method === 'DELETE') {
      // Some DELETE requests in RabbitMQ API accept a body
      try {
        const text = await request.text();
        if (text) {
          body = text;
        }
      } catch {
        // No body
      }
    }

    // Make the request to RabbitMQ
    const response = await fetch(targetUrl, {
      method,
      headers,
      body,
    });

    // Get response body
    const responseText = await response.text();

    // Return the response
    if (!response.ok) {
      return NextResponse.json(
        responseText ? JSON.parse(responseText) : { error: 'Request failed', reason: `HTTP ${response.status}` },
        { status: response.status }
      );
    }

    if (response.status === 204 || !responseText) {
      return new NextResponse(null, { status: 204 });
    }

    return NextResponse.json(JSON.parse(responseText));
  } catch (error) {
    console.error('RabbitMQ API proxy error:', error);
    return NextResponse.json(
      { 
        error: 'Proxy error', 
        reason: error instanceof Error ? error.message : 'Failed to connect to RabbitMQ server' 
      },
      { status: 502 }
    );
  }
}
