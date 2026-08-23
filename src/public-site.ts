const NGINX_WELCOME = `<!DOCTYPE html>
<html>
<head>
<title>Welcome to nginx!</title>
<style>
html { color-scheme: light dark; }
body { width: 35em; margin: 0 auto;
font-family: Tahoma, Verdana, Arial, sans-serif; }
</style>
</head>
<body>
<h1>Welcome to nginx!</h1>
<p>If you see this page, the nginx web server is successfully installed and
working. Further configuration is required.</p>

<p>For online documentation and support please refer to
<a href="http://nginx.org/">nginx.org</a>.<br/>
Commercial support is available at
<a href="http://nginx.com/">nginx.com</a>.</p>

<p><em>Thank you for using nginx.</em></p>
</body>
</html>
`;

export function publicResponse(_title: string, status = 200): Response {
  return new Response(NGINX_WELCOME, {
    status,
    headers: {
      'Content-Type': 'text/html; charset=utf-8',
      'Content-Length': String(new TextEncoder().encode(NGINX_WELCOME).byteLength),
      'Cache-Control': status === 200 ? 'public, max-age=300' : 'no-store',
      'Server': 'nginx',
      'X-Content-Type-Options': 'nosniff'
    }
  });
}
