function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, (character) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[character]!);
}

export function publicResponse(title: string, status = 200): Response {
  const safe = escapeHtml(title || 'Public Site');
  const body = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${safe}</title><style>body{max-width:48rem;margin:12vh auto;padding:0 1.5rem;font:16px/1.6 system-ui;color:#20242a}h1{font-size:2rem}</style></head><body><h1>${safe}</h1><p>Welcome. This website is currently online.</p></body></html>`;
  return new Response(body, {
    status,
    headers: {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': status === 200 ? 'public, max-age=300' : 'no-store',
      'X-Content-Type-Options': 'nosniff',
      'Referrer-Policy': 'strict-origin-when-cross-origin'
    }
  });
}
