/* SPDX-License-Identifier: GPL-3.0-only */
/** Structured, opt-in diagnostics. Never log tokens, secrets or payload bytes. */
let enabled = false;

export function configureDiagnostics(value: string | undefined): void {
  enabled = value === '1' || value === 'true';
}

export function diag(event: string, fields: Record<string, unknown> = {}): void {
  if (!enabled) return;
  const record: Record<string, unknown> = { tproxy: 1, event, t: Date.now() };
  for (const [key, value] of Object.entries(fields)) if (value !== undefined) record[key] = value;
  console.log(JSON.stringify(record));
}

export function errorKind(error: unknown): string {
  if (error instanceof Error) {
    const message = error.message.replace(/[0-9a-f]{16,}/gi, '<hex>').slice(0, 120);
    return `${error.name}: ${message}`;
  }
  return typeof error;
}
