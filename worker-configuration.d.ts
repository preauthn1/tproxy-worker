declare namespace Cloudflare {
  interface Env {
    WEB_SECRET: string;
    PUBLIC_HOSTNAME: string;
    PUBLIC_SITE_TITLE?: string;
    CARRIER_MODE?: string;
    DIAGNOSTICS?: string;
    WSS_FALLBACK?: string;
    BOOTSTRAPS: DurableObjectNamespace;
    SESSIONS: DurableObjectNamespace;
  }
}

type Env = Cloudflare.Env;
