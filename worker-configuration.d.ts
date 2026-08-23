declare namespace Cloudflare {
  interface Env {
    WEB_SECRET: string;
    PUBLIC_HOSTNAME: string;
    PUBLIC_SITE_TITLE?: string;
    BOOTSTRAPS: DurableObjectNamespace;
    SESSIONS: DurableObjectNamespace;
  }
}

type Env = Cloudflare.Env;
