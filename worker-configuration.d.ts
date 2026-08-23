declare namespace Cloudflare {
  interface Env {
    WEB_SECRET: string;
    PUBLIC_HOSTNAME: string;
    BACKEND_HOST: string;
    BACKEND_PORT: string;
    PUBLIC_SITE_TITLE?: string;
    BOOTSTRAPS: DurableObjectNamespace;
    SESSIONS: DurableObjectNamespace;
  }
}

type Env = Cloudflare.Env;
