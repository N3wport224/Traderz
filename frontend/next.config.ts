import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Phase 9: compile the dashboard to plain HTML/JS/CSS in `out/` so the
  // FastAPI backend (or the packaged .exe) can serve it directly from a
  // single port — no Node.js server in production. `next dev` still works
  // normally for development against the API on :8000.
  output: "export",
};

export default nextConfig;
