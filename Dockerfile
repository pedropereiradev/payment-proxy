FROM oven/bun:latest AS builder
WORKDIR /app
COPY package.json bun.lockb tsconfig.json ./
RUN bun install --frozen-lockfile
COPY src/ ./src/
RUN bun build --entrypoints ./src/index.ts --outdir ./out --target bun


FROM oven/bun:latest AS production
WORKDIR /app
COPY --from=builder /app/out/index.js ./
COPY --from=builder /app/package.json ./
EXPOSE 3001
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD bun -e "fetch('http://localhost:3001/health').then(r => r.ok ? process.exit(0) : process.exit(1)).catch(() => process.exit(1))"
CMD ["bun", "run", "index.js"]
