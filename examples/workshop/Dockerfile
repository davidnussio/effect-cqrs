# The workshop server. Build it from the root of the repository, which
# .dockerignore trims to what the build needs:
#
#   docker build -f examples/workshop/Dockerfile -t effect-cqrs-workshop .
#   docker run --rm -p 3000:3000 -e POSTGRES_URL=... effect-cqrs-workshop
#
# The first stage bundles the server and its dependencies into one file with
# Vite, so the image that runs holds Node and that file only.
FROM node:24-slim AS build

WORKDIR /app
RUN corepack enable

# Only the examples package and the library it depends on: the docs are left out.
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .pnpmfile.cjs ./
COPY examples/package.json ./examples/
RUN pnpm install --frozen-lockfile --ignore-scripts --filter effect-cqrs-examples...

COPY src ./src
COPY examples/workshop/api.ts ./examples/workshop/api.ts
COPY examples/workshop/domain ./examples/workshop/domain
COPY examples/workshop/server ./examples/workshop/server
RUN pnpm build:workshop-server

FROM node:24-slim

WORKDIR /app
COPY --from=build /app/examples/workshop/server/dist/server.mjs ./server.mjs

ENV NODE_ENV=production
ENV PORT=3000
EXPOSE 3000
USER node

CMD ["node", "server.mjs"]
