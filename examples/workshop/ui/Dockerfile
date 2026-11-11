# The workshop's UI, built with Vite and served by nginx. Build it from the root of
# the repository:
#
#   docker build -f examples/workshop/ui/Dockerfile -t effect-cqrs-workshop-ui .
#
# VITE_API_URL is where the UI finds the server. `/`, the default here, means the
# origin the page is served from, for a proxy that routes the API and the UI alike.
FROM node:24-slim AS build

WORKDIR /app
RUN corepack enable

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .pnpmfile.cjs ./
COPY examples/package.json ./examples/
RUN pnpm install --frozen-lockfile --ignore-scripts --filter effect-cqrs-examples...

COPY src ./src
COPY tsconfig.json ./tsconfig.json
COPY examples/tsconfig.json ./examples/tsconfig.json
COPY examples/devtools ./examples/devtools
COPY examples/bank-ui/src/components ./examples/bank-ui/src/components
COPY examples/workshop/api.ts ./examples/workshop/api.ts
COPY examples/workshop/domain ./examples/workshop/domain
COPY examples/workshop/ui ./examples/workshop/ui

ARG VITE_API_URL=/
ENV VITE_API_URL=$VITE_API_URL
RUN pnpm build:workshop

FROM nginx:1.29-alpine
COPY --from=build /app/examples/workshop/ui/dist /usr/share/nginx/html
