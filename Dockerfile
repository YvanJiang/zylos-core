# ────────────────────────────────────────────────────────────────────────────
# Zylos — Official Dockerfile
#
# Build:  docker build -t zylos .
# Run:    docker compose up -d   (see docker-compose.yml)
#
# This image installs Zylos and starts the PM2-supervised Core executor service.
# ────────────────────────────────────────────────────────────────────────────

FROM node:22-slim

LABEL org.opencontainers.image.source="https://github.com/zylos-ai/zylos-core"
LABEL org.opencontainers.image.description="Zylos — autonomous AI agent infrastructure"

# ── System packages ───────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
      git \
      curl \
      bash \
      ca-certificates \
      build-essential \
      python3 \
      python3-dev \
      python3-pip \
      python3-venv \
      # Needed by some Claude Code operations
      procps \
      # For `zylos doctor` network checks
      dnsutils \
    && rm -rf /var/lib/apt/lists/*

# ── Global npm tools ──────────────────────────────────────────────────────────
RUN npm install -g pm2@latest

# ── Local Office document toolchain ──────────────────────────────────────────
RUN python3 -m venv /opt/zylos-office \
    && /opt/zylos-office/bin/pip install --no-cache-dir \
      openpyxl \
      python-docx \
      python-pptx

# ── Create zylos user (non-root) ──────────────────────────────────────────────
RUN useradd -m -s /bin/bash zylos \
    && mkdir -p /home/zylos/.local/bin /home/zylos/.npm-global \
    && chown -R zylos:zylos /home/zylos
USER zylos
ENV HOME=/home/zylos
ENV NPM_CONFIG_PREFIX=/home/zylos/.npm-global
ENV PATH="/opt/zylos-office/bin:/home/zylos/.npm-global/bin:/home/zylos/.local/bin:/usr/local/bin:${PATH}"
ENV ZYLOS_PACKAGE_ROOT=/home/zylos/.npm-global/lib/node_modules/zylos

# ── Provider and productivity CLIs ───────────────────────────────────────────
RUN npm install -g \
      @openai/codex@0.144.5 \
      @larksuite/cli@1.0.69 \
    && codex --version \
    && lark-cli --version

# ── Install zylos-core from local source ─────────────────────────────────────
# COPY the repo (filtered by .dockerignore) and install from it, so the image
# always matches the exact commit/tag being built.
WORKDIR /home/zylos
COPY --chown=zylos:zylos . /tmp/zylos-core
RUN npm install -g --install-links /tmp/zylos-core \
    && node /home/zylos/.npm-global/lib/node_modules/zylos/scripts/install-skill-deps.js \
    && rm -rf /tmp/zylos-core \
    && zylos --version

# ── Workspace directories ─────────────────────────────────────────────────────
# ~/zylos is mounted as a single volume in docker-compose.yml.
# Creating subdirectories here ensures correct ownership in the image.
RUN mkdir -p \
      /home/zylos/zylos/pm2 \
      /home/zylos/.claude

# ── Copy PM2 ecosystem config ─────────────────────────────────────────────────
COPY --chown=zylos:zylos templates/pm2/ecosystem.config.cjs /home/zylos/zylos/pm2/ecosystem.config.cjs

# ── Copy entrypoint ───────────────────────────────────────────────────────────
COPY --chown=zylos:zylos docker/entrypoint.sh /entrypoint.sh
COPY --chown=zylos:zylos docker/skills /opt/zylos/preinstalled-skills
RUN chmod +x /entrypoint.sh

# Healthcheck is defined in docker-compose.yml (start_period=600s for slow init).
# No HEALTHCHECK here to avoid a conflicting override.

USER root
ENTRYPOINT ["/entrypoint.sh"]
