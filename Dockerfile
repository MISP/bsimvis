# One image for the app and the workers: python + uv, plus whatever install.sh
# fetches (Ghidra, portable JDK, capa, UPX, YARA rules, frontend assets).
# Redis/Kvrocks are not built here; docker-compose.yml runs them as images.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim@sha256:531f855bda2c73cd6ef67d56b733b357cea384185b3022bd09f05e002cd144ca

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl unzip git ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# install.sh reads .env over the environment, so set the flag there.
# The cache mount keeps the Ghidra/JDK downloads across rebuilds.
RUN cp .env.example .env \
    && sed -i 's/^DOCKER_DATASTORES=.*/DOCKER_DATASTORES=true/' .env
ENV JDK_VERSION=21.0.12.1+1
RUN --mount=type=cache,target=/app/scratch_build ./install.sh

# pyghidra runs a bare `java`, so the portable JDK has to be on PATH.
RUN ln -s "$(ls -d /app/bin/jdk-21* | head -n 1)" /opt/jdk
ENV JAVA_HOME=/opt/jdk
ENV PATH="/opt/jdk/bin:/app/.venv/bin:/app/bin:${PATH}"

# Non-root. One gunicorn worker: the app keeps in-process caches. Threads serve concurrency.
RUN useradd -m -u 1000 bsimvis && chown bsimvis /app
USER bsimvis
EXPOSE 5000
CMD ["gunicorn", "-b", "0.0.0.0:5000", "-w", "1", "--threads", "16", "--timeout", "600", "app:app"]
