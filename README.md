# SEC EDGAR Filings RAG Demo

Docker Compose orchestration for an end-to-end pipeline: download SEC EDGAR filings, embed them into pgvector, and search them with a local RAG UI backed by Ollama.

This repo contains **no application source code** — only Compose wiring that pulls published images from Docker Hub and connects them on a shared network.

## What it does

| Step | Component | Role |
|------|-----------|------|
| 1 | [sec-edgar-filings](https://github.com/sanjuthomas/sec-edgar-filings) | Downloads filings from SEC EDGAR, stores metadata in MongoDB, writes `.htm` files to disk, publishes Kafka events |
| 2 | [sec-edgar-filings-to-pgvector](https://github.com/sanjuthomas/sec-edgar-filings-to-pgvector) | Consumes the `filings` Kafka topic, reads filing content from disk, generates embeddings, loads pgvector |
| 3 | [sec-edgar-filings-semantic-search-ui](https://github.com/sanjuthomas/sec-edgar-filings-semantic-search-ui) | Semantic search + RAG answers over pgvector, using Ollama on your Mac for generation |
| 4 | [kafka-web-clients](https://github.com/sanjuthomas/kafka-web-clients) | Optional browser UI to inspect Kafka messages (debug) |

## Architecture

```mermaid
flowchart LR
    SEC[SEC EDGAR] --> API[sec-edgar-filings]
    API --> Mongo[(MongoDB)]
    API --> Kafka[[Kafka: filings]]
    API --> Disk[/Volumes/Transcend/edgar]

    Kafka --> ETL[sec-edgar-filings-to-pgvector]
    Mongo --> ETL
    Disk --> ETL
    ETL --> PG[(pgvector)]

    PG --> UI[semantic-search-ui]
    Ollama[Ollama on host] --> UI

    Kafka -.->|debug profile| KWC[kafka-web-clients]
```

**Data flow**

1. Downloader registers a filing in MongoDB and publishes a `filing.downloaded` event to Kafka.
2. ETL consumer reads the event, looks up metadata in MongoDB, reads the `.htm` file from the shared EDGAR mount, chunks and embeds text, upserts into pgvector.
3. Search UI embeds your question (ONNX, same model as ingest), retrieves nearest chunks from pgvector, and asks Ollama to synthesize a cited answer.

## Prerequisites

- **Docker Desktop** (or Docker Engine + Compose v2)
- **Apple Silicon / arm64** — all custom images publish `linux/arm64` manifests
- **Transcend drive** mounted at `/Volumes/Transcend`
- **macOS file sharing** — Docker Desktop → Settings → Resources → File sharing → include `/Volumes`
- **Ollama** running on the host with a chat model (UI default: `qwen3:14b`)

```bash
ollama list
```

Create data directories on first run if they do not exist:

```bash
mkdir -p /Volumes/Transcend/edgar \
         /Volumes/Transcend/mongo-data \
         /Volumes/Transcend/kafka-data \
         /Volumes/Transcend/pgvector-data
```

## Quick start

```bash
git clone <this-repo>
cd sec-edgar-filings-rag-demo

cp .env.example .env
# Edit .env — SEC requires a real name + email in SEC_USER_AGENT

docker compose pull
docker compose up -d
```

Verify services:

```bash
docker compose ps
curl http://localhost:8080/health
```

Open the RAG UI: **http://localhost:8095**

## End-to-end demo

The stack starts infrastructure and the filings API, but **does not download filings automatically**. Run the S&P 500 batch jobs to populate data:

```bash
# Refresh S&P 500 ticker universe in MongoDB
docker compose run --rm sec-edgar-filings python -m app.jobs.refresh_sp500

# Download recent filings (publishes to Kafka as each filing is registered)
docker compose run --rm sec-edgar-filings python -m app.jobs.download_sp500

# Shorter lookback for a quicker demo
docker compose run --rm sec-edgar-filings python -m app.jobs.download_sp500 -- --lookback-days 30 -v
```

Watch ETL progress:

```bash
docker compose logs -f sec-edgar-filings-to-pgvector
```

Once chunks are loaded, try the UI at http://localhost:8095. Example questions:

- *Do you know if the Adobe board approved a buyback program?*
- *Who are the elected directors in Goldman Sachs?*

Optional filters: ticker (`GS`), form type (`10-K`).

## Services and ports

| Compose service | Image | Host port | Notes |
|-----------------|-------|-----------|-------|
| `sec-edgar-filings` | `sanjuthomas/sec-edgar-filings:latest` | **8080** | EDGAR downloader API |
| `mongo` | `mongo:7` | **27017** | Filing metadata |
| `kafka` | `apache/kafka:3.9.0` | **9092** | `filings` topic |
| `pgvector` | `pgvector/pgvector:pg17` | **5433** | DB `edgar`, user `postgres` |
| `sec-edgar-filings-to-pgvector` | `sanjuthomas/sec-edgar-filings-to-pgvector:latest` | — | Kafka consumer / ETL |
| `sec-edgar-filings-semantic-search-ui` | `sanjuthomas/sec-edgar-filings-semantic-search-ui:latest` | **8095** | RAG search UI |
| `kafka-web-clients` | `sanjuthomas/kafka-web-clients:latest` | **8081** | Debug only (`debug` profile) |

Startup order is enforced with healthchecks: MongoDB and Kafka become healthy first, then the downloader API; pgvector must be healthy before the ETL consumer and UI start.

## Host data paths

Bind mounts are hard-coded to the Transcend drive:

| Host path | Used by |
|-----------|---------|
| `/Volumes/Transcend/edgar` | Downloaded filing `.htm` files (read-write for downloader, read-only for ETL) |
| `/Volumes/Transcend/mongo-data` | MongoDB data |
| `/Volumes/Transcend/kafka-data` | Kafka broker logs |
| `/Volumes/Transcend/pgvector-data` | PostgreSQL / pgvector data |

The EDGAR mount target **inside containers is the same path** (`/Volumes/Transcend/edgar`) so `local_path` values in MongoDB and Kafka events match files on disk.

## Configuration

Copy `.env.example` to `.env` and set:

```bash
SEC_USER_AGENT=Your Name your.email@example.com
```

The SEC requires a descriptive `User-Agent` on every programmatic request. A placeholder is used if unset, which may lead to throttling.

Ollama is **not** started by this compose file. The UI reaches it at `http://host.docker.internal:11434` (your local Ollama install).

## Kafka debug UI

Start the optional debug profile to watch messages on the `filings` topic:

```bash
docker compose --profile debug up -d
```

Open **http://localhost:8081** and configure:

| Field | Value |
|-------|-------|
| Bootstrap servers | `kafka:9092` |
| Topic | `filings` |

The kafka-web-clients container runs on the same Compose network, so use the service hostname `kafka`, not `localhost`.

## Useful commands

```bash
# Start / stop
docker compose up -d
docker compose down

# With debug UI
docker compose --profile debug up -d
docker compose --profile debug down

# Logs
docker compose logs -f
docker compose logs -f sec-edgar-filings sec-edgar-filings-to-pgvector

# Pull latest images
docker compose pull && docker compose up -d

# Query stored filings via API
curl http://localhost:8080/api/filings/GS
```

## Troubleshooting

| Symptom | Check |
|---------|-------|
| Downloader can't write filings | Transcend mounted? `/Volumes` shared in Docker Desktop? |
| ETL skips filings | File exists at `local_path` in MongoDB under `/Volumes/Transcend/edgar`? |
| UI returns no results | ETL logs show chunks loaded? `docker compose logs sec-edgar-filings-to-pgvector` |
| UI errors on answer generation | Ollama running? `curl http://localhost:11434/api/tags` |
| Kafka debug can't connect | Use `kafka:9092` (not `localhost:9092`) from the debug container |

## Related projects

| Project | Docker Hub |
|---------|------------|
| [sec-edgar-filings](https://github.com/sanjuthomas/sec-edgar-filings) | [`sanjuthomas/sec-edgar-filings`](https://hub.docker.com/r/sanjuthomas/sec-edgar-filings) |
| [sec-edgar-filings-to-pgvector](https://github.com/sanjuthomas/sec-edgar-filings-to-pgvector) | [`sanjuthomas/sec-edgar-filings-to-pgvector`](https://hub.docker.com/r/sanjuthomas/sec-edgar-filings-to-pgvector) |
| [sec-edgar-filings-semantic-search-ui](https://github.com/sanjuthomas/sec-edgar-filings-semantic-search-ui) | [`sanjuthomas/sec-edgar-filings-semantic-search-ui`](https://hub.docker.com/r/sanjuthomas/sec-edgar-filings-semantic-search-ui) |
| [kafka-web-clients](https://github.com/sanjuthomas/kafka-web-clients) | [`sanjuthomas/kafka-web-clients`](https://hub.docker.com/r/sanjuthomas/kafka-web-clients) |

## License

See individual component repositories for license terms.
