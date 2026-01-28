# SQL Q&A Pipeline

Downloads a SQL dump from the Simulator API, parses Q&A records by category,
writes `category_<id>.json` files, and uploads each file to OpenAI.

## Requirements

- Node.js 18+

Install dependencies:

```bash
npm install
```

## Environment variables

Simulator API:

- `SIMULATOR_DOWNLOAD_URL` (required)
- `SIMULATOR_DOWNLOAD_METHOD` (default: `POST`)
- `SIMULATOR_DOWNLOAD_JSON_TEMPLATE` (optional JSON template, uses `$accId` and `$fileName`)
- `SIMULATOR_TOKEN` (optional, sent as `Authorization: Bearer ...`)
- `SIMULATOR_API_KEY` (optional, sent as `X-API-Key`)
- `SIMULATOR_AUTH_HEADER` + `SIMULATOR_AUTH_VALUE` (optional custom auth header)
- `SIMULATOR_AUTH_PREFIX` + `SIMULATOR_AUTH_VALUE` (optional prefix-based auth)

OpenAI API:

- `OPENAI_API_KEY` (required)
- `OPENAI_FILE_PURPOSE` (default: `assistants`)
- `OPENAI_VECTOR_STORE_NAME` (default: `kb-<timestamp>`)
- `OPENAI_BETA_HEADER` (optional, e.g. `assistants=v2`)
- `OPENAI_MAX_RETRIES` (default: `5`)
- `OPENAI_RETRY_BASE_DELAY` (default: `1.0`)

## Run

```bash
node main.js --input payload.json
```

Optional flags:

- `--output-dir` (default: current directory)
- `--download-path` (default: system temp `input.sql`)
