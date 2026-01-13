---
title: Environment Variables
description: Configure walrust with environment variables
---

Walrust reads configuration from environment variables and CLI options.

## Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `AWS_ACCESS_KEY_ID` | S3 access key | `AKIA...` or `tid_...` |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key | `wJalr...` or `tsec_...` |

## Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_ENDPOINT_URL_S3` | Custom S3 endpoint for Tigris, R2, MinIO, etc. | AWS S3 |
| `AWS_REGION` | AWS region | `auto` |
| `RUST_LOG` | Log level and filtering | `walrust=info` |

## Setting Variables

### Shell (temporary)

```bash
export AWS_ACCESS_KEY_ID=tid_xxxxx
export AWS_SECRET_ACCESS_KEY=tsec_xxxxx
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
```

### .env file (development)

```bash
# .env
AWS_ACCESS_KEY_ID=tid_xxxxx
AWS_SECRET_ACCESS_KEY=tsec_xxxxx
AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
```

Load with:
```bash
source .env
# or use direnv, dotenv, etc.
```

### systemd (production)

```ini
[Service]
Environment=AWS_ACCESS_KEY_ID=tid_xxxxx
Environment=AWS_SECRET_ACCESS_KEY=tsec_xxxxx
Environment=AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
```

### Docker

```yaml
environment:
  AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
  AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
  AWS_ENDPOINT_URL_S3: https://fly.storage.tigris.dev
```

### Fly.io

```bash
fly secrets set AWS_ACCESS_KEY_ID=tid_xxxxx
fly secrets set AWS_SECRET_ACCESS_KEY=tsec_xxxxx
```

## Variable Precedence

CLI options override environment variables:

```bash
# Uses environment variable
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
walrust list --bucket my-bucket

# CLI option overrides environment
walrust list --bucket my-bucket --endpoint http://localhost:9000
```
