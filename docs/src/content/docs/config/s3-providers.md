---
title: S3 Providers
description: Configure walrust with different S3-compatible storage providers
---

Walrust works with any S3-compatible storage. Here's how to configure popular providers.

## Tigris (Fly.io)

Tigris is an S3-compatible object store from Fly.io with global distribution and no egress fees.

```bash
# Create a bucket
fly storage create my-walrust-bucket

# Credentials are shown after creation
export AWS_ACCESS_KEY_ID=tid_xxxxxxxxxxxxx
export AWS_SECRET_ACCESS_KEY=tsec_xxxxxxxxxxxxx
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
```

**Characteristics:**
- No egress fees
- Global distribution (data replicated across regions)
- Native Fly.io integration via `fly storage` CLI
- S3-compatible API

## AWS S3

```bash
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
# No endpoint needed for AWS S3
```

**Recommended IAM Policy:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-walrust-bucket",
        "arn:aws:s3:::my-walrust-bucket/*"
      ]
    }
  ]
}
```

**Best practices:**
- Use IAM roles instead of access keys when possible
- Enable bucket versioning
- Enable server-side encryption

## Cloudflare R2

R2 offers zero egress fees and S3 compatibility.

```bash
export AWS_ACCESS_KEY_ID=...  # R2 access key
export AWS_SECRET_ACCESS_KEY=...  # R2 secret key
export AWS_ENDPOINT_URL_S3=https://<account-id>.r2.cloudflarestorage.com
```

Get your account ID and create API tokens in the Cloudflare dashboard under R2.

## MinIO (Self-Hosted)

MinIO is a self-hosted S3-compatible object store.

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_URL_S3=http://localhost:9000
```

**Docker Compose with MinIO:**

```yaml
version: '3.8'
services:
  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio-data:/data
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin

  walrust:
    image: walrust
    command: watch /data/app.db --bucket my-bucket
    environment:
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
      AWS_ENDPOINT_URL_S3: http://minio:9000
    depends_on:
      - minio

volumes:
  minio-data:
```

## Backblaze B2

```bash
export AWS_ACCESS_KEY_ID=...  # B2 application key ID
export AWS_SECRET_ACCESS_KEY=...  # B2 application key
export AWS_ENDPOINT_URL_S3=https://s3.us-west-002.backblazeb2.com
```

Replace `us-west-002` with your bucket's region.

## DigitalOcean Spaces

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ENDPOINT_URL_S3=https://nyc3.digitaloceanspaces.com
```

Replace `nyc3` with your Spaces region.

## Wasabi

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ENDPOINT_URL_S3=https://s3.wasabisys.com
# Or region-specific: https://s3.us-east-1.wasabisys.com
```

## Testing Your Configuration

Verify your S3 connection:

```bash
# List databases (should work with valid credentials)
walrust list --bucket my-bucket

# Or test with AWS CLI
aws s3 ls s3://my-bucket --endpoint-url $AWS_ENDPOINT_URL_S3
```
