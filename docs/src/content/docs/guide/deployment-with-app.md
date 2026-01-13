---
title: Deploying with Your App
description: Running walrust alongside your application
---

Walrust runs as a sidecar process next to your app. Here's how to set that up without shooting yourself in the foot.

## Basic Setup

Your app and walrust share access to the same SQLite file:

```
┌─────────────────────────────────────┐
│            Your Server              │
│                                     │
│  ┌─────────┐      ┌──────────┐     │
│  │ Your App│      │ Walrust  │     │
│  └────┬────┘      └────┬─────┘     │
│       │                │           │
│       ▼                ▼           │
│    ┌──────────────────────┐        │
│    │      app.db          │        │
│    │      app.db-wal      │        │
│    └──────────────────────┘        │
└─────────────────────────────────────┘
```

## Docker Compose

```yaml
services:
  app:
    image: your-app
    volumes:
      - ./data:/data

  walrust:
    image: ghcr.io/russellromney/walrust
    volumes:
      - ./data:/data
    environment:
      - AWS_ACCESS_KEY_ID
      - AWS_SECRET_ACCESS_KEY
      - AWS_ENDPOINT_URL_S3
    command: watch /data/app.db -b my-bucket
```

## systemd

If you're not using containers:

```ini
[Unit]
Description=Walrust backup
After=your-app.service

[Service]
ExecStart=/usr/local/bin/walrust watch /var/lib/app/app.db -b my-bucket
Restart=always
Environment=AWS_ACCESS_KEY_ID=xxx
Environment=AWS_SECRET_ACCESS_KEY=xxx

[Install]
WantedBy=multi-user.target
```

## Gotchas

### SQLite Must Be in WAL Mode

Walrust only works with WAL mode. If your database isn't in WAL mode, walrust has nothing to watch.

```sql
PRAGMA journal_mode=WAL;
```

Most ORMs do this automatically. If yours doesn't, run this once on database creation.

### Encryption Will Break Everything

**SQLite Encryption Extension (SEE), SQLCipher, etc. - don't use them with walrust.**

Encrypted databases have encrypted pages. Walrust can back them up, but the pages are meaningless without the key. Worse, some encryption schemes modify the WAL format in ways that break LTX encoding entirely.

If you need encryption:
- Encrypt at the S3 layer (server-side encryption)
- Encrypt the volume/disk
- Don't encrypt the SQLite file itself

### Compression Will Also Break Everything

Same deal. If you're using a SQLite extension that compresses pages, the LTX format won't understand them.

Just... don't. SQLite pages compress terribly anyway (lots of internal pointers and metadata). You won't save much space, and you'll break replication.

### File Permissions

Both your app and walrust need read access to the database and WAL files. If you're running walrust as a different user, make sure permissions allow it.

### Busy Databases

Walrust is designed to work alongside busy databases. It reads the WAL file without blocking your app's writes. However, if your WAL file grows very large (gigabytes), consider tuning SQLite's `wal_autocheckpoint` to checkpoint more frequently.

## Fly.io

Walrust works great with [Fly.io](https://fly.io) and [Tigris](https://tigris.dev):

```bash
# Set secrets
fly secrets set AWS_ACCESS_KEY_ID=tid_xxx
fly secrets set AWS_SECRET_ACCESS_KEY=tsec_xxx
fly secrets set AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev

# Run in your Dockerfile or as a process
walrust watch /data/app.db -b my-bucket
```

See [Deployment](/config/deployment/) for more options.
