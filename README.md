# Mailcow Sync Service (Authentik → Mailcow)

This service automatically syncs users and aliases from Authentik to Mailcow.

## ✨ Features

- Auto-create mailboxes
- Sync aliases
- Remove stale aliases
- Idempotent (safe to run repeatedly)
- Supports multiple domains
- Docker-ready

## 🧠 Architecture

Authentik → Sync Service → Mailcow API

## ⚙️ Setup

### 1. Clone repo

```bash
git clone https://github.com/YOUR_USERNAME/mailcow-sync.git
cd mailcow-sync
