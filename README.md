# Educational FastAPI Demo with XHTTP

**This is a purely academic project created for learning purposes.** It demonstrates how to build an asynchronous web service with FastAPI, WebSocket and XHTTP tunneling, HTTP proxying, and a Telegram bot integration.

---

## ⚠️ Important Disclaimer

This repository **does not provide**, **does not endorse**, and **has no relation to** any VPN services, censorship circumvention tools, or real‑world networking infrastructure.

All implementations are **minimal, non‑functional stubs** designed solely to illustrate programming concepts such as:
- Handling WebSocket connections in FastAPI
- Implementing XHTTP transport (packet‑up and stream‑up modes)
- Building a simple HTTP proxy with `httpx`
- Creating a Telegram bot with long polling
- Managing asynchronous I/O and background tasks
- Using environment variables for configuration

The included "VLESS" and "XHTTP" labels are **abstract placeholders** — they do not implement any actual tunneling, encryption, or routing logic. The UUID is **static and pre‑defined**, and there is **no user management, authentication, or persistent storage** of any kind.

---

## 🎯 Purpose

This project was developed as a **student exercise** to explore:
- FastAPI's async capabilities
- Real‑time communication via WebSockets
- XHTTP transport with adaptive flow control (AIMD)
- Integration with Telegram's Bot API
- Deployment automation with Railway

It is **not intended for production use** and should **never be used to bypass network restrictions**.

---

## 📖 How It Works (At a Glance)

- A single **static UUID** (set via `VLESS_UUID` or auto‑generated) is used for demo endpoints.
- The entire code is contained in a single file (`main.py`) for easy experimentation.
- Two abstract transports are supported:
  - **WebSocket** (`/ws/{uuid}`) — for connection‑oriented streaming
  - **XHTTP** (`/xhttp-siz10/{mode}/{uuid}/{session_id}`) — with two modes:
    - `packet‑up` — packet‑based upload with sequencing
    - `stream‑up` — continuous stream upload with adaptive flow control (AIMD)
- The Telegram bot offers two commands:
  - `/config` – displays placeholder links (these are **not functional** outside this demo).
  - `/stats` – shows in‑memory request counters (reset on restart).
- The HTTP proxy forwards requests to external URLs **without** any filtering or modification — it is a basic educational example.

---

## 🛠️ Local Setup

```bash
git clone https://github.com/your-username/x4g-lite.git
cd x4g-lite
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN="your_test_token"
export TELEGRAM_ADMIN_IDS="123456"
export XHTTP_TARGET_HOST="example.com"   # optional, default: localhost
export XHTTP_TARGET_PORT="443"           # optional, default: 443
uvicorn main:app --host 0.0.0.0 --port 8000
```

> **Note:** The Telegram bot is **mandatory** and will cause the application to exit if `TELEGRAM_BOT_TOKEN` or `TELEGRAM_ADMIN_IDS` are not set.

---

## 🤖 Telegram Bot (Demo Only)

The bot is a **lightweight demonstration** of the Telegram API. It does **not** manage any real configurations — it simply prints a pre‑defined text string.

- `/config` → shows sample VLESS‑style and XHTTP‑style links (format only, no actual connectivity).
- `/stats` → displays total request count and uptime from memory.

Access is restricted to numeric IDs listed in `TELEGRAM_ADMIN_IDS`.

---

## 🚀 Deployment on Railway

The included `railway.json` enforces required environment variables (`TELEGRAM_BOT_TOKEN` and `TELEGRAM_ADMIN_IDS`). Railway will prompt the user to enter these values before deployment.

- `VLESS_UUID` is optional and auto‑generated using Railway's `generator: "secret"`.
- `XHTTP_TARGET_HOST` and `XHTTP_TARGET_PORT` are optional and default to `localhost:443`.
- The public domain (`RAILWAY_PUBLIC_DOMAIN`) is automatically set by Railway.
- The application binds to the port provided by Railway (`$PORT`).
- The `railway.json` includes health check and restart policies for robust deployment.

---

## 📚 Educational Value

By studying this code, you can learn:
- How to structure a FastAPI project (in a single file for simplicity) with WebSocket and XHTTP routing
- How to implement adaptive flow control (AIMD) for streaming transports
- How to manage packet‑based sequencing and session state
- How to write a long‑polling Telegram bot
- How to manage environment variables in cloud platforms
- How to deploy on Railway with a `railway.json` configuration

---

## ❌ What This Project Is **Not**

- ❌ A VPN or proxy service
- ❌ A censorship circumvention tool
- ❌ A production‑grade application
- ❌ A user management or subscription system
- ❌ A tool for bypassing any network policies

---

## 📄 License

MIT License — free for educational and research use only.

---

**Developer:** Computer Engineering student — created for learning and experimentation.
**Contact:** [Telegram](https://t.me/Farajian2004f) for academic questions only.
