# Educational FastAPI Demo

**This is a purely academic project created for learning purposes.** It demonstrates how to build an asynchronous web service with FastAPI, WebSocket tunneling, HTTP proxying, and a Telegram bot integration.

---

## ⚠️ Important Disclaimer

This repository **does not provide**, **does not endorse**, and **has no relation to** any VPN services, censorship circumvention tools, or real-world networking infrastructure.

All implementations are **minimal, non‑functional stubs** designed solely to illustrate programming concepts such as:
- Handling WebSocket connections in FastAPI
- Building a simple HTTP proxy with `httpx`
- Creating a Telegram bot with long polling
- Managing asynchronous I/O and background tasks
- Using environment variables for configuration

The included "VLESS" label is an **abstract placeholder** — it does not implement any actual tunneling, encryption, or routing logic. The UUID is **static and pre‑defined**, and there is **no user management, authentication, or persistent storage** of any kind.

---

## 🎯 Purpose

This project was developed as a **student exercise** to explore:
- FastAPI's async capabilities
- Real‑time communication via WebSockets
- Integration with Telegram's Bot API
- Deployment automation with Railway

It is **not intended for production use** and should **never be used to bypass network restrictions**.

---

## 📖 How It Works (At a Glance)

- A single **static UUID** (set via `VLESS_UUID` or auto‑generated) is used for the demo endpoint.
- Only WebSocket (`vless-ws`) is supported as a **dummy path** to showcase routing and streaming responses.
- The Telegram bot offers two commands:
  - `/config` – displays a placeholder link (this is **not functional** outside this demo).
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
uvicorn main:app --host 0.0.0.0 --port 8000
```

> **Note:** The Telegram bot is **mandatory** and will cause the application to exit if `TELEGRAM_BOT_TOKEN` or `TELEGRAM_ADMIN_IDS` are not set.

---

## 🤖 Telegram Bot (Demo Only)

The bot is a **lightweight demonstration** of the Telegram API. It does **not** manage any real configurations — it simply prints a pre‑defined text string.

- `/config` → shows a sample VLESS‑style link (format only, no actual connectivity).
- `/stats` → displays total request count and uptime from memory.

Access is restricted to numeric IDs listed in `TELEGRAM_ADMIN_IDS`.

---

## 🚀 Deployment on Railway

The included `railway.json` enforces required environment variables (`TELEGRAM_BOT_TOKEN` and `TELEGRAM_ADMIN_IDS`). Railway will prompt the user to enter these values before deployment.

- `VLESS_UUID` is optional and auto‑generated using Railway's `generator: "secret"`.
- The public domain (`RAILWAY_PUBLIC_DOMAIN`) is automatically set by Railway.
- The application binds to the port provided by Railway (`$PORT`).

---

## 📚 Educational Value

By studying this code, you can learn:
- How to structure a FastAPI project with a separate router for WebSocket logic
- How to integrate WebSockets and HTTP streaming
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
