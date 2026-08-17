# Educational FastAPI Demonstration  

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

The included “VLESS” and “XHTTP” labels are **abstract placeholders** — they do not implement any actual tunneling, encryption, or routing logic. The UUID is **static and pre‑defined**, and there is **no user management, authentication, or persistent storage** of any kind.  

---

## 🎯 Purpose  

This project was developed as a **student exercise** to explore:  
- FastAPI’s async capabilities  
- Real‑time communication via WebSockets  
- Integration with Telegram’s Bot API  
- Deployment automation with Railway and Docker  

It is **not intended for production use** and should **never be used to bypass network restrictions**.  

---

## 📖 How It Works (At a Glance)  

- A single **static UUID** (set via `VLESS_UUID` or auto‑generated) is used for all demo endpoints.  
- Two abstract “protocols” (`vless-ws` and `xhttp-*`) are defined as **dummy paths** to showcase routing and streaming responses.  
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
uvicorn main:app --host 0.0.0.0 --port 8000
```

> **Note:** The Telegram bot is **optional** and only works if valid tokens are provided. Without them, the service runs as a plain FastAPI app.

---

## 🤖 Telegram Bot (Demo Only)  

The bot is a **lightweight demonstration** of the Telegram API. It does **not** manage any real configurations — it simply prints pre‑defined text strings.  

- `/config` → shows sample VLESS/XHTTP‑style links (format only, no actual connectivity).  
- `/stats` → displays total request count and uptime from memory.  

Access is restricted to numeric IDs listed in `TELEGRAM_ADMIN_IDS`.

---

## 🚀 Deployment on Railway  

The included `railway.json` automatically sets up environment variables. Railway’s `generator: "secret"` creates random values for `VLESS_UUID` and `SECRET_KEY`, ensuring each deployment is unique — **purely for demo isolation**.  

---

## 📚 Educational Value  

By studying this code, you can learn:  
- How to structure a FastAPI project with multiple routers  
- How to integrate WebSockets and HTTP streaming  
- How to write a long‑polling Telegram bot  
- How to manage environment variables in cloud platforms  
- How to write a `Dockerfile` and deploy on Railway  

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
