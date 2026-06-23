"""Streamlit cyber dashboard for triggering and monitoring all boot services."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from streamlit_autorefresh import st_autorefresh

DATA_BOOT_ROOT_URL = "http://127.0.0.1:8001/"
REQUEST_TIMEOUT_SECONDS = 10.0
AUTO_REFRESH_INTERVAL_MS = 10_000
MAX_LOG_ITEMS = 30
HEALTH_TIMEOUT_SECONDS = 3.0

SERVICE_HEALTH_URLS = {
    "data_boot": "http://127.0.0.1:8001/health",
    "forecast_boot": "http://127.0.0.1:8002/health",
    "execution_boot": "http://127.0.0.1:8003/health",
}

SERVICE_PIPELINE_URLS = {
    "data_boot": "http://127.0.0.1:8001/api/v1/data/pipeline-status",
    "forecast_boot": "http://127.0.0.1:8002/api/v1/forecast/pipeline-status",
    "execution_boot": "http://127.0.0.1:8003/api/v1/execution/pipeline-status",
}

LOCAL_HERO_IMAGE = Path(__file__).resolve().parent / "assets" / "neon_power_grid.svg"
LOCAL_CREW_IMAGE = Path(__file__).resolve().parent / "assets" / "pirate_war_original.svg"


def short_text(value: Any, max_len: int = 24) -> str:
    """Return compact text for dashboard cards."""

    if value is None:
        return "-"
    text = str(value)
    return text if len(text) <= max_len else f"{text[: max_len - 3]}..."


def call_data_boot(source: str) -> dict[str, Any]:
    """Send one trigger request to data_boot and return normalized result."""

    timestamp = datetime.now().strftime("%H:%M:%S")
    try:
        response = httpx.get(DATA_BOOT_ROOT_URL, timeout=REQUEST_TIMEOUT_SECONDS)
        try:
            payload: Any = response.json()
        except ValueError:
            payload = response.text

        return {
            "ok": response.is_success,
            "source": source,
            "status_code": response.status_code,
            "time": timestamp,
            "payload": payload,
            "error": "",
        }
    except httpx.HTTPError as exc:
        return {
            "ok": False,
            "source": source,
            "status_code": 503,
            "time": timestamp,
            "payload": {},
            "error": str(exc),
        }


def fetch_health_snapshot() -> dict[str, dict[str, Any]]:
    """Check health endpoints for all services and return normalized status map."""

    snapshot: dict[str, dict[str, Any]] = {}
    now_text = datetime.now().strftime("%H:%M:%S")

    for service_name, health_url in SERVICE_HEALTH_URLS.items():
        try:
            response = httpx.get(health_url, timeout=HEALTH_TIMEOUT_SECONDS)
            payload: dict[str, Any]
            try:
                payload = response.json()
            except ValueError:
                payload = {"raw": response.text}

            snapshot[service_name] = {
                "ok": response.is_success,
                "status_code": response.status_code,
                "url": health_url,
                "time": now_text,
                "payload": payload,
                "error": "",
            }
        except httpx.HTTPError as exc:
            snapshot[service_name] = {
                "ok": False,
                "status_code": 503,
                "url": health_url,
                "time": now_text,
                "payload": {},
                "error": str(exc),
            }

    return snapshot


def fetch_pipeline_snapshot() -> dict[str, dict[str, Any]]:
    """Fetch pipeline-status payloads from all boot services."""

    snapshot: dict[str, dict[str, Any]] = {}
    now_text = datetime.now().strftime("%H:%M:%S")

    for service_name, pipeline_url in SERVICE_PIPELINE_URLS.items():
        try:
            response = httpx.get(pipeline_url, timeout=HEALTH_TIMEOUT_SECONDS)
            payload: dict[str, Any]
            try:
                payload = response.json()
            except ValueError:
                payload = {"raw": response.text}

            snapshot[service_name] = {
                "ok": response.is_success,
                "status_code": response.status_code,
                "url": pipeline_url,
                "time": now_text,
                "payload": payload,
                "error": "",
            }
        except httpx.HTTPError as exc:
            snapshot[service_name] = {
                "ok": False,
                "status_code": 503,
                "url": pipeline_url,
                "time": now_text,
                "payload": {},
                "error": str(exc),
            }

    return snapshot


def push_log(entry: dict[str, Any]) -> None:
    """Insert trigger logs in reverse chronological order."""

    st.session_state.logs.insert(0, entry)
    del st.session_state.logs[MAX_LOG_ITEMS:]


def trigger_once(source: str) -> None:
    """Trigger one data_boot update request and store state."""

    result = call_data_boot(source)
    st.session_state.last_result = result
    push_log(result)


def append_history() -> None:
    """Append one time-point metrics sample for realtime charts."""

    now_text = datetime.now().strftime("%H:%M:%S")
    health_snapshot = st.session_state.health_snapshot
    healthy_count = sum(1 for item in health_snapshot.values() if item["ok"])
    total_count = len(health_snapshot) or 1
    health_score = healthy_count / total_count

    last = st.session_state.last_result
    trigger_ok = 1.0 if last and last.get("ok") else 0.0

    st.session_state.health_history.append(
        {
            "time": now_text,
            "health_score": health_score,
            "healthy_count": healthy_count,
            "trigger_ok": trigger_ok,
        }
    )
    st.session_state.health_history = st.session_state.health_history[-120:]


st.set_page_config(page_title="Power Trading Cyber Panel", page_icon="⚡", layout="wide")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Audiowide&family=Space+Grotesk:wght@400;600;700&display=swap');

    :root {
      --neon-cyan: #4ffcff;
      --neon-green: #6dff77;
      --neon-pink: #ff6bd6;
      --hot-orange: #ff9f38;
      --danger-red: #ff5a78;
      --glass-bg: rgba(6, 19, 46, 0.55);
      --glass-border: rgba(255, 255, 255, 0.22);
    }

    .stApp {
      background:
        radial-gradient(900px 500px at 8% -5%, rgba(79, 252, 255, 0.24), transparent 62%),
        radial-gradient(1100px 560px at 100% 5%, rgba(255, 107, 214, 0.2), transparent 55%),
        radial-gradient(1000px 540px at 50% 110%, rgba(109, 255, 119, 0.2), transparent 58%),
        linear-gradient(125deg, #030712 0%, #0a1133 40%, #150b3f 100%);
      color: #f4f7ff;
      font-family: 'Space Grotesk', 'Segoe UI', sans-serif;
    }

    .hero-wrap {
      border: 1px solid var(--glass-border);
      background: linear-gradient(120deg, rgba(79, 252, 255, 0.12), rgba(255, 107, 214, 0.12), rgba(109, 255, 119, 0.12));
      border-radius: 18px;
      padding: 20px 24px;
      box-shadow: 0 0 0 1px rgba(255,255,255,0.08) inset, 0 22px 50px rgba(0,0,0,0.45);
      animation: pulseGlow 4s ease-in-out infinite;
      margin-bottom: 12px;
    }

    .hero-title {
      font-family: 'Audiowide', 'Space Grotesk', sans-serif;
      font-size: clamp(1.5rem, 2.8vw, 2.35rem);
      margin: 0;
      letter-spacing: 0.04em;
    }

    .hero-subtitle {
      margin: 8px 0 0 0;
      color: #d9e7ff;
      font-size: 0.98rem;
      opacity: 0.92;
    }

    .neon-chip {
      display: inline-block;
      border-radius: 999px;
      padding: 0.24rem 0.72rem;
      margin: 0.35rem 0.45rem 0 0;
      border: 1px solid rgba(255,255,255,0.25);
      background: rgba(255,255,255,0.08);
      font-size: 0.82rem;
      color: #f0f5ff;
    }

    .stButton > button {
      border-radius: 999px;
      border: 0;
      color: #041312;
      font-weight: 800;
      letter-spacing: 0.03em;
      padding: 0.66rem 1.36rem;
      background: linear-gradient(100deg, var(--neon-cyan), var(--neon-green), var(--hot-orange));
      background-size: 220% 220%;
      box-shadow: 0 10px 34px rgba(79, 252, 255, 0.35);
      animation: gradientShift 5s ease infinite;
      transition: transform 180ms ease, filter 180ms ease;
    }

    .stButton > button:hover {
      transform: translateY(-2px) scale(1.03);
      filter: brightness(1.08);
    }

    .status-banner {
      border-radius: 14px;
      padding: 0.72rem 0.86rem;
      margin: 0.52rem 0 0.7rem 0;
      border: 1px solid rgba(255,255,255,0.28);
      background: rgba(9, 20, 48, 0.6);
      font-size: 0.93rem;
    }

    .status-ok {
      color: #9dffbb;
      box-shadow: 0 0 0 1px rgba(109, 255, 119, 0.25) inset;
    }

    .status-bad {
      color: #ffb5c2;
      box-shadow: 0 0 0 1px rgba(255, 90, 120, 0.25) inset;
    }

    .health-grid, .pipeline-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      margin: 0.35rem 0 1.2rem 0;
    }

    .health-card, .pipeline-card {
      border-radius: 14px;
      border: 1px solid var(--glass-border);
      background: var(--glass-bg);
      padding: 0.78rem;
      backdrop-filter: blur(8px);
      box-shadow: 0 15px 35px rgba(0,0,0,0.25);
    }

    .health-ok, .pipeline-ok {
      border-color: rgba(109, 255, 119, 0.8);
      box-shadow: 0 0 0 1px rgba(109, 255, 119, 0.2) inset;
    }

    .health-bad, .pipeline-bad {
      border-color: rgba(255, 90, 120, 0.8);
      box-shadow: 0 0 0 1px rgba(255, 90, 120, 0.2) inset;
    }

    .health-name {
      font-weight: 700;
      color: var(--neon-cyan);
      margin-bottom: 0.22rem;
      font-size: 1.02rem;
    }

    .health-ok-text {
      color: #8fffb2;
      font-weight: 700;
      margin-bottom: 0.14rem;
    }

    .health-bad-text {
      color: #ff97ab;
      font-weight: 700;
      margin-bottom: 0.14rem;
    }

    .log-line {
      font-family: 'Consolas', 'SFMono-Regular', monospace;
      font-size: 0.85rem;
      border-bottom: 1px dashed rgba(255, 255, 255, 0.12);
      padding: 0.42rem 0;
    }

    .notranslate {
      translate: no;
    }

        .status-orbs {
            display: flex;
            flex-wrap: wrap;
            gap: 14px;
            margin: 0.4rem 0 1.2rem 0;
        }

        .orb-card {
            min-width: 180px;
            display: flex;
            align-items: center;
            gap: 10px;
            border-radius: 12px;
            padding: 0.5rem 0.7rem;
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,0.2);
        }

        .orb {
            width: 24px;
            height: 24px;
            border-radius: 999px;
            position: relative;
            transform-style: preserve-3d;
        }

        .orb.ok {
            background: radial-gradient(circle at 30% 30%, #f4fff6 0%, #9dffb0 25%, #31de6f 72%, #1f9748 100%);
            box-shadow: 0 0 18px rgba(109, 255, 119, 0.65), inset -3px -3px 6px rgba(0,0,0,0.35);
            animation: pulseOk 1.7s infinite ease-in-out;
        }

        .orb.bad {
            background: radial-gradient(circle at 30% 30%, #fff1f4 0%, #ff9fb3 25%, #ff5a78 72%, #bd2041 100%);
            box-shadow: 0 0 18px rgba(255, 90, 120, 0.65), inset -3px -3px 6px rgba(0,0,0,0.35);
            animation: pulseBad 1.35s infinite ease-in-out;
        }

        .orb-label {
            font-size: 0.9rem;
        }

        @keyframes pulseOk {
            0%, 100% { transform: scale(1); }
            50% { transform: scale(1.12); }
        }

        @keyframes pulseBad {
            0%, 100% { transform: scale(1); }
            50% { transform: scale(1.16); }
        }

    @keyframes gradientShift {
      0% { background-position: 0% 50%; }
      50% { background-position: 100% 50%; }
      100% { background-position: 0% 50%; }
    }

    @keyframes pulseGlow {
      0% { box-shadow: 0 0 0 1px rgba(255,255,255,0.08) inset, 0 22px 50px rgba(0,0,0,0.45); }
      50% { box-shadow: 0 0 0 1px rgba(255,255,255,0.14) inset, 0 22px 58px rgba(79,252,255,0.14); }
      100% { box-shadow: 0 0 0 1px rgba(255,255,255,0.08) inset, 0 22px 50px rgba(0,0,0,0.45); }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if "logs" not in st.session_state:
    st.session_state.logs = []
if "last_result" not in st.session_state:
    st.session_state.last_result = None
if "last_auto_count" not in st.session_state:
    st.session_state.last_auto_count = 0
if "initialized" not in st.session_state:
    st.session_state.initialized = False
if "health_snapshot" not in st.session_state:
    st.session_state.health_snapshot = {}
if "pipeline_snapshot" not in st.session_state:
    st.session_state.pipeline_snapshot = {}
if "health_history" not in st.session_state:
    st.session_state.health_history = []

refresh_count = st_autorefresh(interval=AUTO_REFRESH_INTERVAL_MS, key="auto-refresh")

st.session_state.health_snapshot = fetch_health_snapshot()
st.session_state.pipeline_snapshot = fetch_pipeline_snapshot()
overall_ok = all(item["ok"] for item in st.session_state.health_snapshot.values())

if not st.session_state.initialized:
    trigger_once("initial-load")
    st.session_state.initialized = True

if refresh_count > st.session_state.last_auto_count:
    st.session_state.last_auto_count = refresh_count
    trigger_once("auto-10s")

append_history()

st.markdown(
    """
    <div class='hero-wrap'>
      <h1 class='hero-title'>Power Trading Neon Control Deck</h1>
      <p class='hero-subtitle'>三大 boot 链路实时监控 + 自动触发 + 手动强制触发，10 秒刷新一轮。</p>
      <span class='neon-chip'>Data Stream</span>
      <span class='neon-chip'>Forecast Engine</span>
      <span class='neon-chip'>Execution Guard</span>
      <span class='neon-chip'>Cyber Dashboard</span>
    </div>
    """,
    unsafe_allow_html=True,
)

left_col, mid_col, right_col = st.columns([1.25, 1, 1], gap="large")

with left_col:
    st.markdown("### Trigger Control")
    st.caption(f"目标: GET {DATA_BOOT_ROOT_URL}")
    if st.button("立即触发一次", use_container_width=False):
        trigger_once("manual-click")

    last = st.session_state.last_result
    if last is None:
        st.markdown("<div class='status-banner'>等待首次触发...</div>", unsafe_allow_html=True)
    elif last["ok"]:
        st.markdown(
            (
                "<div class='status-banner status-ok'>"
                f"触发成功 | source={last['source']} | status={last['status_code']} | time={last['time']}"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            (
                "<div class='status-banner status-bad'>"
                f"触发失败 | source={last['source']} | status={last['status_code']} | error={last['error']}"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

with mid_col:
    st.markdown("### Visual Pulse")
    st.image(str(LOCAL_HERO_IMAGE), caption="Grid Energy Flow", use_container_width=True)

with right_col:
    st.markdown("### Overall")
    if overall_ok:
        st.success("ONLINE")
    else:
        st.error("DEGRADED")
    st.image(str(LOCAL_HERO_IMAGE), caption="Neon Mesh", use_container_width=True)

components.html(
        """
        <canvas id='particles' width='1200' height='170' style='width:100%;height:170px;border-radius:12px;background:linear-gradient(90deg,#111833,#1b1244,#102744)'></canvas>
        <script>
            const canvas = document.getElementById('particles');
            const ctx = canvas.getContext('2d');
            const dots = Array.from({length: 75}, () => ({
                x: Math.random() * canvas.width,
                y: Math.random() * canvas.height,
                vx: (Math.random() - 0.5) * 0.6,
                vy: (Math.random() - 0.5) * 0.6,
                r: Math.random() * 2 + 1
            }));

            function draw() {
                ctx.clearRect(0, 0, canvas.width, canvas.height);
                for (const d of dots) {
                    d.x += d.vx;
                    d.y += d.vy;
                    if (d.x < 0 || d.x > canvas.width) d.vx *= -1;
                    if (d.y < 0 || d.y > canvas.height) d.vy *= -1;

                    const g = ctx.createRadialGradient(d.x, d.y, 0, d.x, d.y, 10);
                    g.addColorStop(0, 'rgba(110,255,190,0.9)');
                    g.addColorStop(1, 'rgba(110,255,190,0)');
                    ctx.fillStyle = g;
                    ctx.beginPath();
                    ctx.arc(d.x, d.y, 10, 0, Math.PI * 2);
                    ctx.fill();
                }
                requestAnimationFrame(draw);
            }
            draw();
        </script>
        """,
        height=180,
)

st.markdown("### Fleet Status Orbs")
orb_html: list[str] = []
for service_name, health in st.session_state.health_snapshot.items():
        orb_class = "ok" if health["ok"] else "bad"
        orb_state = "ONLINE" if health["ok"] else "OFFLINE"
        orb_html.append(
                (
                        "<div class='orb-card'>"
                        f"<div class='orb {orb_class}'></div>"
                        f"<div class='orb-label'>{service_name} | {orb_state}</div>"
                        "</div>"
                )
        )
st.markdown(f"<div class='status-orbs'>{''.join(orb_html)}</div>", unsafe_allow_html=True)

st.markdown("### Original Pirate War Crew (原创)")
st.image(str(LOCAL_CREW_IMAGE), caption="Original manga-inspired pirate battle crew", use_container_width=True)

st.markdown("### Service Health Matrix")
health_cards: list[str] = []
for service_name, health in st.session_state.health_snapshot.items():
    card_class = "health-ok" if health["ok"] else "health-bad"
    status_class = "health-ok-text" if health["ok"] else "health-bad-text"
    status_text = "正常" if health["ok"] else "异常"
    health_payload = health.get("payload") or {}
    service_display = health_payload.get("service", service_name)
    status_display = health_payload.get("status", "-")
    error_line = f"<div>error={short_text(health['error'], 42)}</div>" if health["error"] else ""

    health_cards.append(
        (
            f"<div class='health-card {card_class} notranslate' translate='no'>"
            f"<div class='health-name'>{service_name}</div>"
            f"<div class='{status_class}'>{status_text}</div>"
            f"<div>service={service_display}</div>"
            f"<div>health_status={status_display}</div>"
            f"<div>status={health['status_code']}</div>"
            f"<div>updated={health['time']}</div>"
            f"{error_line}"
            "</div>"
        )
    )

st.markdown(f"<div class='health-grid'>{''.join(health_cards)}</div>", unsafe_allow_html=True)

st.markdown("### Pipeline Telemetry")
pipeline_cards: list[str] = []
for service_name, entry in st.session_state.pipeline_snapshot.items():
    card_class = "pipeline-ok" if entry["ok"] else "pipeline-bad"
    payload = entry.get("payload") or {}
    last_consumed = short_text(payload.get("last_consumed_event_id"))
    last_published = short_text(payload.get("last_published_event_id"))
    last_feedback = short_text(payload.get("last_feedback_event_id"))
    error_html = f"<div>error={short_text(entry['error'], 42)}</div>" if entry["error"] else ""

    pipeline_cards.append(
        (
            f"<div class='pipeline-card {card_class} notranslate' translate='no'>"
            f"<div class='health-name'>{service_name}</div>"
            f"<div>status={entry['status_code']}</div>"
            f"<div>last_consumed={last_consumed}</div>"
            f"<div>last_published={last_published}</div>"
            f"<div>last_feedback={last_feedback}</div>"
            f"<div>updated={entry['time']}</div>"
            f"{error_html}"
            "</div>"
        )
    )

st.markdown(f"<div class='pipeline-grid'>{''.join(pipeline_cards)}</div>", unsafe_allow_html=True)

st.markdown("### Realtime Trend")
history_df = pd.DataFrame(st.session_state.health_history)
if not history_df.empty:
    trend_df = history_df[["health_score", "trigger_ok"]].rename(
        columns={"health_score": "health", "trigger_ok": "trigger"}
    )
    st.line_chart(trend_df, height=220, use_container_width=True)
    st.caption("health=健康率(0-1), trigger=最近触发成功(1)/失败(0)")

st.markdown("### Trigger Logs")
if not st.session_state.logs:
    st.write("暂无日志")
else:
    for item in st.session_state.logs:
        text = (
            f"[{item['time']}] {item['source']} -> "
            f"{'success' if item['ok'] else 'failed'}, status={item['status_code']}"
        )
        if item["error"]:
            text = f"{text}, error={item['error']}"
        st.markdown(f"<div class='log-line'>{text}</div>", unsafe_allow_html=True)

with st.expander("展开查看完整 Debug Payload", expanded=False):
    st.json(
        {
            "health_snapshot": st.session_state.health_snapshot,
            "pipeline_snapshot": st.session_state.pipeline_snapshot,
            "last_trigger_result": st.session_state.last_result,
        }
    )
