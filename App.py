import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import create_engine
import plotly.express as px
import plotly.io as pio

pio.templates.default = "plotly_white"  # Match light “template” shell; charts stay readable on mesh bg

import os
import io
import json
from typing import Dict, List, Optional, Tuple
import textwrap
import re
import html
from datetime import date, timedelta
from urllib import request as urlrequest
from urllib.error import URLError, HTTPError
import base64
import urllib.parse

try:
    import google.generativeai as genai
except Exception:
    genai = None

try:
    from googleapiclient.discovery import build as _gdrive_build
    from googleapiclient.errors import HttpError as _GDriveHttpError
    _GDRIVE_OK = True
except Exception:
    _gdrive_build = None  # type: ignore
    _GDriveHttpError = Exception
    _GDRIVE_OK = False

# --- Configuration ---
st.set_page_config(page_title="Operation Dashboard", page_icon="📊", layout="wide")

# Main app shell: typography, mesh background, hero, cards (template-style polish)
_APP_THEME_CSS = """
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&family=Inter:wght@400;500;600;700&family=Outfit:wght@500;600;700&display=swap" rel="stylesheet">
<style>
/* Hide only the right-side action buttons and decoration.
   stToolbar and stHeader are intentionally left alone so the
   sidebar collapse/expand arrow is always visible and clickable. */
[data-testid="stToolbarActions"] { display: none !important; }
[data-testid="stDecoration"] { display: none !important; }
#MainMenu { display: none !important; }
header[data-testid="stHeader"] {
    background-color: transparent !important;
    box-shadow: none !important;
}
/* Download button hidden for non-admin — overridden to visible for admin via inline style injected below */
.no-download [data-testid="stElementToolbar"],
.no-download [data-testid="stElementToolbarButton"],
.no-download button[title="Download"],
.no-download [aria-label="Download"] { display: none !important; }
:root {
  --pnl-bg: #f1f5f9;
  --pnl-surface: #ffffff;
  --pnl-accent: #4f46e5;
  --pnl-accent2: #0ea5e9;
  --pnl-text: #0f172a;
  --pnl-muted: #64748b;
  --pnl-radius: 14px;
  --pnl-shadow: 0 1px 3px rgba(15, 23, 42, 0.06), 0 8px 24px rgba(15, 23, 42, 0.06);
  --pnl-border: #e2e8f0;
}
/* Admin-style content canvas: calm gray workspace */
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] {
  background: var(--pnl-bg) !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] .block-container {
  padding-top: 1.35rem !important;
  padding-bottom: 2.75rem !important;
  font-family: "Inter", "DM Sans", system-ui, sans-serif !important;
  color: #1e293b !important;
  max-width: 100% !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] h1,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] h2,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] h3 {
  font-family: "Outfit", "Inter", system-ui, sans-serif !important;
  letter-spacing: -0.025em;
  color: #0f172a !important;
  font-weight: 600 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] .block-container p,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] .block-container li {
  color: #475569 !important;
}
/* Page hero — admin panel header strip */
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .app-hero {
  background: var(--pnl-surface);
  border: 1px solid var(--pnl-border);
  border-radius: var(--pnl-radius);
  padding: 1.25rem 1.5rem 1.15rem;
  margin-bottom: 1.35rem;
  box-shadow: var(--pnl-shadow);
  position: relative;
  overflow: hidden;
  border-left: 4px solid var(--pnl-accent);
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .app-hero::before {
  content: "";
  position: absolute;
  top: 0; right: 0;
  width: 180px; height: 100%;
  background: linear-gradient(105deg, transparent 40%, rgba(79, 70, 229, 0.04) 100%);
  pointer-events: none;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .app-hero-badge {
  display: inline-block;
  font-size: 0.68rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: #4338ca;
  background: #eef2ff;
  padding: 0.3rem 0.6rem;
  border-radius: 6px;
  margin-bottom: 0.45rem;
  border: 1px solid #e0e7ff;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .app-hero-title {
  font-family: "Outfit", sans-serif !important;
  font-size: 1.65rem !important;
  font-weight: 700 !important;
  margin: 0 0 0.35rem 0 !important;
  color: #0f172a !important;
  -webkit-text-fill-color: #0f172a !important;
  background: none !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .app-hero-sub {
  margin: 0 !important;
  color: var(--pnl-muted) !important;
  font-size: 0.94rem !important;
  line-height: 1.55 !important;
  max-width: 52rem;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .app-hero-view {
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
  margin-top: 0.75rem;
  font-size: 0.78rem;
  font-weight: 600;
  color: #475569;
  padding: 0.35rem 0.65rem;
  background: #f8fafc;
  border-radius: 8px;
  border: 1px solid var(--pnl-border);
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .app-hero-view strong {
  color: var(--pnl-accent);
}
/* Content cards */
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .pnl-card {
  background: var(--pnl-surface);
  border: 1px solid var(--pnl-border);
  border-radius: var(--pnl-radius);
  padding: 1.1rem 1.25rem;
  margin: 0.85rem 0 1.1rem 0;
  box-shadow: var(--pnl-shadow);
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .pnl-card h3,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .pnl-card p { margin-top: 0; }
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] [data-testid="stMetricValue"] {
  font-family: "Outfit", "Inter", sans-serif !important;
  font-weight: 600 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] [data-testid="stMetricLabel"] {
  font-weight: 500 !important;
  font-size: 0.8rem !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] [data-testid="stMetricContainer"] {
  background: var(--pnl-surface) !important;
  border: 1px solid var(--pnl-border) !important;
  border-radius: var(--pnl-radius) !important;
  padding: 1rem !important;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04) !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] [data-testid="stDataFrame"],
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] .js-plotly-plot {
  border-radius: var(--pnl-radius) !important;
  overflow: hidden;
  border: 1px solid var(--pnl-border) !important;
  box-shadow: 0 1px 3px rgba(15, 23, 42, 0.05) !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] [data-testid="stChatInput"] {
  border-radius: var(--pnl-radius) !important;
  border: 1px solid var(--pnl-border) !important;
  box-shadow: var(--pnl-shadow) !important;
}
/* Main widgets — consistent with admin shell */
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] .stButton > button[kind="primary"],
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] button[data-testid="baseButton-primary"] {
  background: linear-gradient(180deg, #6366f1 0%, #4f46e5 100%) !important;
  border: none !important;
  color: #fff !important;
  border-radius: 10px !important;
  font-weight: 600 !important;
  box-shadow: 0 1px 2px rgba(79, 70, 229, 0.2) !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] .stButton > button[kind="secondary"],
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] button[data-testid="baseButton-secondary"] {
  background: #fff !important;
  border: 1px solid var(--pnl-border) !important;
  color: #334155 !important;
  border-radius: 10px !important;
  font-weight: 500 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] .stTextInput input,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] .stTextArea textarea {
  border-radius: 10px !important;
  border: 1px solid var(--pnl-border) !important;
  background: #fff !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] [data-baseweb="select"] > div {
  border-radius: 10px !important;
  border-color: var(--pnl-border) !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] [data-testid="stExpander"] details {
  border: 1px solid var(--pnl-border) !important;
  border-radius: var(--pnl-radius) !important;
  background: var(--pnl-surface) !important;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04) !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] .stTabs [data-baseweb="tab-list"] {
  background: #e2e8f0 !important;
  border-radius: 10px !important;
  padding: 4px !important;
  gap: 4px !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] .stTabs [aria-selected="true"] {
  background: #fff !important;
  border-radius: 8px !important;
  box-shadow: 0 1px 3px rgba(15, 23, 42, 0.08) !important;
  color: #0f172a !important;
  font-weight: 600 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] [data-testid="stAlert"] {
  border-radius: var(--pnl-radius) !important;
  border: 1px solid var(--pnl-border) !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stMain"] [data-testid="stVerticalBlock"] > div[data-testid="column"] {
  min-width: 0 !important;
}
</style>
"""

# When Streamlit theme is Dark (or System → OS dark): match sidebar + main + hero to one palette
_APP_THEME_DARK_CSS = """
<style>
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] {
  background:
    radial-gradient(900px 480px at 100% 0%, rgba(79, 70, 229, 0.12), transparent 52%),
    radial-gradient(700px 400px at 0% 50%, rgba(14, 165, 233, 0.05), transparent 48%),
    linear-gradient(180deg, #0b0b0f 0%, #111116 45%, #14141c 100%) !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] .block-container {
  font-family: "Inter", "DM Sans", system-ui, sans-serif !important;
  color: #e2e8f0 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] h1,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] h2,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] h3 {
  font-family: "Outfit", "DM Sans", system-ui, sans-serif !important;
  color: #f1f5f9 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] .app-hero {
  background: linear-gradient(145deg, #16161f 0%, #1c1c28 100%) !important;
  border: 1px solid rgba(129, 140, 248, 0.22) !important;
  border-left: 4px solid #6366f1 !important;
  box-shadow: 0 4px 28px rgba(0, 0, 0, 0.5) !important;
  border-radius: 14px !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] .app-hero::before {
  display: none !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] .app-hero-badge {
  color: #c7d2fe !important;
  background: rgba(99, 102, 241, 0.22) !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] .app-hero-title {
  background: none !important;
  -webkit-text-fill-color: #f8fafc !important;
  color: #f8fafc !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] .app-hero-sub {
  color: #94a3b8 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] .app-hero-view {
  color: #cbd5e1 !important;
  background: rgba(15, 23, 42, 0.5) !important;
  border: 1px solid rgba(148, 163, 184, 0.15) !important;
  border-radius: 8px !important;
  padding: 0.35rem 0.65rem !important;
  display: inline-flex !important;
  margin-top: 0.65rem !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] .app-hero-view strong {
  color: #a5b4fc !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] .pnl-card {
  background: #1a1a24 !important;
  border: 1px solid rgba(148, 163, 184, 0.14) !important;
  box-shadow: 0 4px 24px rgba(0, 0, 0, 0.4) !important;
  color: #e2e8f0 !important;
  border-radius: 14px !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stMetricContainer"] {
  background: #1a1a24 !important;
  border: 1px solid rgba(148, 163, 184, 0.14) !important;
  border-radius: 14px !important;
  padding: 1rem !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stDataFrame"],
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] .js-plotly-plot {
  border-radius: 12px !important;
  border: 1px solid rgba(148, 163, 184, 0.2) !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stChatInput"] {
  border-radius: 14px !important;
  box-shadow: 0 2px 20px rgba(0, 0, 0, 0.35) !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] .block-container p,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] .block-container li,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] .block-container td {
  color: #e2e8f0 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] .stMarkdown,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] .stMarkdown p,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] .stMarkdown li,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stMarkdownContainer"] p {
  color: #e2e8f0 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] a {
  color: #a5b4fc !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stCaption"] {
  color: #94a3b8 !important;
}
/* Metrics: light numbers + labels on dark bg */
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stMetricValue"] {
  color: #f8fafc !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stMetricLabel"] {
  color: #94a3b8 !important;
}
/* Alerts / info boxes — dark surface, light copy */
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stAlert"],
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] div[role="alert"] {
  background: #1e293b !important;
  color: #f1f5f9 !important;
  border: 1px solid #334155 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stAlert"] p,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stAlert"] div {
  color: #f1f5f9 !important;
}
/* Chat bubbles */
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stChatMessage"] {
  background: #1a1a24 !important;
  color: #e2e8f0 !important;
  border: 1px solid #2d2d3d !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stChatMessage"] p,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stChatMessage"] div {
  color: #e2e8f0 !important;
}
/* Code blocks in main */
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] pre,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] code {
  background: #0f172a !important;
  color: #e2e8f0 !important;
  border: 1px solid #334155 !important;
}
/* Expanders */
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stExpander"] summary,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stExpander"] summary p {
  color: #f1f5f9 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stExpander"] details {
  background: #1a1a24 !important;
  border: 1px solid #334155 !important;
  border-radius: 12px !important;
}
/* Widget labels in main (not sidebar) */
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] label,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-testid="stWidgetLabel"] p {
  color: #cbd5e1 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] .stTextInput input,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] .stTextArea textarea {
  background: #0f172a !important;
  color: #f1f5f9 !important;
  border-color: #475569 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-baseweb="select"] > div {
  background-color: #1e293b !important;
  color: #f1f5f9 !important;
  border-color: #475569 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stMain"] [data-baseweb="select"] span {
  color: #f1f5f9 !important;
}
/* Theme-aware HTML snippets (no inline colors — works in light + dark) */
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .pnl-card .pnl-card-title { color: #0f172a !important; }
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .pnl-card .pnl-card-desc { color: #64748b !important; }
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .pnl-card-meta { color: #64748b !important; }
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) .pnl-card-meta strong { color: #4f46e5 !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .pnl-card .pnl-card-title { color: #f1f5f9 !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .pnl-card .pnl-card-desc { color: #94a3b8 !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .pnl-card-meta { color: #94a3b8 !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .pnl-card-meta strong { color: #a5b4fc !important; }
</style>
"""


def _render_app_hero(main_view_label: str) -> None:
    """Branded header strip — title, badge, and sub-text change per page."""
    if main_view_label == "PNL":
        hero_title = "PNL Dashboard"
        hero_badge = "P&amp;L Workspace"
        hero_sub = (
            "Load data from the sidebar (same <strong>Select Database Type</strong> as everywhere else), "
            "then build your P&amp;L view here."
        )
    elif main_view_label == "AI Chat":
        hero_title = "AI Assistant"
        hero_badge = "Conversational Analyst"
        hero_sub = (
            "Ask questions about your loaded data in plain English. "
            "Use <strong>sql:</strong> prefix to run DuckDB queries directly on the table."
        )
    elif main_view_label == "Admin":
        hero_title = "Admin Panel"
        hero_badge = "User Management"
        hero_sub = (
            "Create and manage user accounts, assign roles (<strong>admin</strong> / <strong>user</strong>), "
            "and control which pages each user can access."
        )
    else:
        hero_title = "Operation Dashboard"
        hero_badge = "AI · BigQuery · Sheets"
        hero_sub = (
            "Connect a data source in the sidebar, then switch between PNL analytics, an operations view, "
            "and a conversational analyst — all on one workspace."
        )
    safe = html.escape(main_view_label)
    st.markdown(
        f"""
        <div class="app-hero">
          <span class="app-hero-badge">{hero_badge}</span>
          <h1 class="app-hero-title">{hero_title}</h1>
          <p class="app-hero-sub">{hero_sub}</p>
          <div class="app-hero-view">● Viewing: <strong>{safe}</strong></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# Sidebar nav styling (vertical nav + active state, similar to dashboard sidebar references)
_SIDEBAR_NAV_CSS = """
<style>
/* Admin shell: dark sidebar rail (Streamlit light theme) */
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f172a 0%, #1e293b 72%, #1e293b 100%) !important;
    color-scheme: dark !important;
    --st-primary-color: #818cf8 !important;
    color: #e2e8f0 !important;
    border-right: 1px solid rgba(148, 163, 184, 0.12) !important;
    box-shadow: 4px 0 24px rgba(15, 23, 42, 0.12) !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .block-container {
    padding-top: 0.85rem !important;
    padding-bottom: 1.25rem !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] label,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stWidget label,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stMarkdown p,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] p[data-testid="stCaption"] {
    color: #e2e8f0 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] [data-testid="stCaption"],
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stCaption,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] div[data-testid="stCaption"] p {
    color: #94a3b8 !important;
    opacity: 1 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] h1,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] h2,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] h3 {
    color: #f8fafc !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] hr {
    border-color: rgba(148, 163, 184, 0.2) !important;
    opacity: 1 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stTextInput input {
    border-radius: 10px !important;
    border: 1px solid rgba(148, 163, 184, 0.22) !important;
    background: rgba(15, 23, 42, 0.55) !important;
    color: #f1f5f9 !important;
    box-shadow: none !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stTextInput input::placeholder {
    color: #64748b !important;
    opacity: 1 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] [data-baseweb="select"] > div {
    background-color: rgba(15, 23, 42, 0.55) !important;
    border: 1px solid rgba(148, 163, 184, 0.22) !important;
    border-radius: 10px !important;
    color: #f1f5f9 !important;
    box-shadow: none !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] [data-baseweb="select"] span,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] [data-baseweb="popover"] span {
    color: #f1f5f9 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] [data-baseweb="select"]:focus-within > div {
    border-color: #818cf8 !important;
    box-shadow: 0 0 0 2px rgba(129, 140, 248, 0.25) !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stCheckbox label,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stCheckbox label span,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stRadio label,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stRadio label span {
    color: #e2e8f0 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] [data-testid="stExpander"] summary,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] [data-testid="stExpander"] summary p,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] [data-testid="stExpander"] summary span {
    color: #f1f5f9 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .streamlit-expanderHeader {
    color: #f1f5f9 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stToggle label p,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stToggle label span {
    color: #e2e8f0 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] [data-testid="stFileUploader"] section label,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] [data-testid="stFileUploader"] small {
    color: #94a3b8 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .sb-brand {
    display: flex;
    align-items: center;
    gap: 0.65rem;
    padding: 0.15rem 0 0.5rem 0;
    font-family: system-ui, -apple-system, Segoe UI, sans-serif;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .sb-logo {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 2.35rem;
    height: 2.35rem;
    border-radius: 10px;
    background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%);
    color: #fff !important;
    font-weight: 800;
    font-size: 0.8rem;
    letter-spacing: -0.02em;
    box-shadow: 0 2px 12px rgba(79, 70, 229, 0.45);
    border: 1px solid rgba(255, 255, 255, 0.12);
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .sb-title {
    font-weight: 700;
    font-size: 1.08rem;
    color: #f8fafc !important;
    font-family: "Outfit", "Inter", system-ui, sans-serif !important;
    letter-spacing: -0.02em;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] h2,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] h3 {
    font-family: "Outfit", "DM Sans", sans-serif !important;
    letter-spacing: -0.02em !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stButton > button {
    border-radius: 12px !important;
    justify-content: flex-start !important;
    text-align: left !important;
    padding: 0.55rem 0.85rem !important;
    font-weight: 600 !important;
    transition: background 0.15s ease, color 0.15s ease, box-shadow 0.15s ease !important;
}
/* Streamlit 1.28+ uses data-testid; theme often overrides [kind] — force both */
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stButton > button[kind="secondary"],
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] button[data-testid="baseButton-secondary"] {
    background: rgba(30, 41, 59, 0.65) !important;
    background-image: none !important;
    border: 1px solid rgba(148, 163, 184, 0.2) !important;
    color: #cbd5e1 !important;
    box-shadow: none !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stButton > button[kind="secondary"]:hover,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] button[data-testid="baseButton-secondary"]:hover {
    background: rgba(51, 65, 85, 0.85) !important;
    border-color: rgba(148, 163, 184, 0.35) !important;
    color: #f1f5f9 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stButton > button[kind="primary"],
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] button[data-testid="baseButton-primary"] {
    background: linear-gradient(180deg, #6366f1 0%, #4f46e5 100%) !important;
    border: none !important;
    border-color: transparent !important;
    color: #ffffff !important;
    box-shadow: 0 2px 12px rgba(79, 70, 229, 0.4) !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stButton > button[kind="primary"]:hover,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] button[data-testid="baseButton-primary"]:hover {
    background: linear-gradient(180deg, #818cf8 0%, #6366f1 100%) !important;
    color: #ffffff !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] button[data-testid="baseButton-primary"] p,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] button[data-testid="baseButton-primary"] span {
    color: #ffffff !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] button[data-testid="baseButton-secondary"] p,
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] button[data-testid="baseButton-secondary"] span {
    color: #cbd5e1 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stButton > button[kind="primary"],
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] button[data-testid="baseButton-primary"] {
    background-color: #4f46e5 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .sb-nav-label {
    font-size: 0.68rem !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.16em !important;
    color: #64748b !important;
    margin: 0 0 0.45rem 0 !important;
}
/* Password field trailing icon button only (not full-width nav buttons) */
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stTextInput button[data-testid="baseButton-secondary"],
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stTextInput button[kind="header"] {
    min-height: unset !important;
    background: rgba(30, 41, 59, 0.8) !important;
    border: 1px solid rgba(148, 163, 184, 0.25) !important;
    border-radius: 8px !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stTextInput button svg {
    fill: #94a3b8 !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] [data-testid="stExpander"] details {
    background: rgba(15, 23, 42, 0.35) !important;
    border: 1px solid rgba(148, 163, 184, 0.15) !important;
    border-radius: 10px !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] textarea {
    background: rgba(15, 23, 42, 0.55) !important;
    color: #f1f5f9 !important;
    border: 1px solid rgba(148, 163, 184, 0.22) !important;
    border-radius: 10px !important;
}
[data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"] .stSlider label p {
    color: #e2e8f0 !important;
}
</style>
"""
_SIDEBAR_THEME_DARK_CSS = """
<style>
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1a1b26 0%, #252536 100%) !important;
    color: #e2e8f0 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] .sb-title { color: #f8fafc !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] .sb-nav-label { color: #94a3b8 !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] label,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] h1, [data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] h2, [data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] h3 {
    color: #f1f5f9 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] [data-testid="stCaption"],
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] .stCaption {
    color: #94a3b8 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] .stCheckbox label span,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] .stToggle label p {
    color: #e2e8f0 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] [data-baseweb="select"] > div {
    background-color: #2d3348 !important;
    border-color: #4b5568 !important;
    color: #f1f5f9 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] [data-baseweb="select"] span {
    color: #f1f5f9 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] .stButton > button[kind="secondary"],
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] button[data-testid="baseButton-secondary"] {
    background: #2d3348 !important;
    border: 1px solid #3f3f55 !important;
    color: #cbd5e1 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] .stButton > button[kind="secondary"]:hover,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] button[data-testid="baseButton-secondary"]:hover {
    background: rgba(99, 102, 241, 0.2) !important;
    border-color: #5c5f77 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] .stButton > button[kind="primary"],
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] button[data-testid="baseButton-primary"] {
    background: linear-gradient(135deg, #4c6ef5 0%, #5c7cfa 100%) !important;
    background-color: #4f46e5 !important;
    color: #ffffff !important;
    border: none !important;
    box-shadow: 0 2px 10px rgba(76, 110, 245, 0.35) !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] button[data-testid="baseButton-primary"] p,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] button[data-testid="baseButton-primary"] span {
    color: #ffffff !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] button[data-testid="baseButton-secondary"] p,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] button[data-testid="baseButton-secondary"] span {
    color: #cbd5e1 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] .stTextInput input {
    background: #2d2d3d !important;
    border-color: #3f3f55 !important;
    color: #f1f5f9 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] .stTextInput input::placeholder {
    color: #94a3b8 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] [data-testid="stExpander"] summary,
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] [data-testid="stExpander"] summary p {
    color: #e2e8f0 !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] section[data-testid="stSidebar"] hr { border-color: #3f3f55 !important; }
</style>
"""

_PNL_MOBILITY_CSS = """
<style>
.pnl-mob-wrap { overflow-x: auto; margin: 0.5rem 0 1rem 0; border-radius: 14px; border: 1px solid #e2e8f0; box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06); }
.pnl-mob-table { width: 100%; border-collapse: collapse; font-size: 0.88rem; font-family: "Inter", "DM Sans", system-ui, sans-serif; }
.pnl-mob-table th {
  background: #b91c1c; color: #fff; padding: 0.55rem 0.7rem; text-align: left; font-weight: 600;
  border: 1px solid #991b1b; font-size: 0.82rem; letter-spacing: 0.02em;
}
.pnl-mob-table td { border: 1px solid #e2e8f0; padding: 0.45rem 0.65rem; color: #0f172a; }
.pnl-mob-table td.num { text-align: right; font-variant-numeric: tabular-nums; }
.pnl-mob-tr-rev td { background: #e0f2fe; }
.pnl-mob-tr-cost td { background: #ffedd5; }
.pnl-mob-tr-cm td { background: #e2e8f0; font-weight: 600; }
.pnl-mob-tr-hl td { font-weight: 700; }
.pnl-mob-grand { background: #dbeafe !important; font-weight: 600; }
[data-testid="stAppViewContainer"][data-theme="dark"] .pnl-mob-wrap { border-color: rgba(148, 163, 184, 0.2); box-shadow: 0 4px 20px rgba(0, 0, 0, 0.35); }
[data-testid="stAppViewContainer"][data-theme="dark"] .pnl-mob-table th { background: #9f1239; border-color: #7f1d1d; }
[data-testid="stAppViewContainer"][data-theme="dark"] .pnl-mob-table td { border-color: #475569; color: #f1f5f9; }
[data-testid="stAppViewContainer"][data-theme="dark"] .pnl-mob-tr-rev td { background: #1e3a5f; }
[data-testid="stAppViewContainer"][data-theme="dark"] .pnl-mob-tr-cost td { background: #5c3d1e; }
[data-testid="stAppViewContainer"][data-theme="dark"] .pnl-mob-tr-cm td { background: #334155; }
[data-testid="stAppViewContainer"][data-theme="dark"] .pnl-mob-grand { background: #1e3a8a !important; }
</style>
"""

# One bundle for the whole app (main + sidebar, light + dark branches inside CSS)
_PNL_FULL_SITE_CSS = (
    _APP_THEME_CSS
    + _APP_THEME_DARK_CSS
    + _SIDEBAR_NAV_CSS
    + _SIDEBAR_THEME_DARK_CSS
    + _PNL_MOBILITY_CSS
)


def _build_theme_override_css(dark: bool) -> str:
    """
    Unconditional CSS override — forces sidebar + main into the chosen palette.

    Specificity note: the bundled CSS uses
      [data-testid="stAppViewContainer"]:not([data-theme="dark"]) section[data-testid="stSidebar"]
    which has specificity (0,3,1).  We must match or exceed that to win, even though
    our <style> block is injected later (ordering only wins if specificity is >=).
    Fix: double the app-container attribute selector → (0,2,0) prefix, giving us
      [data-testid="stAppViewContainer"][data-testid="stAppViewContainer"] section[...]
    = (0,2,0)+(0,0,1)+(0,1,0) = (0,3,1) — same specificity, later rule wins. ✓
    For nested / class selectors we similarly double to reach (0,3,0) or (0,4,1).
    """
    # Shorthand prefixes used in template below
    APP  = '[data-testid="stAppViewContainer"][data-testid="stAppViewContainer"]'
    MAIN = f'{APP} section[data-testid="stMain"]'
    SB   = f'{APP} section[data-testid="stSidebar"]'

    if dark:
        return f"""<style>
/* ── DARK THEME OVERRIDE (specificity-hardened) ────────────────── */
{MAIN}{{background:linear-gradient(180deg,#0b0b0f 0%,#111116 45%,#14141c 100%) !important;color:#e2e8f0 !important;}}
{MAIN} .block-container{{color:#e2e8f0 !important;font-family:"Inter","DM Sans",system-ui,sans-serif !important;}}
{MAIN} h1,{MAIN} h2,{MAIN} h3{{color:#f1f5f9 !important;font-family:"Outfit","DM Sans",system-ui,sans-serif !important;}}
{MAIN} .block-container p,{MAIN} .block-container li,{MAIN} .block-container td{{color:#e2e8f0 !important;}}
{MAIN} .stMarkdown p,{MAIN} .stMarkdown li,{MAIN} [data-testid="stMarkdownContainer"] p{{color:#e2e8f0 !important;}}
{MAIN} a{{color:#a5b4fc !important;}}
{MAIN} [data-testid="stCaption"]{{color:#94a3b8 !important;}}
{MAIN} [data-testid="stMetricContainer"]{{background:#1a1a24 !important;border:1px solid rgba(148,163,184,.14) !important;border-radius:14px !important;}}
{MAIN} [data-testid="stMetricValue"]{{color:#f8fafc !important;}}
{MAIN} [data-testid="stMetricLabel"]{{color:#94a3b8 !important;}}
{MAIN} [data-testid="stAlert"],{MAIN} div[role="alert"]{{background:#1e293b !important;color:#f1f5f9 !important;border:1px solid #334155 !important;}}
{MAIN} [data-testid="stAlert"] p,{MAIN} [data-testid="stAlert"] div{{color:#f1f5f9 !important;}}
{MAIN} [data-testid="stExpander"] details{{background:#1a1a24 !important;border:1px solid #334155 !important;border-radius:12px !important;}}
{MAIN} [data-testid="stExpander"] summary p,{MAIN} [data-testid="stExpander"] summary span{{color:#f1f5f9 !important;}}
{MAIN} label,{MAIN} [data-testid="stWidgetLabel"] p{{color:#cbd5e1 !important;}}
{MAIN} .stTextInput input,{MAIN} .stTextArea textarea{{background:#0f172a !important;color:#f1f5f9 !important;border-color:#475569 !important;}}
{MAIN} [data-baseweb="select"]>div{{background-color:#1e293b !important;color:#f1f5f9 !important;border-color:#475569 !important;}}
{MAIN} [data-baseweb="select"] span{{color:#f1f5f9 !important;}}
{MAIN} pre,{MAIN} code{{background:#0f172a !important;color:#e2e8f0 !important;border:1px solid #334155 !important;}}
{MAIN} [data-testid="stChatMessage"]{{background:#1a1a24 !important;border:1px solid #2d2d3d !important;}}
{MAIN} [data-testid="stChatMessage"] p,{MAIN} [data-testid="stChatMessage"] div{{color:#e2e8f0 !important;}}
{APP} .app-hero{{background:linear-gradient(145deg,#16161f 0%,#1c1c28 100%) !important;border:1px solid rgba(129,140,248,.22) !important;border-left:4px solid #6366f1 !important;box-shadow:0 4px 28px rgba(0,0,0,.5) !important;}}
{APP} .app-hero-title{{color:#f8fafc !important;-webkit-text-fill-color:#f8fafc !important;background:none !important;font-family:"Outfit",sans-serif !important;}}
{APP} .app-hero-sub{{color:#94a3b8 !important;}}
{APP} .app-hero-badge{{color:#c7d2fe !important;background:rgba(99,102,241,.22) !important;border-color:transparent !important;}}
{APP} .app-hero-view{{color:#cbd5e1 !important;background:rgba(15,23,42,.5) !important;border:1px solid rgba(148,163,184,.15) !important;}}
{APP} .app-hero-view strong{{color:#a5b4fc !important;}}
{APP} .pnl-card{{background:#1a1a24 !important;border:1px solid rgba(148,163,184,.14) !important;color:#e2e8f0 !important;}}
{APP} .pnl-mob-wrap{{border-color:rgba(148,163,184,.2) !important;}}
{APP} .pnl-mob-table th{{background:#9f1239 !important;border-color:#7f1d1d !important;}}
{APP} .pnl-mob-table td{{border-color:#475569 !important;color:#f1f5f9 !important;}}
{APP} .pnl-mob-tr-rev td{{background:#1e3a5f !important;}}
{APP} .pnl-mob-tr-cost td{{background:#5c3d1e !important;}}
{APP} .pnl-mob-tr-cm td{{background:#334155 !important;}}
{APP} .pnl-mob-grand{{background:#1e3a8a !important;}}
{SB}{{background:linear-gradient(180deg,#1a1b26 0%,#252536 100%) !important;color:#e2e8f0 !important;color-scheme:dark !important;border-right:1px solid rgba(148,163,184,.1) !important;}}
{SB} .sb-title{{color:#f8fafc !important;}}
{SB} .sb-nav-label{{color:#64748b !important;}}
{SB} label,{SB} [data-testid="stWidgetLabel"] p,{SB} .stMarkdown p{{color:#e2e8f0 !important;}}
{SB} h1,{SB} h2,{SB} h3{{color:#f1f5f9 !important;}}
{SB} [data-testid="stCaption"],{SB} .stCaption,{SB} div[data-testid="stCaption"] p{{color:#94a3b8 !important;opacity:1 !important;}}
{SB} .stTextInput input{{background:#2d2d3d !important;color:#f1f5f9 !important;border-color:#3f3f55 !important;}}
{SB} .stTextInput input::placeholder{{color:#64748b !important;}}
{SB} textarea{{background:#2d2d3d !important;color:#f1f5f9 !important;border-color:#3f3f55 !important;}}
{SB} [data-baseweb="select"]>div{{background-color:#2d3348 !important;border-color:#4b5568 !important;color:#f1f5f9 !important;}}
{SB} [data-baseweb="select"] span,{SB} [data-baseweb="popover"] span{{color:#f1f5f9 !important;}}
{SB} .stCheckbox label span,{SB} .stToggle label p,{SB} .stToggle label span,{SB} .stRadio label span{{color:#e2e8f0 !important;}}
{SB} [data-testid="stExpander"] details{{background:rgba(15,23,42,.35) !important;border:1px solid rgba(148,163,184,.15) !important;border-radius:10px !important;}}
{SB} [data-testid="stExpander"] summary p,{SB} [data-testid="stExpander"] summary span{{color:#f1f5f9 !important;}}
{SB} .stButton>button[kind="secondary"],{SB} button[data-testid="baseButton-secondary"]{{background:#2d3348 !important;border:1px solid #3f3f55 !important;color:#cbd5e1 !important;}}
{SB} .stButton>button[kind="secondary"]:hover,{SB} button[data-testid="baseButton-secondary"]:hover{{background:rgba(99,102,241,.2) !important;border-color:#5c5f77 !important;color:#f1f5f9 !important;}}
{SB} .stButton>button[kind="primary"],{SB} button[data-testid="baseButton-primary"]{{background:linear-gradient(135deg,#4c6ef5 0%,#5c7cfa 100%) !important;color:#fff !important;border:none !important;box-shadow:0 2px 12px rgba(76,110,245,.35) !important;}}
{SB} button[data-testid="baseButton-primary"] p,{SB} button[data-testid="baseButton-primary"] span{{color:#fff !important;}}
{SB} button[data-testid="baseButton-secondary"] p,{SB} button[data-testid="baseButton-secondary"] span{{color:#cbd5e1 !important;}}
{SB} hr{{border-color:#3f3f55 !important;opacity:1 !important;}}
{SB} .stSlider label p{{color:#e2e8f0 !important;}}
{SB} .stFileUploader section label,{SB} .stFileUploader small{{color:#94a3b8 !important;}}
</style>"""
    else:
        return f"""<style>
/* ── LIGHT THEME OVERRIDE (specificity-hardened) ───────────────── */
{MAIN}{{background:#f1f5f9 !important;color:#1e293b !important;}}
{MAIN} .block-container{{color:#1e293b !important;font-family:"Inter","DM Sans",system-ui,sans-serif !important;}}
{MAIN} h1,{MAIN} h2,{MAIN} h3{{color:#0f172a !important;font-family:"Outfit","Inter",system-ui,sans-serif !important;}}
{MAIN} .block-container p,{MAIN} .block-container li,{MAIN} .block-container td{{color:#475569 !important;}}
{MAIN} .stMarkdown p,{MAIN} .stMarkdown li,{MAIN} [data-testid="stMarkdownContainer"] p{{color:#475569 !important;}}
{MAIN} a{{color:#4f46e5 !important;}}
{MAIN} [data-testid="stCaption"]{{color:#64748b !important;}}
{MAIN} [data-testid="stMetricContainer"]{{background:#fff !important;border:1px solid #e2e8f0 !important;border-radius:14px !important;box-shadow:0 1px 2px rgba(15,23,42,.04) !important;}}
{MAIN} [data-testid="stMetricValue"]{{color:#0f172a !important;}}
{MAIN} [data-testid="stMetricLabel"]{{color:#64748b !important;}}
{MAIN} [data-testid="stAlert"],{MAIN} div[role="alert"]{{background:#fff !important;color:#0f172a !important;border:1px solid #e2e8f0 !important;}}
{MAIN} [data-testid="stExpander"] details{{background:#fff !important;border:1px solid #e2e8f0 !important;border-radius:12px !important;}}
{MAIN} [data-testid="stExpander"] summary p,{MAIN} [data-testid="stExpander"] summary span{{color:#0f172a !important;}}
{MAIN} label,{MAIN} [data-testid="stWidgetLabel"] p{{color:#334155 !important;}}
{MAIN} .stTextInput input,{MAIN} .stTextArea textarea{{background:#fff !important;color:#0f172a !important;border-color:#e2e8f0 !important;}}
{MAIN} [data-baseweb="select"]>div{{background-color:#fff !important;color:#0f172a !important;border-color:#e2e8f0 !important;}}
{MAIN} [data-baseweb="select"] span{{color:#0f172a !important;}}
{MAIN} pre,{MAIN} code{{background:#f8fafc !important;color:#1e293b !important;border:1px solid #e2e8f0 !important;}}
{MAIN} [data-testid="stChatMessage"]{{background:#fff !important;border:1px solid #e2e8f0 !important;}}
{MAIN} [data-testid="stChatMessage"] p,{MAIN} [data-testid="stChatMessage"] div{{color:#1e293b !important;}}
{APP} .app-hero{{background:#ffffff !important;border:1px solid #e2e8f0 !important;border-left:4px solid #4f46e5 !important;box-shadow:0 1px 3px rgba(15,23,42,.06) !important;}}
{APP} .app-hero-title{{color:#0f172a !important;-webkit-text-fill-color:#0f172a !important;background:none !important;font-family:"Outfit",sans-serif !important;}}
{APP} .app-hero-sub{{color:#64748b !important;}}
{APP} .app-hero-badge{{color:#4338ca !important;background:#eef2ff !important;border:1px solid #e0e7ff !important;}}
{APP} .app-hero-view{{color:#475569 !important;background:#f8fafc !important;border:1px solid #e2e8f0 !important;}}
{APP} .app-hero-view strong{{color:#4f46e5 !important;}}
{APP} .pnl-card{{background:#fff !important;border:1px solid #e2e8f0 !important;color:#1e293b !important;}}
{APP} .pnl-mob-wrap{{border-color:#e2e8f0 !important;}}
{APP} .pnl-mob-table th{{background:#b91c1c !important;border-color:#991b1b !important;}}
{APP} .pnl-mob-table td{{border-color:#e2e8f0 !important;color:#0f172a !important;}}
{APP} .pnl-mob-tr-rev td{{background:#e0f2fe !important;}}
{APP} .pnl-mob-tr-cost td{{background:#ffedd5 !important;}}
{APP} .pnl-mob-tr-cm td{{background:#e2e8f0 !important;}}
{APP} .pnl-mob-grand{{background:#dbeafe !important;}}
{SB}{{background:#f8fafc !important;color:#1e293b !important;color-scheme:light !important;border-right:1px solid #e2e8f0 !important;box-shadow:none !important;}}
{SB} .sb-logo{{background:linear-gradient(135deg,#6366f1 0%,#4f46e5 100%) !important;color:#fff !important;box-shadow:0 2px 12px rgba(79,70,229,.3) !important;}}
{SB} .sb-title{{color:#0f172a !important;}}
{SB} .sb-nav-label{{color:#64748b !important;}}
{SB} label,{SB} [data-testid="stWidgetLabel"] p,{SB} .stMarkdown p{{color:#334155 !important;}}
{SB} h1,{SB} h2,{SB} h3{{color:#0f172a !important;}}
{SB} [data-testid="stCaption"],{SB} .stCaption,{SB} div[data-testid="stCaption"] p{{color:#64748b !important;opacity:1 !important;}}
{SB} .stTextInput input{{background:#fff !important;color:#0f172a !important;border:1px solid #e2e8f0 !important;box-shadow:none !important;}}
{SB} .stTextInput input::placeholder{{color:#94a3b8 !important;}}
{SB} textarea{{background:#fff !important;color:#0f172a !important;border:1px solid #e2e8f0 !important;}}
{SB} [data-baseweb="select"]>div{{background-color:#fff !important;border:1px solid #e2e8f0 !important;color:#0f172a !important;box-shadow:none !important;}}
{SB} [data-baseweb="select"] span,{SB} [data-baseweb="popover"] span{{color:#0f172a !important;}}
{SB} .stCheckbox label span,{SB} .stRadio label span,{SB} .stToggle label p,{SB} .stToggle label span{{color:#334155 !important;}}
{SB} [data-testid="stExpander"] details{{background:#fff !important;border:1px solid #e2e8f0 !important;border-radius:10px !important;}}
{SB} [data-testid="stExpander"] summary p,{SB} [data-testid="stExpander"] summary span{{color:#334155 !important;}}
{SB} .stButton>button[kind="secondary"],{SB} button[data-testid="baseButton-secondary"]{{background:#fff !important;border:1px solid #e2e8f0 !important;color:#334155 !important;box-shadow:none !important;}}
{SB} .stButton>button[kind="secondary"]:hover,{SB} button[data-testid="baseButton-secondary"]:hover{{background:#f1f5f9 !important;border-color:#cbd5e1 !important;color:#0f172a !important;}}
{SB} .stButton>button[kind="primary"],{SB} button[data-testid="baseButton-primary"]{{background:linear-gradient(180deg,#6366f1 0%,#4f46e5 100%) !important;color:#fff !important;border:none !important;box-shadow:0 2px 12px rgba(79,70,229,.25) !important;}}
{SB} button[data-testid="baseButton-primary"] p,{SB} button[data-testid="baseButton-primary"] span{{color:#fff !important;}}
{SB} button[data-testid="baseButton-secondary"] p,{SB} button[data-testid="baseButton-secondary"] span{{color:#334155 !important;}}
{SB} hr{{border-color:#e2e8f0 !important;opacity:1 !important;}}
{SB} .stSlider label p{{color:#334155 !important;}}
{SB} .stFileUploader section label,{SB} .stFileUploader small{{color:#64748b !important;}}
{SB} .stTextInput button[data-testid="baseButton-secondary"]{{background:#f1f5f9 !important;border:1px solid #e2e8f0 !important;}}
</style>"""

# --- Local auth (plain JSON next to this file; first run seeds default admin) ---
_AUTH_CREDS_FILE = "app_auth_credentials.json"
_AUTH_DEFAULT_ID = "Admin@123"
_AUTH_DEFAULT_PASSWORD = "Admin@1234"
_AUTH_ALL_PAGES = ["Dashboard", "PNL", "AI Chat"]


def _auth_credentials_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), _AUTH_CREDS_FILE)


def _auth_default_user() -> dict:
    return {
        "id": _AUTH_DEFAULT_ID,
        "password": _AUTH_DEFAULT_PASSWORD,
        "role": "admin",
        "pages": _AUTH_ALL_PAGES,
    }


def _auth_normalise_user(u: dict) -> dict:
    """Back-fill role/pages for users created before this schema existed."""
    uid = str(u.get("id", ""))
    role = str(u.get("role", "admin" if uid == _AUTH_DEFAULT_ID else "user"))
    pages = u.get("pages")
    if not isinstance(pages, list) or not pages:
        pages = _AUTH_ALL_PAGES if role == "admin" else []
    return {
        "id": uid,
        "password": str(u.get("password", "")),
        "role": role,
        "pages": [p for p in pages if p in _AUTH_ALL_PAGES],
    }


def _load_auth_users() -> List[dict]:
    path = _auth_credentials_path()
    default_payload: dict = {"users": [_auth_default_user()]}
    if not os.path.isfile(path):
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(default_payload, f, indent=2)
        except OSError:
            pass
        return [_auth_default_user()]
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        users = data.get("users") if isinstance(data, dict) else None
        if not isinstance(users, list) or not users:
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(default_payload, f, indent=2)
            except OSError:
                pass
            return [_auth_default_user()]
        out: List[dict] = []
        for u in users:
            if isinstance(u, dict) and u.get("id") and u.get("password") is not None:
                out.append(_auth_normalise_user(u))
        if not out:
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(default_payload, f, indent=2)
            except OSError:
                pass
            return [_auth_default_user()]
        return out
    except (OSError, json.JSONDecodeError, TypeError):
        return [_auth_default_user()]


def _auth_save_users(users: List[dict]) -> Tuple[bool, str]:
    path = _auth_credentials_path()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"users": users}, f, indent=2)
        return True, ""
    except OSError as e:
        return False, f"Could not save: {e}"


def _auth_try_login(user_id: str, password: str) -> Tuple[bool, str]:
    uid = (user_id or "").strip()
    if not uid or not password:
        return False, "Enter user ID and password."
    for u in _load_auth_users():
        if u.get("id") == uid and u.get("password") == password:
            return True, ""
    return False, "Invalid user ID or password."


def _auth_get_user(user_id: str) -> Optional[dict]:
    for u in _load_auth_users():
        if u.get("id") == user_id:
            return u
    return None


def _auth_register(
    user_id: str,
    password: str,
    password_confirm: str,
    role: str = "user",
    pages: Optional[List[str]] = None,
) -> Tuple[bool, str]:
    uid = (user_id or "").strip()
    if not uid:
        return False, "Enter a user ID."
    if not password:
        return False, "Enter a password."
    if password != password_confirm:
        return False, "Passwords do not match."
    users = _load_auth_users()
    if any(u.get("id") == uid for u in users):
        return False, "This user ID is already registered. Use **Sign in**."
    allowed_pages = pages if isinstance(pages, list) else (_AUTH_ALL_PAGES if role == "admin" else [])
    new_entry = {"id": uid, "password": password, "role": role, "pages": allowed_pages}
    ok, err = _auth_save_users(users + [new_entry])
    if not ok:
        return False, f"Could not save account: {err}"
    return True, ""


def _auth_admin_update_user(
    target_id: str,
    new_password: Optional[str],
    new_role: str,
    new_pages: List[str],
) -> Tuple[bool, str]:
    users = _load_auth_users()
    updated = False
    for u in users:
        if u.get("id") == target_id:
            if new_password:
                u["password"] = new_password
            u["role"] = new_role
            u["pages"] = [p for p in new_pages if p in _AUTH_ALL_PAGES]
            updated = True
            break
    if not updated:
        return False, "User not found."
    return _auth_save_users(users)


def _auth_admin_delete_user(target_id: str) -> Tuple[bool, str]:
    if target_id == _AUTH_DEFAULT_ID:
        return False, "Cannot delete the default admin account."
    users = _load_auth_users()
    new_users = [u for u in users if u.get("id") != target_id]
    if len(new_users) == len(users):
        return False, "User not found."
    return _auth_save_users(new_users)


# ── API key vault (stored in app_auth_credentials.json under "api_keys") ──────

def _apikeys_load() -> List[dict]:
    """Return list of {name, key} dicts from credential store."""
    path = _auth_credentials_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        keys = data.get("api_keys") if isinstance(data, dict) else None
        if isinstance(keys, list):
            return [k for k in keys if isinstance(k, dict) and k.get("name") and k.get("key")]
    except (OSError, json.JSONDecodeError):
        pass
    return []


def _apikeys_save(keys: List[dict]) -> Tuple[bool, str]:
    path = _auth_credentials_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        data = {}
    data["api_keys"] = keys
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return True, ""
    except OSError as e:
        return False, str(e)


def _apikeys_add(name: str, key: str) -> Tuple[bool, str]:
    name = name.strip()
    key = key.strip()
    if not name:
        return False, "Give the key a name."
    if not key:
        return False, "Paste the actual API key."
    keys = _apikeys_load()
    if any(k["name"].lower() == name.lower() for k in keys):
        return False, f'A key named "{name}" already exists.'
    keys.append({"name": name, "key": key})
    return _apikeys_save(keys)


def _apikeys_delete(name: str) -> Tuple[bool, str]:
    keys = _apikeys_load()
    new_keys = [k for k in keys if k["name"] != name]
    if len(new_keys) == len(keys):
        return False, "Key not found."
    return _apikeys_save(new_keys)


# ── Apps Script URL vault ──────────────────────────────────────────────────────

def _ascript_sources_load() -> List[dict]:
    """Return list of {name, url, default_tab} dicts."""
    path = _auth_credentials_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        sources = data.get("appscript_sources", [])
        if isinstance(sources, list):
            return [s for s in sources if isinstance(s, dict) and s.get("name") and s.get("url")]
    except (OSError, json.JSONDecodeError):
        pass
    return []


def _ascript_sources_save(sources: List[dict]) -> Tuple[bool, str]:
    path = _auth_credentials_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        data = {}
    data["appscript_sources"] = sources
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return True, ""
    except OSError as e:
        return False, str(e)


def _ascript_sources_add(name: str, url: str, default_tab: str = "") -> Tuple[bool, str]:
    name, url = name.strip(), url.strip()
    if not name:
        return False, "Give the source a name."
    if not url:
        return False, "Paste the Apps Script Web App URL."
    sources = _ascript_sources_load()
    if any(s["name"].lower() == name.lower() for s in sources):
        return False, f'A source named "{name}" already exists.'
    sources.append({"name": name, "url": url, "default_tab": default_tab.strip()})
    return _ascript_sources_save(sources)


def _ascript_sources_delete(name: str) -> Tuple[bool, str]:
    sources = _ascript_sources_load()
    new = [s for s in sources if s["name"] != name]
    if len(new) == len(sources):
        return False, "Source not found."
    return _ascript_sources_save(new)


def _ascript_set_tab_permissions(source_name: str, user_id: str, allowed_tabs: List[str]) -> Tuple[bool, str]:
    """Set which tabs a specific user can see in a given source. Empty list = no access."""
    sources = _ascript_sources_load()
    for s in sources:
        if s["name"] == source_name:
            if "permissions" not in s or not isinstance(s["permissions"], dict):
                s["permissions"] = {}
            s["permissions"][user_id] = allowed_tabs
            return _ascript_sources_save(sources)
    return False, "Source not found."


def _ascript_get_allowed_tabs(source: dict, user_id: str, is_admin: bool, all_tabs: List[str]) -> List[str]:
    """Return the tabs a user is allowed to see. Admins always see all."""
    if is_admin:
        return all_tabs
    perms = source.get("permissions", {})
    # If no permissions set for this user, default = no access
    if user_id not in perms:
        return []
    allowed = perms[user_id]
    # Filter to only tabs that actually exist
    return [t for t in allowed if t in all_tabs]


def render_admin_panel() -> None:
    """Admin-only page: manage users, roles, and page access."""
    st.header("Admin Panel — User Management")
    st.caption(
        "Only users with the **admin** role can see this page. "
        "Changes are saved instantly to `app_auth_credentials.json`."
    )

    users = _load_auth_users()

    # ── User table ──────────────────────────────────────────────────────────
    st.subheader("All users")
    if users:
        tbl_rows = ""
        for u in users:
            pages_str = ", ".join(u.get("pages", [])) or "— none —"
            role_badge = (
                '<span style="background:#7c3aed;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.75rem;">admin</span>'
                if u.get("role") == "admin"
                else '<span style="background:#0369a1;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.75rem;">user</span>'
            )
            tbl_rows += (
                f"<tr><td>{html.escape(u['id'])}</td>"
                f"<td>{role_badge}</td>"
                f"<td>{html.escape(pages_str)}</td></tr>"
            )
        st.markdown(
            f'<table style="width:100%;border-collapse:collapse;">'
            f"<thead><tr>"
            f'<th style="text-align:left;padding:6px 10px;border-bottom:1px solid #334155;">User ID</th>'
            f'<th style="text-align:left;padding:6px 10px;border-bottom:1px solid #334155;">Role</th>'
            f'<th style="text-align:left;padding:6px 10px;border-bottom:1px solid #334155;">Allowed Pages</th>'
            f"</tr></thead><tbody>"
            f"{tbl_rows}"
            f"</tbody></table>",
            unsafe_allow_html=True,
        )
    else:
        st.info("No users found.")

    st.divider()

    # ── Add new user ─────────────────────────────────────────────────────────
    with st.expander("➕  Add new user", expanded=False):
        with st.form("admin_add_user_form", clear_on_submit=True):
            new_uid = st.text_input("User ID", placeholder="e.g., ops_team_member")
            new_pw = st.text_input("Password", type="password", placeholder="••••••")
            new_pw2 = st.text_input("Confirm password", type="password", placeholder="••••••")
            new_role = st.selectbox("Role", ["user", "admin"])
            new_pages = st.multiselect(
                "Allowed pages",
                _AUTH_ALL_PAGES,
                default=_AUTH_ALL_PAGES if new_role == "admin" else [],
                help="Pick which pages this user can access.",
            )
            if st.form_submit_button("Create user", type="primary"):
                ok, msg = _auth_register(new_uid, new_pw, new_pw2, role=new_role, pages=new_pages)
                if ok:
                    st.success(f"User **{new_uid}** created.")
                    st.rerun()
                else:
                    st.error(msg)

    # ── Edit / delete existing user ───────────────────────────────────────────
    with st.expander("✏️  Edit or delete a user", expanded=False):
        user_ids = [u["id"] for u in users]
        edit_uid = st.selectbox("Select user to edit", user_ids, key="admin_edit_user_sel")
        edit_user = next((u for u in users if u["id"] == edit_uid), None)
        if edit_user:
            with st.form("admin_edit_user_form"):
                e_pw = st.text_input(
                    "New password (leave blank to keep)",
                    type="password",
                    placeholder="••••••",
                )
                e_role = st.selectbox(
                    "Role",
                    ["user", "admin"],
                    index=0 if edit_user.get("role") != "admin" else 1,
                )
                e_pages = st.multiselect(
                    "Allowed pages",
                    _AUTH_ALL_PAGES,
                    default=[p for p in edit_user.get("pages", []) if p in _AUTH_ALL_PAGES],
                )
                col_save, col_del = st.columns(2)
                with col_save:
                    if st.form_submit_button("Save changes", type="primary"):
                        ok, msg = _auth_admin_update_user(
                            edit_uid,
                            new_password=e_pw or None,
                            new_role=e_role,
                            new_pages=e_pages,
                        )
                        if ok:
                            st.success(f"User **{edit_uid}** updated.")
                            st.rerun()
                        else:
                            st.error(msg)
                with col_del:
                    if st.form_submit_button("🗑  Delete user", type="secondary"):
                        ok, msg = _auth_admin_delete_user(edit_uid)
                        if ok:
                            st.success(f"User **{edit_uid}** deleted.")
                            st.rerun()
                        else:
                            st.error(msg)

    st.divider()

    # ── API Key Vault ─────────────────────────────────────────────────────────
    st.subheader("🔑  API Key Vault")
    st.caption(
        "Save named API keys here. All users see the key **names** in the sidebar and can pick one — "
        "the raw key is never shown to non-admins."
    )
    saved_keys = _apikeys_load()
    if saved_keys:
        key_tbl = ""
        for k in saved_keys:
            masked = k["key"][:6] + "••••••••" + k["key"][-4:] if len(k["key"]) > 12 else "••••••••"
            key_tbl += (
                f"<tr><td style='padding:5px 10px;'>{html.escape(k['name'])}</td>"
                f"<td style='padding:5px 10px;font-family:monospace;color:#64748b;'>{masked}</td></tr>"
            )
        st.markdown(
            f'<table style="width:100%;border-collapse:collapse;">'
            f'<thead><tr>'
            f'<th style="text-align:left;padding:6px 10px;border-bottom:1px solid #334155;">Key name</th>'
            f'<th style="text-align:left;padding:6px 10px;border-bottom:1px solid #334155;">Key (masked)</th>'
            f'</tr></thead><tbody>{key_tbl}</tbody></table>',
            unsafe_allow_html=True,
        )
    else:
        st.info("No API keys saved yet.")

    with st.expander("➕  Add API key", expanded=False):
        with st.form("admin_add_apikey_form", clear_on_submit=True):
            ak_name = st.text_input("Key name", placeholder="e.g. Work Gemini Key")
            ak_val = st.text_input("API key value", type="password", placeholder="AIza…")
            if st.form_submit_button("Save key", type="primary"):
                ok, msg = _apikeys_add(ak_name, ak_val)
                if ok:
                    st.success(f"Key **{ak_name}** saved.")
                    st.rerun()
                else:
                    st.error(msg)

    if saved_keys:
        with st.expander("🗑  Delete an API key", expanded=False):
            del_key_name = st.selectbox(
                "Select key to delete",
                [k["name"] for k in saved_keys],
                key="admin_del_apikey_sel",
            )
            if st.button("Delete key", key="admin_del_apikey_btn", type="secondary"):
                ok, msg = _apikeys_delete(del_key_name)
                if ok:
                    st.success(f"Key **{del_key_name}** deleted.")
                    st.rerun()
                else:
                    st.error(msg)

    st.divider()

    # ── Apps Script Data Sources ──────────────────────────────────────────────
    st.subheader("📄  Apps Script Data Sources")
    st.caption(
        "Save Apps Script Web App URLs here. Assign per-user tab access. "
        "Users see only their permitted tabs — URL is never shown to non-admins."
    )
    _as_sources = _ascript_sources_load()

    # ── Sources table ─────────────────────────────────────────────────────
    if _as_sources:
        as_tbl = ""
        for s in _as_sources:
            masked_url = s["url"][:45] + "…" if len(s["url"]) > 45 else s["url"]
            perms = s.get("permissions", {})
            perm_summary = ", ".join(
                f"{html.escape(uid)}: [{', '.join(html.escape(t) for t in tabs)}]"
                for uid, tabs in perms.items()
            ) if perms else "<em>all users — no restrictions yet</em>"
            as_tbl += (
                f"<tr>"
                f"<td style='padding:6px 10px;font-weight:600;'>{html.escape(s['name'])}</td>"
                f"<td style='padding:6px 10px;font-family:monospace;color:#64748b;font-size:0.78rem;'>{html.escape(masked_url)}</td>"
                f"<td style='padding:6px 10px;font-size:0.8rem;color:#475569;'>{perm_summary}</td>"
                f"</tr>"
            )
        st.markdown(
            f'<table style="width:100%;border-collapse:collapse;">'
            f'<thead><tr>'
            f'<th style="text-align:left;padding:6px 10px;border-bottom:1px solid #334155;">Source</th>'
            f'<th style="text-align:left;padding:6px 10px;border-bottom:1px solid #334155;">URL</th>'
            f'<th style="text-align:left;padding:6px 10px;border-bottom:1px solid #334155;">Tab permissions</th>'
            f'</tr></thead><tbody>{as_tbl}</tbody></table>',
            unsafe_allow_html=True,
        )
    else:
        st.info("No Apps Script sources saved yet.")

    # ── Add source ────────────────────────────────────────────────────────
    with st.expander("➕  Add Apps Script source", expanded=False):
        with st.form("admin_add_ascript_form", clear_on_submit=True):
            as_name = st.text_input("Source name", placeholder="e.g. Sales Sheet")
            as_url  = st.text_input("Web App URL", placeholder="https://script.google.com/macros/s/…/exec")
            as_tab  = st.text_input("Default tab (optional)", placeholder="Sheet1")
            if st.form_submit_button("Save source", type="primary"):
                ok, msg = _ascript_sources_add(as_name, as_url, as_tab)
                if ok:
                    st.success(f"Source **{as_name}** saved.")
                    st.rerun()
                else:
                    st.error(msg)

    # ── Tab permissions per user ──────────────────────────────────────────
    if _as_sources:
        with st.expander("🔐  Assign tab permissions to users", expanded=False):
            st.caption(
                "Select a source, fetch its tabs, then pick a user and choose which tabs they can see. "
                "Admin users always see all tabs regardless."
            )
            _perm_src_name = st.selectbox(
                "Source", [s["name"] for s in _as_sources], key="admin_perm_src_sel"
            )
            _perm_src = next((s for s in _as_sources if s["name"] == _perm_src_name), None)

            _perm_tabs_key = f"admin_perm_tabs_{_perm_src_name}"
            _perm_cached_tabs: List[str] = st.session_state.get(_perm_tabs_key, [])

            if st.button("🔄 Fetch tabs from this source", key="admin_perm_fetch_tabs"):
                try:
                    with st.spinner("Fetching tabs…"):
                        _fetched = _appscript_list_sheets(_perm_src["url"])
                    st.session_state[_perm_tabs_key] = _fetched
                    _perm_cached_tabs = _fetched
                    st.success(f"Found {len(_fetched)} tab(s): {', '.join(_fetched)}")
                except Exception as _pe:
                    st.error(f"Could not fetch tabs: {_pe}")

            if _perm_cached_tabs:
                _all_users = [u["id"] for u in _load_auth_users() if u.get("role") != "admin"]
                if not _all_users:
                    st.info("No non-admin users found.")
                else:
                    _perm_user = st.selectbox("User to configure", _all_users, key="admin_perm_user_sel")
                    _existing_tabs = (_perm_src or {}).get("permissions", {}).get(_perm_user, _perm_cached_tabs)
                    _sel_tabs = st.multiselect(
                        f"Tabs visible to **{_perm_user}**",
                        options=_perm_cached_tabs,
                        default=[t for t in _existing_tabs if t in _perm_cached_tabs],
                        key="admin_perm_tabs_sel",
                        help="Leave empty to block access entirely. Admin always sees all tabs.",
                    )
                    if st.button("💾 Save permissions", key="admin_perm_save_btn", type="primary"):
                        ok, msg = _ascript_set_tab_permissions(_perm_src_name, _perm_user, _sel_tabs)
                        if ok:
                            st.success(f"Saved: **{_perm_user}** → {_sel_tabs or 'no access'}")
                            st.rerun()
                        else:
                            st.error(msg)
            else:
                st.info("Click **Fetch tabs** above to load available tabs from the source.")

        with st.expander("🗑  Delete an Apps Script source", expanded=False):
            del_as_name = st.selectbox("Select source to delete", [s["name"] for s in _as_sources], key="admin_del_ascript_sel")
            if st.button("Delete source", key="admin_del_ascript_btn", type="secondary"):
                ok, msg = _ascript_sources_delete(del_as_name)
                if ok:
                    st.success(f"Source **{del_as_name}** deleted.")
                    st.rerun()
                else:
                    st.error(msg)

    st.divider()

    # ── Credentials Backup / Restore ──────────────────────────────────────────
    st.subheader("💾  Backup & Restore credentials")
    st.caption(
        "User accounts, API keys, and GitHub settings are stored in `app_auth_credentials.json` "
        "which is **not** pushed to GitHub. Use Export before pushing, and Import after pulling on a new machine."
    )
    _cred_path = _auth_credentials_path()
    col_exp, col_imp = st.columns(2)

    with col_exp:
        st.markdown("**📤 Export**")
        if os.path.isfile(_cred_path):
            with open(_cred_path, "rb") as _cf:
                st.download_button(
                    "Download credentials file",
                    data=_cf.read(),
                    file_name="app_auth_credentials.json",
                    mime="application/json",
                    use_container_width=True,
                    key="admin_export_creds_btn",
                )
            st.caption("Save this file somewhere safe. Never commit it to GitHub.")
        else:
            st.info("No credentials file found yet.")

    with col_imp:
        st.markdown("**📥 Import**")
        _uploaded_creds = st.file_uploader(
            "Upload credentials file",
            type=["json"],
            key="admin_import_creds_upload",
            label_visibility="collapsed",
        )
        if _uploaded_creds is not None:
            try:
                _import_data = json.loads(_uploaded_creds.read().decode("utf-8"))
                if not isinstance(_import_data, dict):
                    st.error("Invalid file format.")
                else:
                    if st.button("✅  Confirm import", key="admin_import_creds_confirm", type="primary", use_container_width=True):
                        with open(_cred_path, "w", encoding="utf-8") as _wf:
                            json.dump(_import_data, _wf, indent=2)
                        st.success("Credentials restored. Reload the page to apply.")
                        st.rerun()
            except Exception as _ie:
                st.error(f"Could not read file: {_ie}")

    st.divider()

    # ── GitHub Push ───────────────────────────────────────────────────────────
    st.subheader("🐙  Push to GitHub")
    st.caption(
        "Upload app files directly to a GitHub repository using a Personal Access Token (PAT). "
        "Sensitive files (`app_auth_credentials.json`, `*.env`) are **never** uploaded."
    )
    _gh_settings = _github_load_settings()
    with st.form("admin_github_push_form"):
        gh_token = st.text_input(
            "GitHub Personal Access Token",
            value=_gh_settings.get("token", ""),
            type="password",
            help="Generate at github.com → Settings → Developer settings → Personal access tokens. Needs repo scope.",
        )
        gh_repo = st.text_input(
            "Repository (owner/repo or full URL)",
            value=_gh_settings.get("repo", ""),
            placeholder="e.g. sagarkhekale-arch/Sagar  or  https://github.com/sagarkhekale-arch/Sagar",
            help="Full GitHub URL is fine — it will be converted automatically.",
        )
        gh_branch = st.text_input(
            "Branch",
            value=_gh_settings.get("branch", "main"),
            placeholder="main",
        )
        gh_save = st.checkbox("Remember these settings (token stored locally only)", value=True)
        if st.form_submit_button("🚀  Push to GitHub", type="primary"):
            if not gh_token or not gh_repo:
                st.error("PAT token and repository are required.")
            else:
                if gh_save:
                    _github_save_settings({"token": gh_token, "repo": gh_repo, "branch": gh_branch or "main"})
                with st.spinner("Pushing files to GitHub…"):
                    results = _github_push_app_files(gh_token, gh_repo, gh_branch or "main")
                ok_files = [r for r in results if r["ok"]]
                err_files = [r for r in results if not r["ok"]]
                if ok_files:
                    st.success(f"✅ Pushed {len(ok_files)} file(s): " + ", ".join(f"`{r['path']}`" for r in ok_files))
                if err_files:
                    for r in err_files:
                        st.error(f"❌ `{r['path']}` — {r['msg']}")


# ── GitHub helper functions ────────────────────────────────────────────────────

_GITHUB_UPLOAD_FILES = [
    "App.py",
    "requirements.txt",
    ".streamlit/config.toml",
    ".gitignore",
]
_GITHUB_SKIP_FILES = {
    "app_auth_credentials.json",
}


def _github_load_settings() -> dict:
    path = _auth_credentials_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        s = data.get("github_settings")
        if isinstance(s, dict):
            return s
    except (OSError, json.JSONDecodeError):
        pass
    return {}


def _github_save_settings(settings: dict) -> None:
    path = _auth_credentials_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        data = {}
    data["github_settings"] = settings
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except OSError:
        pass


def _github_api(token: str, method: str, url: str, body: Optional[dict] = None):
    """Minimal GitHub API call using only stdlib urllib."""
    req_body = json.dumps(body).encode() if body else None
    req = urlrequest.Request(
        url,
        data=req_body,
        method=method,
        headers={
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json",
            "User-Agent": "OperationsDashboard/1.0",
        },
    )
    try:
        with urlrequest.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read().decode())
    except HTTPError as e:
        try:
            err_body = json.loads(e.read().decode())
        except Exception:
            err_body = {}
        return e.code, err_body


def _github_normalise_repo(repo: str) -> str:
    """Accept full URL or owner/repo — always return owner/repo."""
    repo = repo.strip().rstrip("/")
    for prefix in ("https://github.com/", "http://github.com/", "github.com/"):
        if repo.startswith(prefix):
            repo = repo[len(prefix):]
    # strip trailing .git if present
    if repo.endswith(".git"):
        repo = repo[:-4]
    return repo


def _github_push_app_files(token: str, repo: str, branch: str) -> List[dict]:
    """Push whitelisted app files to GitHub via Contents API."""
    repo = _github_normalise_repo(repo)
    branch = branch.strip() or "main"
    app_dir = os.path.dirname(os.path.abspath(__file__))
    results = []

    for rel_path in _GITHUB_UPLOAD_FILES:
        filename = os.path.basename(rel_path)
        if filename in _GITHUB_SKIP_FILES:
            continue
        full_path = os.path.join(app_dir, rel_path.replace("/", os.sep))
        if not os.path.isfile(full_path):
            results.append({"path": rel_path, "ok": False, "msg": "File not found locally — skipped."})
            continue
        try:
            with open(full_path, "rb") as f:
                raw = f.read()
            encoded = base64.b64encode(raw).decode()
        except OSError as e:
            results.append({"path": rel_path, "ok": False, "msg": str(e)})
            continue

        api_url = f"https://api.github.com/repos/{repo}/contents/{rel_path}"

        # Fetch existing file SHA (needed for updates; absent for new files)
        get_status, get_resp = _github_api(
            token, "GET", f"{api_url}?ref={urllib.parse.quote(branch)}"
        )
        sha = get_resp.get("sha") if get_status == 200 else None

        # If repo/branch not found at all, surface a clear message
        if get_status == 404 and "Branch" in get_resp.get("message", ""):
            results.append({
                "path": rel_path,
                "ok": False,
                "msg": (
                    f"Branch '{branch}' not found on GitHub. "
                    "If the repo is empty, create a README on GitHub first to initialise the branch."
                ),
            })
            continue
        if get_status == 401:
            results.append({"path": rel_path, "ok": False, "msg": "Bad credentials — check your PAT token."})
            continue

        payload: dict = {
            "message": f"Update {rel_path} via Operations Dashboard",
            "content": encoded,
            "branch": branch,
        }
        if sha:
            payload["sha"] = sha

        put_status, put_resp = _github_api(token, "PUT", api_url, payload)
        if put_status in (200, 201):
            results.append({"path": rel_path, "ok": True, "msg": ""})
        else:
            msg = put_resp.get("message", f"HTTP {put_status}")
            results.append({"path": rel_path, "ok": False, "msg": msg})

    return results


# Login view only (injected before st.stop() when unauthenticated; not loaded after login)
_LOGIN_SCREEN_CSS = """
<style>
section[data-testid="stSidebar"] { display: none !important; }
[data-testid="stToolbarActions"] { display: none !important; }
.stApp {
    background: #0f0f12 !important;
}
section[data-testid="stMain"] > div {
    background: #0f0f12 !important;
}
/* Card container */
div[data-testid="column"] > div {
    background: #1e1e2a !important;
    border-radius: 28px !important;
    padding: 2rem !important;
    border: 1px solid #3a3a44 !important;
    box-shadow: 0 8px 20px rgba(0,0,0,0.5) !important;
}
/* Title */
section[data-testid="stMain"] h1,
section[data-testid="stMain"] h2,
section[data-testid="stMain"] h3 {
    color: #f8fafc !important;
    text-align: center !important;
}
/* All body text — paragraphs, captions, markdowns */
section[data-testid="stMain"] p,
section[data-testid="stMain"] span,
section[data-testid="stMain"] label,
section[data-testid="stMain"] [data-testid="stMarkdownContainer"] p,
section[data-testid="stMain"] [data-testid="stCaption"] p {
    color: #cbd5e1 !important;
}
/* Input labels */
section[data-testid="stMain"] .stTextInput label,
section[data-testid="stMain"] .stTextInput [data-testid="stWidgetLabel"] p {
    color: #94a3b8 !important;
    font-weight: 500 !important;
}
/* Input boxes */
section[data-testid="stMain"] .stTextInput input {
    background: #2d2d3d !important;
    color: #f1f5f9 !important;
    border: 1px solid #3f3f55 !important;
    border-radius: 10px !important;
}
section[data-testid="stMain"] .stTextInput input::placeholder {
    color: #64748b !important;
}
/* Login button */
section[data-testid="stMain"] button[kind="primary"] {
    background: linear-gradient(135deg, #4f46e5 0%, #6366f1 100%) !important;
    border-radius: 40px !important;
    font-weight: 600 !important;
    color: #fff !important;
    width: 100% !important;
    border: none !important;
}
section[data-testid="stMain"] button[kind="primary"] p {
    color: #fff !important;
}
/* Secondary button */
section[data-testid="stMain"] button[kind="secondary"] {
    background: transparent !important;
    border: 1px solid #4f46e5 !important;
    color: #cbd5e1 !important;
    border-radius: 40px !important;
}
/* Toggle / checkbox */
section[data-testid="stMain"] .stCheckbox label span,
section[data-testid="stMain"] .stToggle label p,
section[data-testid="stMain"] .stToggle label span {
    color: #e2e8f0 !important;
}
/* Alerts */
section[data-testid="stMain"] div[data-testid="stAlert"] {
    border-radius: 16px !important;
}
/* Divider */
section[data-testid="stMain"] hr {
    border-color: #3a3a44 !important;
}
</style>
"""


@st.cache_resource
def get_gemini_model(api_key: str, model_name: str):
    if genai is None:
        raise RuntimeError("google-generativeai is not installed. Install it or switch AI provider to Ollama.")
    genai.configure(api_key=api_key)
    return genai.GenerativeModel(model_name)


def _ollama_generate_request(url: str, payload: dict) -> str:
    data = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urlrequest.urlopen(req, timeout=300) as resp:
        body = resp.read().decode("utf-8")
    out = json.loads(body)
    return (out.get("response") or "").strip()


def call_ollama_generate(base_url: str, model: str, prompt: str) -> str:
    """
    Calls Ollama /api/generate with automatic recovery:
    1) Respect sidebar Force CPU (num_gpu=0)
    2) If runner crashes (HTTP 500), retry with forced num_gpu=0
    3) If still failing, retry with smaller fallback model (env OLLAMA_FALLBACK_MODEL, default qwen2.5:3b-instruct)
    """
    url = base_url.rstrip("/") + "/api/generate"
    fallback_model = os.getenv("OLLAMA_FALLBACK_MODEL", "qwen2.5:3b-instruct")
    force_cpu_pref = st.session_state.get("ollama_force_cpu", True)

    attempts = [(model, force_cpu_pref, "primary")]
    if not force_cpu_pref:
        attempts.append((model, True, "primary+force_cpu"))
    if fallback_model and fallback_model != model:
        attempts.append((fallback_model, True, f"fallback:{fallback_model}"))

    last_runner_err = ""
    for use_model, use_cpu, tag in attempts:
        opts: dict = {"temperature": 0.2}
        if use_cpu:
            opts["num_gpu"] = 0
        payload = {
            "model": use_model,
            "prompt": prompt,
            "stream": False,
            "options": opts,
        }
        try:
            text = _ollama_generate_request(url, payload)
            if tag.startswith("fallback"):
                prefix = f"_(Ollama used fallback model `{use_model}` because `{model}` crashed on this PC.)_\n\n"
                return prefix + text
            if tag == "primary+force_cpu" and not force_cpu_pref:
                return "_(Retry used CPU-only because the GPU runner crashed.)_\n\n" + text
            return text
        except HTTPError as e:
            err_body = ""
            try:
                err_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            if e.code == 404:
                if tag.startswith("fallback"):
                    raise RuntimeError(
                        f"Fallback model `{use_model}` is not installed. Run: `ollama pull {use_model}`. {err_body}"
                    ) from e
                raise RuntimeError(
                    f"Ollama model `{use_model}` is not installed (HTTP 404). "
                    f"Run: `ollama pull {use_model}` then try again, or set **Ollama model** to one from `ollama list`. "
                    f"{err_body}"
                ) from e
            is_runner = e.code == 500 and (
                "runner" in err_body.lower() or "terminated" in err_body.lower()
            )
            if is_runner:
                last_runner_err = err_body
                continue
            raise RuntimeError(f"Ollama HTTP {e.code} at {base_url}. {err_body or e}") from e
        except URLError as e:
            raise RuntimeError(
                f"Ollama unreachable: {e}. Is the server running at {base_url}? (Try `ollama serve`.)"
            ) from e

    raise RuntimeError(
        f"Ollama model runner still failing after retries (often GPU/VRAM). "
        f"Run: `ollama pull {fallback_model}` if missing, then set **Ollama model** to `{fallback_model}`, "
        f"or start Ollama with `$env:OLLAMA_NO_GPU='1'; ollama serve`. Last error: {last_runner_err}"
    )


def extract_first_json_object(text: str) -> dict:
    """
    LLMs sometimes return extra text. Try to extract the first JSON object.
    """
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty LLM response.")
    if text.startswith("{") and text.endswith("}"):
        return json.loads(text)
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        raise ValueError("Could not find JSON object in LLM response.")
    return json.loads(m.group(0))

# --- Database Connection Functions ---

@st.cache_resource
def get_bigquery_client(project_id: str, service_account_info_json: Optional[str]):
    """
    Returns a cached BigQuery client.

    Auth options:
    - If service_account_info_json is provided: uses those service-account credentials.
    - Else: uses Application Default Credentials (ADC), e.g. via GOOGLE_APPLICATION_CREDENTIALS.
    """
    if service_account_info_json:
        info = json.loads(service_account_info_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        if project_id:
            return bigquery.Client(project=project_id, credentials=credentials)
        # Fall back to the service-account's project_id if present
        return bigquery.Client(project=getattr(credentials, "project_id", None), credentials=credentials)

    if project_id:
        return bigquery.Client(project=project_id)
    # Let the library infer the default project from ADC.
    return bigquery.Client()


@st.cache_data
def query_bigquery(query: str, query_project_id: str, service_account_info_json: Optional[str]):
    """Runs a BigQuery SQL query and returns a DataFrame."""
    client = get_bigquery_client(query_project_id, service_account_info_json)
    return client.query(query).to_dataframe()


def test_bigquery_permissions(query_project_id: str, service_account_info_json: Optional[str]) -> tuple[bool, str]:
    """
    Runs a lightweight query to verify jobs.create permission in the runner project.
    """
    try:
        client = get_bigquery_client(query_project_id, service_account_info_json)
        client.query("SELECT 1 AS ok").result()
        return True, f"Permission OK. Query jobs can run in `{query_project_id}`."
    except Exception as e:
        return False, str(e)

@st.cache_data
def query_sql_db(connection_string, query):
    """Connects to any SQL DB (Postgres, MySQL) via SQLAlchemy."""
    engine = create_engine(connection_string)
    df = pd.read_sql(query, engine)
    return df

# --- BigQuery browsing helpers (datasets/tables) ---

@st.cache_data(ttl=300)
def list_bq_datasets(project_id: str, service_account_info_json: Optional[str]) -> list[str]:
    client = get_bigquery_client(project_id, service_account_info_json)
    return sorted([d.dataset_id for d in client.list_datasets(project_id)])


@st.cache_data(ttl=300)
def list_bq_tables(project_id: str, dataset_id: str, service_account_info_json: Optional[str]) -> list[str]:
    client = get_bigquery_client(project_id, service_account_info_json)
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    return sorted([t.table_id for t in client.list_tables(dataset_ref)])


@st.cache_data(ttl=300)
def load_bq_table_sample(table_ref: str, project_id: str, service_account_info_json: Optional[str], row_limit: int) -> pd.DataFrame:
    """
    Loads a sample directly from BigQuery table storage (no query job).
    """
    client = get_bigquery_client(project_id, service_account_info_json)
    table = client.get_table(table_ref)
    rows_iter = client.list_rows(table, max_results=row_limit)
    return rows_iter.to_dataframe()


def filter_dataframe_by_date_range(
    df: pd.DataFrame, column: str, start: Optional[date], end: Optional[date]
) -> pd.DataFrame:
    """Keep rows whose `column` parses to a datetime within [start, end] (inclusive calendar days)."""
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found.")
    # Normalize to UTC-aware so comparisons work for tz-naive and tz-aware (e.g. BigQuery UTC) columns.
    ts = pd.to_datetime(df[column], errors="coerce", utc=True)
    mask = ts.notna()
    if start is not None:
        lo = pd.Timestamp(start, tz="UTC")
        mask &= ts >= lo
    if end is not None:
        hi_excl = pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)
        mask &= ts < hi_excl
    return df.loc[mask].copy()


def sample_column_datetime_summary(df: pd.DataFrame, column: str) -> str:
    """Human-readable min/max UTC for the column in this dataframe (for diagnostics)."""
    if column not in df.columns:
        return ""
    ts = pd.to_datetime(df[column], errors="coerce", utc=True)
    valid = ts.dropna()
    if len(valid) == 0:
        return f"No valid datetimes in `{column}` in this sample ({len(df):,} rows)."
    lo = valid.min()
    hi = valid.max()
    return (
        f"In this **{len(df):,}**-row sample, `{column}` ranges **{lo}** → **{hi}** (UTC), "
        f"**{len(valid):,}** non-null values."
    )


def google_sheet_to_csv_export_url(user_input: str) -> str:
    """Turn a Sheets browser URL into a CSV export URL (works if the sheet is accessible without login)."""
    s = (user_input or "").strip()
    if not s:
        return ""
    if "format=csv" in s and "spreadsheets" in s:
        return s
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", s)
    if m:
        sid = m.group(1)
        gm = re.search(r"[?&#]gid=(\d+)", s)
        gid = gm.group(1) if gm else "0"
        return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={gid}"
    return s


_GDRIVE_OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]


def _gdrive_oauth_auth_url(client_id: str, client_secret: str, redirect_uri: str) -> Tuple[str, str]:
    """
    Build a Google OAuth2 authorization URL.
    Returns (auth_url, state_token).
    The user visits auth_url, approves, and Google redirects back to redirect_uri?code=XXX.
    """
    import urllib.parse as _up
    import secrets as _sec
    state = _sec.token_hex(16)
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(_GDRIVE_OAUTH_SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + _up.urlencode(params), state


def _gdrive_oauth_save_app_creds(client_id: str, client_secret: str, redirect_uri: str) -> None:
    """Persist OAuth2 app credentials so user doesn't re-enter them."""
    path = _auth_credentials_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        data = {}
    data["gdrive_oauth_app"] = {
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except OSError:
        pass


def _gdrive_oauth_load_app_creds() -> dict:
    """Load saved OAuth2 app credentials."""
    path = _auth_credentials_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        c = data.get("gdrive_oauth_app", {})
        if isinstance(c, dict):
            return c
    except (OSError, json.JSONDecodeError):
        pass
    return {}


def _gdrive_oauth_exchange_code(
    client_id: str, client_secret: str, redirect_uri: str, code: str
) -> dict:
    """Exchange an OAuth2 authorization code for access + refresh tokens."""
    import urllib.request as _ur
    import urllib.parse as _up
    body = _up.urlencode(
        {
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }
    ).encode()
    req = _ur.Request(
        "https://oauth2.googleapis.com/token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with _ur.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _gdrive_list_sheets_oauth(token: str) -> List[dict]:
    """List Google Sheets using an OAuth2 Bearer token."""
    if not _GDRIVE_OK or _gdrive_build is None:
        raise ImportError("pip install google-api-python-client")
    import google.oauth2.credentials as _gc
    creds = _gc.Credentials(token=token)
    service = _gdrive_build("drive", "v3", credentials=creds, cache_discovery=False)
    results = (
        service.files()
        .list(
            q="mimeType='application/vnd.google-apps.spreadsheet' and trashed=false",
            fields="files(id,name,modifiedTime)",
            pageSize=200,
            orderBy="modifiedTime desc",
        )
        .execute()
    )
    return results.get("files", [])


def _gdrive_sheet_tabs_oauth(token: str, file_id: str) -> List[dict]:
    """Return worksheet tabs using an OAuth2 Bearer token."""
    if not _GDRIVE_OK or _gdrive_build is None:
        return []
    import google.oauth2.credentials as _gc
    creds = _gc.Credentials(token=token)
    svc = _gdrive_build("sheets", "v4", credentials=creds, cache_discovery=False)
    meta = svc.spreadsheets().get(spreadsheetId=file_id, fields="sheets.properties").execute()
    return [
        {"sheetId": s["properties"]["sheetId"], "title": s["properties"]["title"]}
        for s in meta.get("sheets", [])
    ]


def _gdrive_load_sheet_tab_oauth(token: str, file_id: str, sheet_title: str) -> pd.DataFrame:
    """Download one worksheet tab using an OAuth2 Bearer token."""
    import google.oauth2.credentials as _gc
    creds = _gc.Credentials(token=token)
    svc = _gdrive_build("sheets", "v4", credentials=creds, cache_discovery=False)
    resp = svc.spreadsheets().values().get(spreadsheetId=file_id, range=sheet_title).execute()
    rows = resp.get("values", [])
    if not rows:
        return pd.DataFrame()
    header, *data = rows
    n = len(header)
    return pd.DataFrame([r + [""] * (n - len(r)) for r in data], columns=header)


# ── Apps Script data source ────────────────────────────────────────────────────

_APPS_SCRIPT_CODE = '''\
// ─────────────────────────────────────────────────────────────────
// Paste this into your Google Sheet:
//   Extensions → Apps Script → replace everything → Save → Deploy
// Deploy settings:
//   Execute as: Me   |   Who has access: Anyone
// Copy the Web App URL and paste it into the app sidebar.
// ─────────────────────────────────────────────────────────────────
function doGet(e) {
  var sheetName = (e && e.parameter && e.parameter.sheet) ? e.parameter.sheet : "";
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = sheetName ? ss.getSheetByName(sheetName) : ss.getSheets()[0];
  if (!sheet) {
    return ContentService
      .createTextOutput(JSON.stringify({error: "Sheet not found: " + sheetName}))
      .setMimeType(ContentService.MimeType.JSON);
  }
  var range = sheet.getDataRange();
  var values = range.getValues();
  if (values.length === 0) {
    return ContentService
      .createTextOutput(JSON.stringify({sheet: sheet.getName(), rows: [], count: 0}))
      .setMimeType(ContentService.MimeType.JSON);
  }
  var headers = values[0].map(String);
  var rows = [];
  for (var i = 1; i < values.length; i++) {
    var obj = {};
    for (var j = 0; j < headers.length; j++) {
      obj[headers[j]] = values[i][j];
    }
    rows.push(obj);
  }
  // List available sheet names in metadata
  var sheetNames = ss.getSheets().map(function(s){ return s.getName(); });
  return ContentService
    .createTextOutput(JSON.stringify({
      sheet: sheet.getName(),
      sheets: sheetNames,
      count: rows.length,
      rows: rows
    }))
    .setMimeType(ContentService.MimeType.JSON);
}
'''


def _appscript_fetch(web_app_url: str, sheet_name: str = "") -> pd.DataFrame:
    """Fetch data from a Google Sheet via a deployed Apps Script Web App."""
    url = web_app_url.strip()
    if sheet_name:
        url += ("&" if "?" in url else "?") + urllib.parse.quote_plus(f"sheet={sheet_name}", safe="=")
    req = urlrequest.Request(url, headers={"User-Agent": "OperationsDashboard/1.0"})
    with urlrequest.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if "error" in payload:
        raise ValueError(payload["error"])
    rows = payload.get("rows", [])
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _appscript_list_sheets(web_app_url: str) -> List[str]:
    """Return the list of sheet/tab names from the Apps Script endpoint."""
    url = web_app_url.strip()
    req = urlrequest.Request(url, headers={"User-Agent": "OperationsDashboard/1.0"})
    with urlrequest.urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return payload.get("sheets", [])


def _gdrive_list_sheets(sa_info: dict) -> List[dict]:
    """
    List Google Sheets accessible by the service account.
    Returns list of {id, name, modifiedTime}.
    Requires google-api-python-client (pip install google-api-python-client).
    """
    if not _GDRIVE_OK or _gdrive_build is None:
        raise ImportError(
            "google-api-python-client not installed. Run: pip install google-api-python-client"
        )
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    service = _gdrive_build("drive", "v3", credentials=creds, cache_discovery=False)
    results = (
        service.files()
        .list(
            q="mimeType='application/vnd.google-apps.spreadsheet' and trashed=false",
            fields="files(id,name,modifiedTime)",
            pageSize=200,
            orderBy="modifiedTime desc",
        )
        .execute()
    )
    return results.get("files", [])


def _gdrive_sheet_tabs(sa_info: dict, file_id: str) -> List[dict]:
    """Return worksheet tabs for a Sheets file: [{sheetId, title}]."""
    if not _GDRIVE_OK or _gdrive_build is None:
        return []
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    svc = _gdrive_build("sheets", "v4", credentials=creds, cache_discovery=False)
    meta = svc.spreadsheets().get(spreadsheetId=file_id, fields="sheets.properties").execute()
    return [
        {"sheetId": s["properties"]["sheetId"], "title": s["properties"]["title"]}
        for s in meta.get("sheets", [])
    ]


def _gdrive_load_sheet_tab(sa_info: dict, file_id: str, sheet_title: str) -> pd.DataFrame:
    """Download one worksheet tab and return as DataFrame."""
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    svc = _gdrive_build("sheets", "v4", credentials=creds, cache_discovery=False)
    resp = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=file_id, range=sheet_title)
        .execute()
    )
    rows = resp.get("values", [])
    if not rows:
        return pd.DataFrame()
    header, *data = rows
    # Pad shorter rows so all rows have the same number of columns
    n = len(header)
    padded = [r + [""] * (n - len(r)) for r in data]
    return pd.DataFrame(padded, columns=header)


def extract_sql_from_text(text: str) -> Optional[str]:
    """
    Pull executable SQL from an AI reply: ```sql ... ``` blocks or a `sql: SELECT ...` line.
    """
    if not text:
        return None
    m = re.search(r"```sql\s*([\s\S]*?)```", text, re.I)
    if m:
        s = m.group(1).strip().rstrip(";")
        if s:
            return s
    m = re.search(r"(?is)sql:\s*((?:SELECT|WITH)\s+.*?;)", text)
    if m:
        return m.group(1).strip().rstrip(";")
    m = re.search(r"(?is)sql:\s*((?:SELECT|WITH)\s+[\s\S]+?)(?:\n\n|\Z)", text)
    if m:
        return m.group(1).strip().rstrip(";")
    m = re.search(r"(?im)^sql:\s*(.+)$", text)
    if m:
        return m.group(1).strip().rstrip(";")
    return None


def run_sql_on_dataframe(df: pd.DataFrame, sql: str) -> pd.DataFrame:
    """
    Run SQL (DuckDB) on an in-memory DataFrame registered as `data`.
    Use: SELECT col, COUNT(*) FROM data GROUP BY col
    `FROM data.csv` / FROM data.xlsx are rewritten to FROM data.
    """
    try:
        import duckdb
    except ImportError as e:
        raise RuntimeError("Install duckdb to run SQL on CSV/Excel: pip install duckdb") from e
    sql = (sql or "").strip().rstrip(";")
    sql = re.sub(r"(?i)FROM\s+[`\"']?data\.csv[`\"']?", "FROM data", sql)
    sql = re.sub(r"(?i)FROM\s+[`\"']?data\.xlsx[`\"']?", "FROM data", sql)
    sql = re.sub(r"(?i)FROM\s+[`\"']?data\.xls[`\"']?", "FROM data", sql)
    con = duckdb.connect(database=":memory:")
    con.register("data", df)
    return con.execute(sql).df()


def read_uploaded_table_file(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    raw = uploaded_file.getvalue()
    if name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(raw))
    if name.endswith(".xlsx"):
        try:
            return pd.read_excel(io.BytesIO(raw), engine="openpyxl")
        except ImportError as e:
            raise RuntimeError("Install openpyxl for .xlsx: pip install openpyxl") from e
    if name.endswith(".xls"):
        return pd.read_excel(io.BytesIO(raw))
    raise ValueError("Use .csv, .xlsx, or .xls")


# --- AI helpers ---

def _schema_to_text(table: bigquery.Table) -> str:
    lines = []
    for field in table.schema:
        mode = f" ({field.mode})" if field.mode else ""
        desc = f" - {field.description}" if field.description else ""
        lines.append(f"- {field.name}: {field.field_type}{mode}{desc}")
    return "\n".join(lines) if lines else "- (no schema found)"


def generate_bigquery_sql_from_question(
    question: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
    table_schema_text: str,
    ai_provider: str,
    gemini_api_key: str,
    gemini_model_name: str,
    ollama_base_url: str,
    ollama_model: str,
) -> str:
    prompt = textwrap.dedent(
        f"""
        You are an expert analytics engineer.

        Write a BigQuery Standard SQL query to answer the user's question using ONLY this table:
        `{project_id}.{dataset_id}.{table_id}`

        Table schema:
        {table_schema_text}

        Requirements:
        - Output ONLY SQL (no markdown, no explanations).
        - Use BigQuery Standard SQL.
        - Be conservative: if the question is ambiguous, return a query that surfaces the needed breakdown(s).
        - Prefer explicit column lists (avoid SELECT *).
        - If a date/time column exists, include a recent time filter when reasonable (e.g., last 30/90 days).

        User question:
        {question}
        """
    ).strip()

    if ai_provider == "Local (Ollama)":
        sql = call_ollama_generate(ollama_base_url, ollama_model, prompt)
    else:
        model = get_gemini_model(gemini_api_key, gemini_model_name)
        resp = model.generate_content(prompt)
        sql = (resp.text or "").strip()
    # Basic cleanup: remove fenced blocks if the model includes them anyway.
    if sql.startswith("```"):
        sql = sql.strip("`")
        sql = sql.replace("sql\n", "", 1).strip()
    return sql


# --- AI Insight Function ---

def generate_ai_insights(
    df,
    metric_name,
    ai_provider: str,
    gemini_api_key: str,
    gemini_model_name: str,
    ollama_base_url: str,
    ollama_model: str,
):
    """Passes data context to the AI to explain up/down trends."""
    # Convert a sample or summary of the dataframe to a string for the AI
    data_summary = df.describe().to_string()
    recent_data = df.tail(10).to_string() # Send the latest rows to spot recent trends
    
    prompt = f"""
    You are an expert Data Analyst. I am providing you with data regarding '{metric_name}'.
    
    Data Summary:
    {data_summary}
    
    Recent Data (Last 10 records):
    {recent_data}
    
    Task: 
    1. Analyze the trends. Is the metric going up or down?
    2. Provide a 'Full Insight' explaining exactly WHY it is up or down based ONLY on the provided data. Look for correlations (e.g., did a specific region drop? Did costs spike?).
    3. Keep it professional, highly analytical, and easy to read.
    """
    
    try:
        if ai_provider == "Local (Ollama)":
            return call_ollama_generate(ollama_base_url, ollama_model, prompt)
        model = get_gemini_model(gemini_api_key, gemini_model_name)
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"Could not generate insights. Error: {e}"


def call_ai_text_unified(
    prompt: str,
    ai_provider: str,
    gemini_api_key: str,
    gemini_model_name: str,
    ollama_base_url: str,
    ollama_model: str,
) -> str:
    if ai_provider == "Local (Ollama)":
        return call_ollama_generate(ollama_base_url, ollama_model, prompt)
    model = get_gemini_model(gemini_api_key, gemini_model_name)
    resp = model.generate_content(prompt)
    return (resp.text or "").strip()


def render_loaded_data_panel(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    ai_provider: str,
    gemini_api_key: str,
    gemini_model_name: str,
    ollama_base_url: str,
    ollama_model: str,
) -> None:
    """Table + optional chart + insight — always show the dataframe prominently."""
    st.markdown(
        """
        <div class="pnl-card">
          <h3 class="pnl-card-title" style="margin:0 0 0.25rem 0;font-family:Outfit,DM Sans,sans-serif;">Loaded dataset</h3>
          <p class="pnl-card-desc" style="margin:0;font-size:0.9rem;">Preview, chart, and optional AI insight on this table.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.success(
        f"**{len(df):,}** rows · **{len(df.columns)}** columns — full loaded data (scroll)."
    )
    st.dataframe(df, use_container_width=True, height=480)

    st.markdown("### Quick chart (optional)")
    if len(df.columns) >= 2:
        c1, c2 = st.columns(2)
        with c1:
            x_axis = st.selectbox("X", df.columns, key=f"{key_prefix}_chart_x")
        with c2:
            y_axis = st.selectbox("Y", df.columns, key=f"{key_prefix}_chart_y")
        try:
            fig = px.line(df, x=x_axis, y=y_axis, title=f"{y_axis} over {x_axis}")
            st.plotly_chart(fig, use_container_width=True)
        except Exception:
            try:
                fig = px.bar(df, x=x_axis, y=y_axis, title=f"{y_axis} by {x_axis}")
                st.plotly_chart(fig, use_container_width=True)
            except Exception:
                st.info("Pick columns that work for plotting (e.g. time + number).")
    else:
        st.caption("Need at least two columns to chart.")

    st.markdown("### AI insight on loaded data (optional)")
    if st.button("Generate insight on loaded data", key=f"{key_prefix}_insight_btn"):
        if ai_provider == "Gemini API" and not gemini_api_key:
            st.error("Add Gemini API key in sidebar, or switch provider to Local (Ollama).")
        else:
            with st.spinner("Analyzing…"):
                df_for_ai = df.copy()
                if len(df_for_ai) > 2000:
                    df_for_ai = df_for_ai.head(2000)
                st.write(
                    generate_ai_insights(
                        df_for_ai,
                        "loaded dataset",
                        ai_provider=ai_provider,
                        gemini_api_key=gemini_api_key,
                        gemini_model_name=gemini_model_name,
                        ollama_base_url=ollama_base_url,
                        ollama_model=ollama_model,
                    )
                )


_OPS_DASHBOARD_CSS = """
<style>
.ops-dash-shell {
  border-radius: 20px;
  padding: 1.35rem 1.5rem 1.5rem;
  margin-bottom: 1.25rem;
  background: linear-gradient(145deg, rgba(99, 102, 241, 0.12) 0%, rgba(6, 182, 212, 0.08) 45%, rgba(15, 23, 42, 0.04) 100%);
  border: 1px solid rgba(99, 102, 241, 0.22);
  box-shadow: 0 8px 32px rgba(15, 23, 42, 0.06);
}
.ops-dash-title {
  font-family: Outfit, DM Sans, system-ui, sans-serif;
  font-size: 1.35rem;
  font-weight: 700;
  letter-spacing: -0.03em;
  margin: 0 0 0.35rem 0;
  background: linear-gradient(90deg, #312e81, #0891b2);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}
.ops-dash-sub { margin: 0; font-size: 0.92rem; color: #64748b; }
.ops-kpi-row { display: flex; gap: 1rem; flex-wrap: wrap; margin-top: 0.75rem; }
.ops-kpi-card {
  flex: 1 1 200px;
  border-radius: 16px;
  padding: 1.1rem 1.15rem;
  margin: 0.5rem 0.45rem;
  box-sizing: border-box;
  background: linear-gradient(180deg, rgba(255,255,255,0.92) 0%, rgba(248,250,252,0.88) 100%);
  border: 1px solid rgba(148, 163, 184, 0.35);
  box-shadow: 0 4px 20px rgba(15, 23, 42, 0.05);
  transition: transform 0.15s ease, box-shadow 0.15s ease;
}
.ops-kpi-card:hover { box-shadow: 0 8px 28px rgba(79, 70, 229, 0.12); transform: translateY(-1px); }
.ops-kpi-card.active {
  border-color: rgba(79, 70, 229, 0.55);
  box-shadow: 0 0 0 2px rgba(79, 70, 229, 0.2), 0 8px 28px rgba(79, 70, 229, 0.12);
}
.ops-kpi-label { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.08em; color: #64748b; font-weight: 600; margin-bottom: 0.35rem; }
.ops-kpi-value { font-size: 1.65rem; font-weight: 700; font-family: Outfit, DM Sans, sans-serif; color: #0f172a; line-height: 1.1; }
.ops-kpi-delta-up { color: #059669; font-size: 0.95rem; font-weight: 600; margin-top: 0.45rem; }
.ops-kpi-delta-down { color: #dc2626; font-size: 0.95rem; font-weight: 600; margin-top: 0.45rem; }
.ops-kpi-delta-flat { color: #64748b; font-size: 0.95rem; margin-top: 0.45rem; }
.ops-kpi-meta { font-size: 0.75rem; color: #94a3b8; margin-top: 0.35rem; }
.ops-kpi-rider { font-size: 1.05rem; font-weight: 600; color: #334155; margin-top: 0.4rem; }
.ops-kpi-rider-delta { font-size: 0.82rem; font-weight: 500; margin-top: 0.2rem; }
/* Dark theme — Luna-style: charcoal surfaces, amber accent, white metrics, aligned card height */
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-dash-shell {
  background: linear-gradient(145deg, rgba(245, 158, 11, 0.07) 0%, rgba(30, 34, 41, 0.95) 45%, rgba(15, 17, 23, 0.99) 100%);
  border: 1px solid rgba(245, 158, 11, 0.2);
  box-shadow: 0 8px 40px rgba(0, 0, 0, 0.45);
}
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-dash-title {
  background: none !important;
  -webkit-text-fill-color: #f8fafc !important;
  color: #f8fafc !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-dash-sub { color: #94a3b8 !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-card {
  background: linear-gradient(165deg, #2f343d 0%, #262a32 50%, #1e2229 100%);
  border: 1px solid rgba(255, 255, 255, 0.08);
  box-shadow: 0 4px 22px rgba(0, 0, 0, 0.4), inset 0 1px 0 rgba(255, 255, 255, 0.04);
  min-height: 188px;
  margin: 0.5rem 0.45rem;
  box-sizing: border-box;
}
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-card:hover {
  border-color: rgba(245, 158, 11, 0.28);
  box-shadow: 0 10px 36px rgba(0, 0, 0, 0.55);
}
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-card.active {
  border-color: rgba(245, 158, 11, 0.45);
  box-shadow: 0 0 0 1px rgba(245, 158, 11, 0.22), 0 8px 28px rgba(245, 158, 11, 0.1);
}
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-label { color: #94a3b8 !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-value { color: #f8fafc !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-delta-up { color: #fbbf24 !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-delta-down { color: #f87171 !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-delta-flat { color: #64748b !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-meta { color: #64748b !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-rider { color: #e2e8f0 !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-rider-delta { color: #cbd5e1 !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-rider-delta.ops-kpi-delta-up { color: #fbbf24 !important; }
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-rider-delta.ops-kpi-delta-down { color: #f87171 !important; }
.ops-kpi-card.ops-kpi-alert {
  border-color: rgba(248, 113, 113, 0.65) !important;
  box-shadow: 0 0 0 2px rgba(248, 113, 113, 0.25), 0 4px 20px rgba(248, 113, 113, 0.12) !important;
}
[data-testid="stAppViewContainer"][data-theme="dark"] .ops-kpi-card.ops-kpi-alert {
  border-color: rgba(248, 113, 113, 0.55) !important;
  box-shadow: 0 0 0 2px rgba(248, 113, 113, 0.3), 0 8px 28px rgba(0, 0, 0, 0.45) !important;
}
</style>
"""


def _ops_theme_is_dark() -> bool:
    try:
        if hasattr(st, "context") and hasattr(st.context, "theme"):
            return getattr(st.context.theme, "base", None) == "dark"
    except Exception:
        pass
    return False


def _ops_apply_fig_theme(
    fig,
    *,
    is_dark: bool,
    margin: Optional[dict] = None,
    legend: Optional[dict] = None,
) -> None:
    m = {"t": 48, "b": 40, "l": 48, "r": 24}
    if margin:
        m = {**m, **margin}
    if is_dark:
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="#1a1d24",
            font=dict(color="#e2e8f0", size=12),
            margin=m,
            colorway=["#f59e0b", "#38bdf8", "#a78bfa", "#34d399", "#fb7185", "#fbbf24"],
        )
        if legend:
            fig.update_layout(legend=legend)
        try:
            fig.update_xaxes(
                gridcolor="rgba(255,255,255,0.08)",
                zerolinecolor="rgba(255,255,255,0.1)",
                tickfont=dict(color="#94a3b8"),
                title_font=dict(color="#cbd5e1"),
            )
            fig.update_yaxes(
                gridcolor="rgba(255,255,255,0.08)",
                zerolinecolor="rgba(255,255,255,0.1)",
                tickfont=dict(color="#94a3b8"),
                title_font=dict(color="#cbd5e1"),
            )
        except Exception:
            pass
    else:
        fig.update_layout(
            template="plotly_white",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(248,250,252,0.95)",
            margin=m,
        )
        if legend:
            fig.update_layout(legend=legend)


def _opd_status_metrics(dfx: pd.DataFrame, order_col: str, status_col: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """Returns (OPD status 5+6, Delivered 5, Rejected 6) distinct order_id counts."""
    if not status_col or status_col not in dfx.columns or dfx.empty:
        return None, None, None
    stt = pd.to_numeric(dfx[status_col], errors="coerce")
    oid = dfx[order_col]
    opd = oid[stt.isin([5, 6])].nunique()
    del5 = oid[stt == 5].nunique()
    rej6 = oid[stt == 6].nunique()
    return int(opd), int(del5), int(rej6)


def _ops_counts_or_zero(
    dfx: pd.DataFrame, order_col: str, status_col: Optional[str]
) -> Tuple[int, int, int]:
    o, a, b = _opd_status_metrics(dfx, order_col, status_col)
    if o is None:
        return 0, 0, 0
    return int(o), int(a), int(b)


def _pct_vs_d1(cur: int, prev: int) -> Tuple[Optional[float], str]:
    if prev > 0:
        p = (cur - prev) / prev * 100.0
        if p > 1e-9:
            return p, "↑"
        if p < -1e-9:
            return p, "↓"
        return 0.0, "→"
    if prev == 0 and cur > 0:
        return None, "↑"
    return None, "→"


_TOP_CITY_LABELS = [
    "Bengaluru",
    "Hyderabad",
    "Chennai",
    "Kolkata",
    "Mumbai",
    "Navi Mumbai",
    "Delhi",
    "Faridabad",
    "Ghaziabad",
    "Gurgaon",
    "Noida",
]


def _norm_city_token(val: object) -> str:
    if pd.isna(val):
        return ""
    s = str(val).strip().lower()
    return " ".join(s.split())


def _norm_set_for_top_city_label(label: str) -> frozenset:
    m = {
        "Bengaluru": frozenset({"bengaluru", "bangalore"}),
        "Hyderabad": frozenset({"hyderabad"}),
        "Chennai": frozenset({"chennai"}),
        "Kolkata": frozenset({"kolkata"}),
        "Mumbai": frozenset({"mumbai"}),
        "Navi Mumbai": frozenset({"navi mumbai"}),
        "Delhi": frozenset({"delhi", "new delhi"}),
        "Faridabad": frozenset({"faridabad"}),
        "Ghaziabad": frozenset({"ghaziabad", "gazhiabad"}),
        "Gurgaon": frozenset({"gurgaon", "gurugram"}),
        "Noida": frozenset({"noida"}),
    }
    return m.get(label, frozenset({_norm_city_token(label)}))


def _row_matches_top_city_label(val: object, label: str) -> bool:
    return _norm_city_token(val) in _norm_set_for_top_city_label(label)


def _is_top_tier_city(val: object) -> bool:
    return any(_row_matches_top_city_label(val, lab) for lab in _TOP_CITY_LABELS)


# HR = hour-of-day buckets from creation_time (local hour 0–23).
_OPS_HR_SLOT_DEFS: Tuple[Tuple[str, str], ...] = (
    ("breakfast", "Breakfast (7am–12pm)"),
    ("lunch", "Lunch (12pm–4pm)"),
    ("snacks", "Snacks (4pm–7pm)"),
    ("dinner", "Dinner (7pm–10pm)"),
    ("latenight", "Late night (10pm–4am)"),
)
_OPS_HR_LABEL_TO_KEY = {label: key for key, label in _OPS_HR_SLOT_DEFS}


def _ops_hr_hour_in_slot(hour_val: object, slot_key: str) -> bool:
    if hour_val is None or (isinstance(hour_val, float) and pd.isna(hour_val)):
        return False
    try:
        h = int(hour_val)
    except (TypeError, ValueError):
        return False
    if slot_key == "breakfast":
        return 7 <= h < 12
    if slot_key == "lunch":
        return 12 <= h < 16
    if slot_key == "snacks":
        return 16 <= h < 19
    if slot_key == "dinner":
        return 19 <= h < 22
    if slot_key == "latenight":
        return h >= 22 or h < 4
    return False


def _slot_opd_rider_counts(
    dfx: pd.DataFrame,
    slot_key: str,
    order_col: str,
    rider_col: Optional[str],
    status_col: str,
) -> Tuple[int, int]:
    """Distinct order_id and distinct rider_id on OPD rows (status 5 or 6) in this HR slot."""
    if dfx.empty or "_chr" not in dfx.columns:
        return 0, 0
    m_slot = dfx["_chr"].map(lambda h, s=slot_key: _ops_hr_hour_in_slot(h, s))
    sub = dfx.loc[m_slot]
    if sub.empty:
        return 0, 0
    stt = pd.to_numeric(sub[status_col], errors="coerce")
    opd_rows = sub.loc[stt.isin([5, 6])]
    if opd_rows.empty:
        return 0, 0
    o = int(opd_rows[order_col].nunique())
    if rider_col and rider_col in opd_rows.columns:
        r = int(opd_rows[rider_col].nunique())
    else:
        r = 0
    return o, r


def _city_opd_rider_counts(
    day_df: pd.DataFrame,
    city_key: str,
    city_col: str,
    order_col: str,
    rider_col: Optional[str],
    status_col: str,
) -> Tuple[int, int]:
    """OPD (distinct orders 5+6) and distinct riders for one top city or Other — full day, no HR slot split."""
    if day_df.empty:
        return 0, 0
    if city_key == "Other":
        sub = day_df.loc[~day_df[city_col].map(_is_top_tier_city)]
    else:
        sub = day_df.loc[day_df[city_col].map(lambda v, t=city_key: _row_matches_top_city_label(v, t))]
    if sub.empty:
        return 0, 0
    stt = pd.to_numeric(sub[status_col], errors="coerce")
    opd_rows = sub.loc[stt.isin([5, 6])]
    if opd_rows.empty:
        return 0, 0
    o = int(opd_rows[order_col].nunique())
    if rider_col and rider_col in opd_rows.columns:
        r = int(opd_rows[rider_col].nunique())
    else:
        r = 0
    return o, r


def _df_opd_only(dfx: pd.DataFrame, status_col: str) -> pd.DataFrame:
    if dfx.empty or status_col not in dfx.columns:
        return dfx.iloc[0:0].copy()
    stt = pd.to_numeric(dfx[status_col], errors="coerce")
    return dfx.loc[stt.isin([5, 6])].copy()


def _df_reject_only(dfx: pd.DataFrame, status_col: str) -> pd.DataFrame:
    if dfx.empty or status_col not in dfx.columns:
        return dfx.iloc[0:0].copy()
    stt = pd.to_numeric(dfx[status_col], errors="coerce")
    return dfx.loc[stt == 6].copy()


def _rider_city_mode(s: pd.Series) -> str:
    s2 = s.dropna().astype(str)
    if s2.empty:
        return ""
    m = s2.mode()
    return str(m.iloc[0]) if len(m) else str(s2.iloc[0])


def render_uber_ops_dashboard(df: pd.DataFrame) -> None:
    """Ops dashboard: filters, direct city focus, HR slot KPIs (OPD + riders) and chart HR filter."""
    cols = list(df.columns)
    if not cols:
        st.warning("No columns in data.")
        return

    lc = {str(c).lower(): c for c in df.columns}

    def _guess(*names: str) -> Optional[str]:
        for n in names:
            if n in lc:
                return lc[n]
        return None

    city_col = _guess("city_name", "city", "service_city", "pickup_city", "origin_city") or cols[0]
    order_col = _guess("order_id", "trip_id", "delivery_id", "task_id") or cols[min(1, len(cols) - 1)]
    date_col = (
        _guess("order_date", "order_date_local", "order_dt")
        or _guess("creation_time", "created_at", "scheduled_time")
        or cols[0]
    )
    time_col = _guess("allot_time", "pickup_time", "order_date", "created_at", "scheduled_time") or date_col
    creation_col = _guess("creation_time", "created_at", "order_creation_time", "creation_ts")
    creation_is_fallback = False
    if not creation_col:
        creation_col = time_col
        creation_is_fallback = True
    status_col = _guess("current_status", "order_status", "status", "trip_status")
    cluster_col = _guess("cluster_name", "cluster", "zone_cluster", "hub_cluster")
    rider_col = _guess("rider_id", "driver_id", "partner_id", "delivery_partner_id")

    if not status_col or status_col not in cols:
        st.error("Dataset needs a status column such as **current_status** (codes 5 = Delivered, 6 = Rejected).")
        return

    _ops_dark = _ops_theme_is_dark()
    _c_bar = "#f59e0b" if _ops_dark else "#6366f1"
    _c_bar2 = "#38bdf8" if _ops_dark else "#0891b2"
    _c_line = "#f59e0b" if _ops_dark else "#6366f1"
    _c_neg = "#fb7185" if _ops_dark else "#dc2626"

    st.session_state.pop("uber_ops_city_tile", None)

    st.markdown(_OPS_DASHBOARD_CSS, unsafe_allow_html=True)
    st.markdown(
        """
        <div class="ops-dash-shell">
          <div class="ops-dash-title">Operations command center</div>
            <p class="ops-dash-sub">
            <strong>OPD</strong> = distinct <code>order_id</code> (status <strong>5 + 6</strong>); <strong>riders</strong> = distinct <code>rider_id</code> on those rows.
            HR slots use <strong>creation_time</strong>. Analytics (below) adds Top vs Other, HR slot charts, daily trend, rider leaderboards, and reject-rate insight.
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    dfp = df.copy()
    dfp["_fdate"] = pd.to_datetime(dfp[date_col], errors="coerce")
    d_valid = dfp["_fdate"].dropna()
    if d_valid.empty:
        st.warning(f"No parseable dates in **{date_col}** — check your file.")
        return

    d_lo = pd.Timestamp(d_valid.min()).date()
    d_hi = pd.Timestamp(d_valid.max()).date()

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        dr = st.date_input(
            "Order date range",
            value=(d_lo, d_hi),
            key="uber_ops_date_range",
        )
    if isinstance(dr, tuple) and len(dr) == 2:
        d_start, d_end = dr[0], dr[1]
    else:
        d_start = d_end = dr if dr is not None else d_lo

    with f2:
        city_opts = sorted(dfp[city_col].dropna().astype(str).unique().tolist())
        city_pick = st.multiselect(
            "City (empty = all)",
            city_opts,
            default=[],
            key="uber_ops_city_filter",
        )

    with f3:
        if cluster_col:
            cl_opts = sorted(dfp[cluster_col].dropna().astype(str).unique().tolist())
            cluster_pick = st.multiselect(
                "Cluster (empty = all)",
                cl_opts,
                default=[],
                key="uber_ops_cluster_filter",
            )
        else:
            cluster_pick = []
            st.caption("No **cluster_name** — skipped.")

    hr_labels = [lb for _, lb in _OPS_HR_SLOT_DEFS]
    with f4:
        hr_pick_labels = st.multiselect(
            "HR slot (charts only — empty = all day)",
            hr_labels,
            default=[],
            key="uber_ops_hr_slots",
        )
        if creation_is_fallback:
            st.caption("No **creation_time** — HR uses the mapped time column.")
        st.caption("4am–7am outside slots — use empty HR to include.")

    fc1, fc2 = st.columns([1, 2])
    with fc1:
        city_focus_label = st.selectbox(
            "City focus",
            ["All"] + _TOP_CITY_LABELS + ["Other"],
            index=0,
            key="uber_ops_city_focus",
            help="Top-city list + Other (non-top). Applies to slot KPIs and charts.",
        )
    with fc2:
        if rider_col:
            st.caption(f"**Rider** metric uses column `{rider_col}` (distinct on OPD rows).")
        else:
            st.caption("Add a **rider_id** (or driver/partner) column for rider counts.")

    focus_key = "all" if city_focus_label == "All" else city_focus_label

    mask = dfp["_fdate"].notna()
    if d_start is not None:
        mask &= dfp["_fdate"].dt.date >= d_start
    if d_end is not None:
        mask &= dfp["_fdate"].dt.date <= d_end
    df_stage = dfp.loc[mask].copy()
    if city_pick:
        df_stage = df_stage[df_stage[city_col].astype(str).isin(city_pick)]
    if cluster_col and cluster_pick:
        df_stage = df_stage[df_stage[cluster_col].astype(str).isin(cluster_pick)]

    df_stage["_chr"] = pd.to_datetime(df_stage[creation_col], errors="coerce").dt.hour

    st.session_state.setdefault("ops_cf_city", "All")
    st.session_state.setdefault("ops_cf_cluster", "All")
    st.session_state.setdefault("ops_ts_gran", "Daily")

    def _ops_apply_crossfilter(dfx: pd.DataFrame) -> pd.DataFrame:
        if dfx.empty:
            return dfx
        out = dfx.copy()
        cf = st.session_state.get("ops_cf_city", "All")
        if cf != "All":
            if cf == "Other":
                out = out.loc[~out[city_col].map(_is_top_tier_city)]
            else:
                out = out.loc[out[city_col].map(lambda v, t=cf: _row_matches_top_city_label(v, t))]
        ccl = st.session_state.get("ops_cf_cluster", "All")
        if cluster_col and ccl != "All":
            out = out.loc[out[cluster_col].astype(str) == ccl]
        return out

    with st.expander("Command center — cross-filter, trend granularity & targets", expanded=False):
        st.caption(
            "Cross-filter applies to **Analytics & insights** (trend, riders, pie, etc.). "
            "Use it like linked brushing — pick a city (or cluster) to focus without changing sidebar filters."
        )
        cf_a, cf_b = st.columns(2)
        with cf_a:
            _u_cities = sorted(df_stage[city_col].dropna().astype(str).unique().tolist())
            _city_cf_opts = ["All", "Other"] + [c for c in _u_cities if c not in ("All", "Other")]
            st.selectbox("Cross-filter · city", _city_cf_opts, key="ops_cf_city")
        cfv = st.session_state.get("ops_cf_city", "All")
        _tmp_cl = df_stage.copy()
        if cfv != "All":
            if cfv == "Other":
                _tmp_cl = _tmp_cl.loc[~_tmp_cl[city_col].map(_is_top_tier_city)]
            else:
                _tmp_cl = _tmp_cl.loc[
                    _tmp_cl[city_col].map(lambda v, t=cfv: _row_matches_top_city_label(v, t))
                ]
        _cl_opts = ["All"]
        if cluster_col:
            _cl_opts += sorted(_tmp_cl[cluster_col].dropna().astype(str).unique().tolist())
        if cluster_col and st.session_state.get("ops_cf_cluster", "All") not in _cl_opts:
            st.session_state.ops_cf_cluster = "All"
        with cf_b:
            if cluster_col:
                st.selectbox("Cross-filter · cluster", _cl_opts, key="ops_cf_cluster")
            else:
                st.caption("No **cluster** column — cluster drill-down skipped.")
        gr1, gr2, gr3 = st.columns(3)
        with gr1:
            st.radio(
                "OPD trend · time bucket",
                ["Daily", "Weekly", "Hourly"],
                horizontal=True,
                key="ops_ts_gran",
            )
        with gr2:
            st.number_input(
                "Target OPD (max day · highlights tile if below)",
                min_value=0,
                value=0,
                step=1,
                key="ops_target_opd",
            )
        with gr3:
            st.slider(
                "Reject-rate alert threshold %",
                0.0,
                100.0,
                25.0,
                key="ops_max_reject_pct",
            )

    df_ana = _ops_apply_crossfilter(df_stage)

    _vds = df_stage["_fdate"].dropna()
    if _vds.empty:
        max_d_stage = None
    else:
        max_d_stage = pd.Timestamp(_vds.max()).normalize().date()
    prev_d_stage = max_d_stage - timedelta(days=1) if max_d_stage else None
    day_t_stage = df_stage[df_stage["_fdate"].dt.date == max_d_stage] if max_d_stage else pd.DataFrame()
    day_p_stage = df_stage[df_stage["_fdate"].dt.date == prev_d_stage] if prev_d_stage else pd.DataFrame()
    day_t_ana = _ops_apply_crossfilter(day_t_stage) if max_d_stage else pd.DataFrame()
    day_p_ana = _ops_apply_crossfilter(day_p_stage) if prev_d_stage else pd.DataFrame()

    def _filter_by_city_focus(dfx: pd.DataFrame, tile: str) -> pd.DataFrame:
        if tile == "all":
            return dfx.copy()
        if tile == "Other":
            return dfx.loc[~dfx[city_col].map(_is_top_tier_city)].copy()
        return dfx.loc[dfx[city_col].map(lambda v, t=tile: _row_matches_top_city_label(v, t))].copy()

    df_kpi = _filter_by_city_focus(df_stage, focus_key)

    df_work = df_kpi.copy()
    if hr_pick_labels:
        hr_keys = [_OPS_HR_LABEL_TO_KEY[x] for x in hr_pick_labels if x in _OPS_HR_LABEL_TO_KEY]
        if hr_keys:
            slot_m = pd.Series(False, index=df_work.index)
            for sk in hr_keys:
                slot_m |= df_work["_chr"].map(lambda h, s=sk: _ops_hr_hour_in_slot(h, s))
            df_work = df_work.loc[slot_m].copy()

    vd = df_kpi["_fdate"].dropna()
    if vd.empty:
        max_d = None
    else:
        max_d = pd.Timestamp(vd.max()).normalize().date()
    prev_d = max_d - timedelta(days=1) if max_d else None

    day_t = df_kpi[df_kpi["_fdate"].dt.date == max_d] if max_d is not None else pd.DataFrame()
    day_p = df_kpi[df_kpi["_fdate"].dt.date == prev_d] if prev_d is not None else pd.DataFrame()

    def _delta_html(pct: Optional[float], arrow: str) -> str:
        if pct is None and arrow == "↑":
            return '<div class="ops-kpi-delta-up">↑ new vs D-1 (no prior day)</div>'
        if pct is None:
            return '<div class="ops-kpi-delta-flat">→ — vs D-1</div>'
        cls = "ops-kpi-delta-up" if pct > 0 else ("ops-kpi-delta-down" if pct < 0 else "ops-kpi-delta-flat")
        sign = "+" if pct > 0 else ""
        return f'<div class="{cls}">{arrow} {sign}{pct:.1f}% vs D-1</div>'

    def _rider_delta_html(pct: Optional[float], arrow: str) -> str:
        if pct is None and arrow == "↑":
            return '<div class="ops-kpi-rider-delta ops-kpi-delta-up">Riders ↑ new vs D-1</div>'
        if pct is None:
            return '<div class="ops-kpi-rider-delta ops-kpi-delta-flat">Riders → — vs D-1</div>'
        cls = "ops-kpi-delta-up" if pct > 0 else ("ops-kpi-delta-down" if pct < 0 else "ops-kpi-delta-flat")
        sign = "+" if pct > 0 else ""
        return f'<div class="ops-kpi-rider-delta {cls}">Riders {arrow} {sign}{pct:.1f}% vs D-1</div>'

    st.markdown("**Max date · day totals**")
    st.caption(
        f"Per top city / Other — max date **{max_d_stage}** vs **{prev_d_stage}** (sidebar filters). "
        "Totals below follow **Command center** cross-filter when set."
        if max_d_stage
        else "No max date in range."
    )
    tot_opd_col, tot_rid_col = st.columns(2)
    if max_d_stage is not None and len(day_t_stage) > 0 and len(day_t_ana) == 0:
        st.warning(
            "Cross-filter excludes all rows on the latest date — set **Command center → city/cluster** to **All** "
            "or widen sidebar filters."
        )
    if max_d_stage is not None and len(day_t_ana) > 0:
        _od_t = _df_opd_only(day_t_ana, status_col)
        _od_p = _df_opd_only(day_p_ana, status_col) if len(day_p_ana) else _od_t.iloc[0:0].copy()
        _tot_opd = int(_od_t[order_col].nunique())
        _tot_opd_p = int(_od_p[order_col].nunique()) if len(_od_p) else 0
        po, ao = _pct_vs_d1(_tot_opd, _tot_opd_p)
        ord_html = _delta_html(po, ao)
        if rider_col and rider_col in _od_t.columns:
            _tot_rid = int(_od_t[rider_col].nunique())
            _tot_rid_p = int(_od_p[rider_col].nunique()) if len(_od_p) and rider_col in _od_p.columns else 0
            pr, ar = _pct_vs_d1(_tot_rid, _tot_rid_p)
            rid_html = _rider_delta_html(pr, ar)
            rider_line = f"{_tot_rid:,}"
        else:
            rid_html = '<div class="ops-kpi-rider-delta ops-kpi-delta-flat">Riders → — vs D-1</div>'
            rider_line = "No"
        _tgt = int(st.session_state.get("ops_target_opd", 0) or 0)
        _max_rej = float(st.session_state.get("ops_max_reject_pct", 100.0) or 100.0)
        _rr_pct = 0.0
        try:
            _sdf = day_t_ana.copy()
            _sdf["_st"] = pd.to_numeric(_sdf[status_col], errors="coerce")
            _all_d = int(_sdf[order_col].nunique())
            _rej_d = int(_sdf.loc[_sdf["_st"] == 6, order_col].nunique()) if _all_d else 0
            _rr_pct = (100.0 * _rej_d / _all_d) if _all_d else 0.0
        except Exception:
            pass
        _kpi_cls = "ops-kpi-card"
        if (_tgt > 0 and _tot_opd < _tgt) or (_rr_pct > _max_rej):
            _kpi_cls += " ops-kpi-alert"
        opd_card = f"""
                <div class="{_kpi_cls}">
                  <div class="ops-kpi-label">TOTAL · OPD</div>
                  <div class="ops-kpi-value">{_tot_opd:,}</div>
                  <div class="ops-kpi-meta">OPD · distinct orders</div>
                  {ord_html}
                  <div class="ops-kpi-meta">Max date · command center scope · reject { _rr_pct:.1f}% vs threshold {_max_rej:.0f}%</div>
                </div>
                """
        rid_card = f"""
                <div class="ops-kpi-card">
                  <div class="ops-kpi-label">TOTAL · RIDERS</div>
                  <div class="ops-kpi-value">{rider_line}</div>
                  <div class="ops-kpi-meta">Distinct rider_id · OPD rows (5+6)</div>
                  {rid_html}
                  <div class="ops-kpi-meta">Max date · day total scope</div>
                </div>
                """
        with tot_opd_col:
            st.markdown(opd_card, unsafe_allow_html=True)
        with tot_rid_col:
            st.markdown(rid_card, unsafe_allow_html=True)
    else:
        empty_opd = """
                <div class="ops-kpi-card">
                  <div class="ops-kpi-label">TOTAL · OPD</div>
                  <div class="ops-kpi-value">No</div>
                  <div class="ops-kpi-meta">OPD · distinct orders</div>
                  <div class="ops-kpi-delta-flat">→ — vs D-1</div>
                  <div class="ops-kpi-meta">Max date · day total scope</div>
                </div>
                """
        empty_rid = """
                <div class="ops-kpi-card">
                  <div class="ops-kpi-label">TOTAL · RIDERS</div>
                  <div class="ops-kpi-value">No</div>
                  <div class="ops-kpi-meta">Distinct rider_id · OPD rows (5+6)</div>
                  <div class="ops-kpi-rider-delta ops-kpi-delta-flat">Riders → — vs D-1</div>
                  <div class="ops-kpi-meta">Max date · day total scope</div>
                </div>
                """
        with tot_opd_col:
            st.markdown(empty_opd, unsafe_allow_html=True)
        with tot_rid_col:
            st.markdown(empty_rid, unsafe_allow_html=True)
        if max_d_stage is None or len(day_t_stage) == 0:
            st.warning("No rows on the latest date in this date range — widen filters or pick **All** cities.")

    st.markdown("**Top city KPIs · OPD & riders (status 5+6) vs D-1**")
    st.caption(
        f"Per top city / Other — max date **{max_d_stage}** vs **{prev_d_stage}** (uses date · city · cluster filters; ignores City focus dropdown)."
        if max_d_stage
        else "No dates for top-city tiles."
    )
    city_labels_tc = list(_TOP_CITY_LABELS) + ["Other"]
    _city_ncols = 5
    for row_i in range(0, len(city_labels_tc), _city_ncols):
        tcrow = st.columns(_city_ncols)
        for j in range(_city_ncols):
            idx = row_i + j
            with tcrow[j]:
                if idx >= len(city_labels_tc):
                    continue
                clab = city_labels_tc[idx]
                ot, rt = _city_opd_rider_counts(
                    day_t_stage, clab, city_col, order_col, rider_col, status_col
                )
                op, rp = _city_opd_rider_counts(
                    day_p_stage, clab, city_col, order_col, rider_col, status_col
                )
                po, ao = _pct_vs_d1(ot, op)
                pr, ar = _pct_vs_d1(rt, rp)
                ord_html = _delta_html(po, ao)
                rid_html = _rider_delta_html(pr, ar) if rider_col else (
                    '<div class="ops-kpi-rider-delta ops-kpi-delta-flat">Riders —</div>'
                )
                rider_line = f"{rt:,}" if rider_col else "—"
                active = "active" if city_focus_label == clab else ""
                safe = html.escape(clab)
                st.markdown(
                    f"""
                    <div class="ops-kpi-card {active}">
                      <div class="ops-kpi-label">{safe} · OPD & riders</div>
                      <div class="ops-kpi-value">{ot:,}</div>
                      <div class="ops-kpi-meta">OPD · distinct orders</div>
                      {ord_html}
                      <div class="ops-kpi-rider">Riders · {rider_line}</div>
                      {rid_html}
                      <div class="ops-kpi-meta">Max date · top-city scope</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    meta = (
        f"Slot row: “today” = **{max_d}** · D-1 = **{prev_d}** · {len(df_kpi):,} rows in City focus"
        if max_d
        else "No dates in range"
    )

    st.markdown("**Slot KPIs · OPD & riders (status 5+6) vs D-1**")
    slot_cols = st.columns(5)
    for i, (sk, lab) in enumerate(_OPS_HR_SLOT_DEFS):
        ot, rt = _slot_opd_rider_counts(day_t, sk, order_col, rider_col, status_col)
        op, rp = _slot_opd_rider_counts(day_p, sk, order_col, rider_col, status_col)
        po, ao = _pct_vs_d1(ot, op)
        pr, ar = _pct_vs_d1(rt, rp)
        ord_html = _delta_html(po, ao)
        rid_html = _rider_delta_html(pr, ar) if rider_col else '<div class="ops-kpi-rider-delta ops-kpi-delta-flat">Riders —</div>'
        rider_line = f"{rt:,}" if rider_col else "—"
        with slot_cols[i]:
            st.markdown(
                f"""
                <div class="ops-kpi-card">
                  <div class="ops-kpi-label">{lab}</div>
                  <div class="ops-kpi-value">{ot:,}</div>
                  <div class="ops-kpi-meta">OPD · distinct orders</div>
                  {ord_html}
                  <div class="ops-kpi-rider">Riders · {rider_line}</div>
                  {rid_html}
                  <div class="ops-kpi-meta">Max date · same OPD rules</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.caption(meta)
    if focus_key != "all":
        st.info(f"City focus **{city_focus_label}** — charts use HR slot filter when set.")

    st.divider()
    st.markdown("### Analytics & insights")
    st.caption(
        "This block respects **Command center** cross-filter (city/cluster) on top of sidebar filters. "
        "Max-day charts use **max date in range**; rider tables use the **full selected date range**."
    )

    opd_range = _df_opd_only(df_ana, status_col)

    with st.expander("Drill-down · OPD status mix (distinct orders in selected range)", expanded=False):
        dfb = df_ana.copy()
        if dfb.empty:
            st.caption("No rows after filters.")
        else:
            dfb["_st"] = pd.to_numeric(dfb[status_col], errors="coerce")
            s5 = int(dfb.loc[dfb["_st"] == 5, order_col].nunique())
            s6 = int(dfb.loc[dfb["_st"] == 6, order_col].nunique())
            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("Status 5 (OPD-eligible)", f"{s5:,}")
            with m2:
                st.metric("Status 6 (customer reject)", f"{s6:,}")
            with m3:
                tot_st = s5 + s6
                st.metric("Share status 6", f"{(100.0 * s6 / tot_st):.1f}%" if tot_st else "—")
            if st.button("Ask AI · status mix", key="ai_ops_status_mix"):
                st.session_state.ops_ai_suggested_prompt = (
                    f"Explain the OPD vs reject mix (status 5={s5:,}, status 6={s6:,}) for the current dashboard filters."
                )
                st.rerun()

    if not df_ana.empty and max_d_stage is not None and len(day_t_ana) > 0:
        try:
            m_top = day_t_ana[city_col].map(_is_top_tier_city)
            od_top = _df_opd_only(day_t_ana.loc[m_top], status_col)
            od_other = _df_opd_only(day_t_ana.loc[~m_top], status_col)
            opd_top_n = int(od_top[order_col].nunique())
            opd_other_n = int(od_other[order_col].nunique())
            rid_top_n = (
                int(od_top[rider_col].nunique())
                if rider_col and rider_col in od_top.columns
                else 0
            )
            rid_other_n = (
                int(od_other[rider_col].nunique())
                if rider_col and rider_col in od_other.columns
                else 0
            )
            comp = pd.DataFrame(
                {
                    "segment": ["Top cities (combined)", "Other"],
                    "OPD": [opd_top_n, opd_other_n],
                    "Riders": [rid_top_n, rid_other_n],
                }
            )
            g1, g2 = st.columns(2)
            with g1:
                fig_top_opd = px.bar(
                    comp,
                    x="segment",
                    y="OPD",
                    title=f"OPD (distinct orders) · max day {max_d_stage} · Top vs Other",
                    color_discrete_sequence=[_c_bar],
                )
                _ops_apply_fig_theme(fig_top_opd, is_dark=_ops_dark)
                st.plotly_chart(fig_top_opd, use_container_width=True)
            with g2:
                fig_top_rid = px.bar(
                    comp,
                    x="segment",
                    y="Riders",
                    title=f"Unique riders (OPD rows) · max day {max_d_stage} · Top vs Other",
                    color_discrete_sequence=[_c_bar2],
                )
                _ops_apply_fig_theme(fig_top_rid, is_dark=_ops_dark)
                st.plotly_chart(fig_top_rid, use_container_width=True)
            if st.button("Ask AI · Top vs Other", key="ai_ops_top_other"):
                st.session_state.ops_ai_suggested_prompt = (
                    f"Interpret Top vs Other OPD on max day {max_d_stage}: "
                    f"Top combined={opd_top_n:,}, Other={opd_other_n:,} (command center filters applied)."
                )
                st.rerun()
        except Exception as ex:
            st.caption(f"Top vs Other chart: {ex}")

        try:
            slot_rows = []
            for sk, lab in _OPS_HR_SLOT_DEFS:
                o, r = _slot_opd_rider_counts(day_t_ana, sk, order_col, rider_col, status_col)
                slot_rows.append({"Slot": lab, "OPD": o, "Riders": r})
            slot_df = pd.DataFrame(slot_rows)
            slot_long = slot_df.melt(id_vars=["Slot"], var_name="Metric", value_name="Count")
            _slot_colors = (
                {"OPD": "#f59e0b", "Riders": "#38bdf8"}
                if _ops_dark
                else {"OPD": "#6366f1", "Riders": "#0891b2"}
            )
            fig_slot = px.bar(
                slot_long,
                x="Slot",
                y="Count",
                color="Metric",
                barmode="group",
                title=f"HR slot · OPD & riders · max day {max_d_stage}",
                color_discrete_map=_slot_colors,
            )
            _ops_apply_fig_theme(
                fig_slot,
                is_dark=_ops_dark,
                margin=dict(t=48, b=80),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_slot, use_container_width=True)
        except Exception as ex:
            st.caption(f"Slot chart: {ex}")

        try:
            slot_rows2 = []
            for sk, lab in _OPS_HR_SLOT_DEFS:
                o, r = _slot_opd_rider_counts(day_t_ana, sk, order_col, rider_col, status_col)
                slot_rows2.append({"HR_slot": lab, "OPD": o, "unique_riders": r})
            _sdf_slot = pd.DataFrame(slot_rows2)
            st.dataframe(_sdf_slot, use_container_width=True, hide_index=True)
            st.download_button(
                "Download slot table (CSV)",
                _sdf_slot.to_csv(index=False).encode("utf-8"),
                file_name="ops_hr_slot_max_day.csv",
                mime="text/csv",
                key="dl_ops_slot_tbl",
            )
        except Exception as ex:
            st.caption(f"Slot table: {ex}")
    else:
        st.info("No max-day data for Top vs Other / slot charts — check date range or command center filters.")

    if not opd_range.empty:
        try:
            _od = opd_range.copy()
            _od["_ts"] = pd.to_datetime(_od["_fdate"], errors="coerce")
            _od = _od[_od["_ts"].notna()]
            gran = st.session_state.get("ops_ts_gran", "Daily")
            if gran == "Daily":
                daily = _od.groupby(_od["_ts"].dt.floor("D"))[order_col].nunique().reset_index()
                daily.columns = ["bucket", "OPD"]
                tit = "OPD trend · daily buckets (distinct orders, status 5+6)"
            elif gran == "Weekly":
                daily = (
                    _od.groupby(pd.Grouper(key="_ts", freq="W-MON"))[order_col].nunique().reset_index()
                )
                daily = daily.dropna(subset=["_ts"])
                daily.columns = ["bucket", "OPD"]
                tit = "OPD trend · weekly buckets (week starting Monday)"
            else:
                daily = _od.groupby(_od["_ts"].dt.hour)[order_col].nunique().reset_index()
                daily.columns = ["bucket", "OPD"]
                tit = "OPD trend · hourly buckets (order timestamp hour, aggregated across range)"
            fig_daily = px.line(
                daily,
                x="bucket",
                y="OPD",
                markers=True,
                title=tit,
                color_discrete_sequence=[_c_line],
            )
            _ops_apply_fig_theme(fig_daily, is_dark=_ops_dark)
            tr1, tr2 = st.columns([5, 1])
            with tr1:
                st.plotly_chart(fig_daily, use_container_width=True)
            with tr2:
                st.download_button(
                    "CSV",
                    daily.to_csv(index=False).encode("utf-8"),
                    file_name=f"opd_trend_{gran.lower()}.csv",
                    mime="text/csv",
                    key="dl_ops_trend",
                )
                if st.button("Ask AI", key="ai_ops_trend"):
                    st.session_state.ops_ai_suggested_prompt = (
                        f"Explain this {gran} OPD trend. Top buckets by volume: "
                        f"{daily.sort_values('OPD', ascending=False).head(5).to_string(index=False)}"
                    )
                    st.rerun()
        except Exception as ex:
            st.caption(f"OPD trend: {ex}")

        try:
            if rider_col:
                top10 = (
                    opd_range.groupby(rider_col, dropna=False)
                    .agg(
                        city_name=(city_col, _rider_city_mode),
                        OPD_orders=(order_col, "nunique"),
                    )
                    .reset_index()
                    .rename(columns={rider_col: "rider_id"})
                    .sort_values("OPD_orders", ascending=False)
                    .head(10)
                )
                st.markdown("**Top 10 riders by OPD volume** (full date range · command center scope)")
                st.dataframe(top10, use_container_width=True, hide_index=True)
                z1, z2 = st.columns(2)
                with z1:
                    st.download_button(
                        "Download Top 10 (CSV)",
                        top10.to_csv(index=False).encode("utf-8"),
                        file_name="top10_riders_opd.csv",
                        mime="text/csv",
                        key="dl_top10_opd",
                    )
                with z2:
                    if st.button("Ask AI · Top 10", key="ai_top10"):
                        st.session_state.ops_ai_suggested_prompt = (
                            "Review the Top 10 riders by OPD volume for the current filters. "
                            "Call out concentration risk and any odd patterns."
                        )
                        st.rerun()

                bot10 = (
                    opd_range.groupby(rider_col, dropna=False)
                    .agg(
                        city_name=(city_col, _rider_city_mode),
                        OPD_orders=(order_col, "nunique"),
                    )
                    .reset_index()
                    .rename(columns={rider_col: "rider_id"})
                    .query("OPD_orders >= 1")
                    .sort_values("OPD_orders", ascending=True)
                    .head(10)
                )
                st.markdown("**Bottom 10 riders by OPD** (min 1 OPD order)")
                st.dataframe(bot10, use_container_width=True, hide_index=True)
                st.download_button(
                    "Download Bottom 10 (CSV)",
                    bot10.to_csv(index=False).encode("utf-8"),
                    file_name="bottom10_riders_opd.csv",
                    mime="text/csv",
                    key="dl_bot10_opd",
                )

                rej = _df_reject_only(df_ana, status_col)
                if not rej.empty:
                    top_rej = (
                        rej.groupby(rider_col, dropna=False)
                        .agg(
                            city_name=(city_col, _rider_city_mode),
                            reject_orders=(order_col, "nunique"),
                        )
                        .reset_index()
                        .rename(columns={rider_col: "rider_id"})
                        .sort_values("reject_orders", ascending=False)
                        .head(10)
                    )
                    st.markdown("**Top 10 riders by customer rejections** (status 6, distinct orders)")
                    st.dataframe(top_rej, use_container_width=True, hide_index=True)
                    st.download_button(
                        "Download reject leaders (CSV)",
                        top_rej.to_csv(index=False).encode("utf-8"),
                        file_name="top10_riders_rejects.csv",
                        mime="text/csv",
                        key="dl_top_rej",
                    )
        except Exception as ex:
            st.caption(f"Rider tables: {ex}")

        try:
            if max_d_stage is not None and len(day_t_ana) > 0:
                st.markdown("**OPD share · top cities (max day)**")
                city_opd = []
                for clab in _TOP_CITY_LABELS:
                    o, _ = _city_opd_rider_counts(
                        day_t_ana, clab, city_col, order_col, rider_col, status_col
                    )
                    city_opd.append({"city": clab, "OPD": o})
                o_other, _ = _city_opd_rider_counts(
                    day_t_ana, "Other", city_col, order_col, rider_col, status_col
                )
                city_opd.append({"city": "Other", "OPD": o_other})
                pie_df = pd.DataFrame(city_opd)
                pie_df = pie_df[pie_df["OPD"] > 0]
                if len(pie_df) > 0:
                    fig_pie = px.pie(
                        pie_df,
                        names="city",
                        values="OPD",
                        title=f"OPD mix by city · {max_d_stage}",
                        hole=0.35,
                    )
                    _ops_apply_fig_theme(fig_pie, is_dark=_ops_dark, margin=dict(t=48, b=20))
                    try:
                        st.plotly_chart(
                            fig_pie,
                            use_container_width=True,
                            on_select="rerun",
                            key="ops_pie_sel",
                            selection_mode="points",
                        )
                    except Exception:
                        st.plotly_chart(fig_pie, use_container_width=True)
                    st.caption("Tip: use **Command center → city** to mirror pie-slice filtering (linked analytics).")
                    if st.button("Ask AI · city mix", key="ai_ops_pie"):
                        st.session_state.ops_ai_suggested_prompt = (
                            f"Explain the OPD city mix on {max_d_stage}. Data: {pie_df.to_string(index=False)}"
                        )
                        st.rerun()
        except Exception as ex:
            st.caption(f"Pie chart: {ex}")

        try:
            st.markdown("**Reject rate (orders)** — by day in range")
            dfp2 = df_ana.copy()
            dfp2["_st"] = pd.to_numeric(dfp2[status_col], errors="coerce")
            dfp2 = dfp2[dfp2["_st"].notna()]
            dfp2["_day"] = pd.to_datetime(dfp2["_fdate"]).dt.date
            daily_rej = dfp2.loc[dfp2["_st"] == 6].groupby("_day")[order_col].nunique().rename("rejects")
            daily_all = dfp2.groupby("_day")[order_col].nunique().rename("all_orders")
            rr = pd.concat([daily_all, daily_rej], axis=1).fillna(0)
            rr["reject_rate_pct"] = (rr["rejects"] / rr["all_orders"].replace(0, pd.NA)) * 100.0
            rr = rr.reset_index()
            fig_rr = px.line(
                rr,
                x="_day",
                y="reject_rate_pct",
                markers=True,
                title="Daily reject rate % (status 6 / all orders)",
                color_discrete_sequence=[_c_neg],
            )
            _ops_apply_fig_theme(fig_rr, is_dark=_ops_dark)
            st.plotly_chart(fig_rr, use_container_width=True)
        except Exception as ex:
            st.caption(f"Reject rate trend: {ex}")

        try:
            if rider_col:
                rp = (
                    opd_range.groupby(rider_col, dropna=False)[order_col]
                    .nunique()
                    .reset_index(name="orders_per_rider")
                )
                fig_hist = px.histogram(
                    rp,
                    x="orders_per_rider",
                    nbins=min(40, max(5, int(rp["orders_per_rider"].max() or 5))),
                    title="Distribution of OPD orders per rider (range)",
                    color_discrete_sequence=[_c_bar],
                )
                _ops_apply_fig_theme(fig_hist, is_dark=_ops_dark)
                st.plotly_chart(fig_hist, use_container_width=True)
        except Exception as ex:
            st.caption(f"Histogram: {ex}")
    else:
        st.info("No OPD rows in selected date range — no daily / rider analytics.")

    st.markdown("#### Detail charts")
    try:
        by_city = df_work.groupby(city_col, dropna=False)[order_col].nunique().reset_index()
        by_city.columns = ["zone", "distinct_orders"]
        by_city = by_city.sort_values("distinct_orders", ascending=False).head(40)
        chart_title = "Distinct orders by city (top 40)"
        if focus_key != "all":
            chart_title += f" · focus: {city_focus_label}"
        fig1 = px.bar(
            by_city,
            x="zone",
            y="distinct_orders",
            title=chart_title,
            color_discrete_sequence=[_c_bar],
        )
        _ops_apply_fig_theme(fig1, is_dark=_ops_dark)
        st.plotly_chart(fig1, use_container_width=True)
        cf_city = st.session_state.get("ops_cf_city", "All")
        if cluster_col and cf_city not in ("All", "Other") and not df_ana.empty:
            try:
                by_cl = df_ana.groupby(cluster_col, dropna=False)[order_col].nunique().reset_index()
                by_cl.columns = ["cluster", "distinct_orders"]
                by_cl = by_cl.sort_values("distinct_orders", ascending=False).head(40)
                if len(by_cl) > 0:
                    fig_cl = px.bar(
                        by_cl,
                        x="cluster",
                        y="distinct_orders",
                        title=f"Clusters in {cf_city} · distinct orders (command center scope)",
                        color_discrete_sequence=[_c_bar],
                    )
                    _ops_apply_fig_theme(fig_cl, is_dark=_ops_dark)
                    st.plotly_chart(fig_cl, use_container_width=True)
                    st.caption("Shows when **Command center** filters to a single city — city → cluster view.")
            except Exception as ex:
                st.caption(f"Cluster drill-down: {ex}")
    except Exception as e:
        st.warning(f"City chart: {e}")

    h1, h2 = st.columns(2)
    with h1:
        try:
            dfx = df_work.copy()
            dfx["_ts"] = pd.to_datetime(dfx[time_col], errors="coerce")
            dfx = dfx[dfx["_ts"].notna()]
            if len(dfx) > 0:
                dfx["_hour"] = dfx["_ts"].dt.hour
                hourly = dfx.groupby("_hour")[order_col].nunique().reset_index()
                fig2 = px.line(
                    hourly,
                    x="_hour",
                    y=order_col,
                    markers=True,
                    title="Distinct orders by hour",
                    color_discrete_sequence=[_c_bar2],
                )
                _ops_apply_fig_theme(fig2, is_dark=_ops_dark)
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("No parseable timestamps for the hourly chart.")
        except Exception as ex:
            st.caption(f"Hourly chart skipped: {ex}")

    with h2:
        st.markdown("**SQL hint** (`sql:` — table `data`):")
        st.code(
            f"SELECT {city_col}, COUNT(DISTINCT {order_col}) AS orders FROM data GROUP BY {city_col} ORDER BY orders DESC;",
            language="sql",
        )


def render_dashboard_ai_panel(
    work_df: pd.DataFrame,
    *,
    ai_provider: str,
    gemini_api_key: str,
    gemini_model_name: str,
    ollama_base_url: str,
    ollama_model: str,
) -> None:
    """
    AI + sql: embedded in the Dashboard. Table is always DuckDB `data`; columns come from the loaded
    DataFrame (any schema — order-level Uber-style columns or a future dataset). Swap or extend the
    dashboard charts later; this block stays the same as long as `work_df` is the active table.
    """
    st.divider()
    st.markdown("### AI assistant · interactive")
    st.caption(
        "Plain-English questions or **`sql:`** (DuckDB on table **`data`**). "
        "Uses whatever columns your data has — no fixed schema in code."
    )
    sug = st.session_state.get("ops_ai_suggested_prompt")
    if sug:
        st.info("**Chart suggestion** — paste into the chat input below, or edit before sending.")
        st.code(sug, language=None)
        if st.button("Dismiss suggestion", key="dismiss_ops_ai_sug"):
            st.session_state.ops_ai_suggested_prompt = None
            st.rerun()

    c1, c2 = st.columns([1, 4])
    with c1:
        if st.button("Clear dashboard chat", key="btn_clear_dash_chat"):
            st.session_state.dashboard_messages = []
            st.rerun()
    with c2:
        st.caption("Tip: `sql: SELECT city_name, COUNT(*) FROM data GROUP BY 1` — adjust column names to your file.")

    _uo = st.session_state.get(_k_upload_orig("Dashboard"))
    base_for_sql = _uo if _uo is not None else work_df.copy()

    for m in st.session_state.dashboard_messages:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])
            if m.get("df_preview") is not None:
                st.dataframe(m["df_preview"], use_container_width=True)

    ut = st.chat_input(
        "Ask about this data… or `sql: SELECT … FROM data …`",
        key="dashboard_chat_input",
    )
    if not ut:
        return

    st.session_state.dashboard_messages.append({"role": "user", "content": ut})
    assistant_content = ""
    df_preview = None
    sql_used = None
    rerun_after_sql = False

    with st.chat_message("assistant"):
        try:
            if ai_provider == "Gemini API" and not gemini_api_key:
                raise ValueError("Add a Gemini API key in the sidebar (or use Local Ollama).")
            if ut.strip().lower().startswith("sql:"):
                sql_used = ut.split(":", 1)[1].strip()
                with st.spinner("Running SQL on your data…"):
                    result_df = run_sql_on_dataframe(base_for_sql, sql_used)
                # SQL results are preview-only — base data is never replaced
                assistant_content = f"SQL returned **{len(result_df):,}** rows (preview below — your base data is unchanged)."
                st.code(sql_used, language="sql")
                df_preview = result_df.head(500)
                st.dataframe(df_preview, use_container_width=True)
            else:
                sample_cols = ", ".join([str(c) for c in work_df.columns])
                sample_preview = work_df.head(200).to_string(index=False)
                stats_preview = work_df.describe(include="all").fillna("").to_string()
                sample_prompt = textwrap.dedent(
                    f"""
                    You are a data analyst. Answer ONLY from this dataset.
                    If the user wants a pivot, aggregation, or GROUP BY summary, reply with ONE line:
                    sql: SELECT ... FROM data ...;
                    Table name is `data` (columns: {sample_cols}).

                    Columns:
                    {sample_cols}

                    Summary stats:
                    {stats_preview}

                    Sample rows:
                    {sample_preview}

                    User question:
                    {ut}
                    """
                ).strip()
                with st.spinner("Analyzing…"):
                    assistant_content = call_ai_text_unified(
                        sample_prompt,
                        ai_provider,
                        gemini_api_key,
                        gemini_model_name,
                        ollama_base_url,
                        ollama_model,
                    )
                st.markdown(assistant_content or "OK.")
                extracted_sql = extract_sql_from_text(assistant_content)
                if extracted_sql:
                    try:
                        with st.spinner("Running SQL from AI reply…"):
                            result_df = run_sql_on_dataframe(base_for_sql, extracted_sql)
                        # Preview only — base data unchanged
                        sql_used = extracted_sql
                        assistant_content = (
                            (assistant_content or "")
                            + f"\n\n**Executed SQL** → **{len(result_df):,}** rows (preview, base data unchanged)."
                        )
                        df_preview = result_df.head(500)
                        st.success(f"Executed query: **{len(result_df):,}** rows.")
                        st.code(extracted_sql, language="sql")
                        st.dataframe(df_preview, use_container_width=True)
                    except Exception as sql_err:
                        st.warning(f"Could not run SQL from reply: {sql_err}")
                        df_preview = work_df.head(300)
                        st.dataframe(df_preview, use_container_width=True)
                else:
                    df_preview = work_df.head(300)
                    st.dataframe(df_preview, use_container_width=True)
        except Exception as e:
            st.error(str(e))
            assistant_content = f"Error: {e}"

    st.session_state.dashboard_messages.append(
        {
            "role": "assistant",
            "content": assistant_content or "OK.",
            "sql": sql_used,
            "df_preview": df_preview,
        }
    )
    if rerun_after_sql:
        st.rerun()


# --- Per-tab data (Dashboard / PNL / AI Chat): uploads & loads apply to the active tab only ---
_TAB_SUF = {"Dashboard": "dash", "PNL": "pnl", "AI Chat": "chat"}


def _tab_suf(view: str) -> str:
    return _TAB_SUF.get(view, "dash")


def _k_last(view: str) -> str:
    return f"last_df_{_tab_suf(view)}"


def _k_upload_orig(view: str) -> str:
    return f"upload_df_original_{_tab_suf(view)}"


def _k_sample(view: str) -> str:
    return f"sample_df_{_tab_suf(view)}"


def _k_sample_raw(view: str) -> str:
    return f"sample_df_raw_{_tab_suf(view)}"


def _k_file_label(view: str) -> str:
    return f"file_source_label_{_tab_suf(view)}"


def _k_other_sql(view: str) -> str:
    return f"other_sql_df_{_tab_suf(view)}"


def _migrate_legacy_shared_dfs() -> None:
    """Copy old single last_df / upload / sample keys into per-tab keys (one-time)."""
    if st.session_state.get("_per_tab_dfs_migrated_v1"):
        return
    leg = st.session_state.get("last_df")
    if isinstance(leg, pd.DataFrame) and not leg.empty:
        if st.session_state.get(_k_last("Dashboard")) is None:
            st.session_state[_k_last("Dashboard")] = leg.copy()
        if st.session_state.get(_k_last("AI Chat")) is None:
            st.session_state[_k_last("AI Chat")] = leg.copy()
    leg_u = st.session_state.get("upload_df_original")
    if isinstance(leg_u, pd.DataFrame) and not leg_u.empty:
        if st.session_state.get(_k_upload_orig("Dashboard")) is None:
            st.session_state[_k_upload_orig("Dashboard")] = leg_u.copy()
        if st.session_state.get(_k_upload_orig("AI Chat")) is None:
            st.session_state[_k_upload_orig("AI Chat")] = leg_u.copy()
    s = st.session_state.get("sample_df")
    if isinstance(s, pd.DataFrame) and not s.empty:
        if st.session_state.get(_k_sample("Dashboard")) is None:
            st.session_state[_k_sample("Dashboard")] = s.copy()
        if st.session_state.get(_k_sample("AI Chat")) is None:
            st.session_state[_k_sample("AI Chat")] = s.copy()
    sr = st.session_state.get("sample_df_raw")
    if isinstance(sr, pd.DataFrame) and not sr.empty:
        if st.session_state.get(_k_sample_raw("Dashboard")) is None:
            st.session_state[_k_sample_raw("Dashboard")] = sr.copy()
        if st.session_state.get(_k_sample_raw("AI Chat")) is None:
            st.session_state[_k_sample_raw("AI Chat")] = sr.copy()
    st.session_state._per_tab_dfs_migrated_v1 = True


def _resolve_tab_work_df(
    db_type: str,
    other_sql_data: Optional[pd.DataFrame],
    view: str,
) -> Optional[pd.DataFrame]:
    """Active DataFrame for one tab — **Select Database Type** is shared; stored data is per tab."""
    uber_df: Optional[pd.DataFrame] = None
    if db_type == "Other SQL (Postgres/MySQL)":
        if other_sql_data is not None and isinstance(other_sql_data, pd.DataFrame) and not other_sql_data.empty:
            uber_df = other_sql_data
    else:
        uber_df = st.session_state.get(_k_last(view))
        if (uber_df is None or uber_df.empty) and db_type == "BigQuery":
            s = st.session_state.get(_k_sample(view))
            if s is not None and isinstance(s, pd.DataFrame) and not s.empty:
                uber_df = s
        if (uber_df is None or uber_df.empty) and other_sql_data is not None:
            if isinstance(other_sql_data, pd.DataFrame) and not other_sql_data.empty:
                uber_df = other_sql_data
    return uber_df


def _pnl_wavg(df: pd.DataFrame, val_col: str, w_col: str) -> float:
    if val_col not in df.columns or w_col not in df.columns:
        return float("nan")
    v = pd.to_numeric(df[val_col], errors="coerce")
    w = pd.to_numeric(df[w_col], errors="coerce")
    m = v.notna() & w.notna() & (w > 0)
    if not m.any():
        return float("nan")
    return float((v[m] * w[m]).sum() / w[m].sum())


def _pnl_guess_mobility_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    cols = list(df.columns)

    def score_col(c: str, needles: Tuple[str, ...]) -> int:
        s = str(c).lower().replace("_", " ")
        return sum(1 for n in needles if n in s)

    def best_match(needles: Tuple[str, ...], numeric: bool = False) -> Optional[str]:
        best_c, best_s = None, 0
        for c in cols:
            if numeric and not pd.api.types.is_numeric_dtype(df[c]):
                continue
            sc = score_col(c, needles)
            if sc > best_s:
                best_s, best_c = sc, c
        return best_c if best_s else None

    date_c = best_match(("order", "date"))
    if date_c is None:
        for c in cols:
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                date_c = c
                break
    ord_c = best_match(("orders",), True) or best_match(("ord", "count"), True)
    oid = best_match(("order", "id"), True)
    if oid is None:
        for c in cols:
            if str(c).lower() in ("order_id", "orderid"):
                oid = c
                break
    wt_trip = None
    wt_cost = None
    for c in cols:
        cl = str(c).lower().replace("_", " ")
        if "wait" in cl and any(k in cl for k in ("trip", "rev", "revenue", "gross")):
            wt_trip = c
            break
    for c in cols:
        cl = str(c).lower().replace("_", " ")
        if "wait" in cl and any(k in cl for k in ("cost", "rider", "pay", "base")):
            wt_cost = c
            break
    wt_fallback = best_match(("wait", "time"), True)
    if wt_trip is None:
        wt_trip = wt_fallback
    if wt_cost is None:
        wt_cost = wt_fallback
    mgmt = best_match(("management", "fee"), True) or best_match(("mgmt",), True)
    inc_pay = best_match(("incentive", "pay"), True) or best_match(("incentive",), True)
    return {
        "date": date_c,
        "orders": ord_c,
        "order_id": oid,
        "uber_rev_net": best_match(("uber", "rev"), True) or best_match(("uber_rev",), True),
        "wait_time_trip": wt_trip,
        "wait_time_cost": wt_cost,
        "management_fee": mgmt,
        "incentive_pay": inc_pay,
        "base_pay": best_match(("base", "pay"), True),
        "daily_mg_pay": best_match(("daily", "mg"), True),
        "dinner_mg_pay": best_match(("dinner", "mg"), True),
        "lunch_mg_pay": best_match(("lunch", "mg"), True),
        "platform_fee": best_match(("platform", "fee"), True),
        "rev_additional": best_match(("rev", "additional"), True) or best_match(("additional",), True),
        "margin": next(
            (
                c
                for c in cols
                if pd.api.types.is_numeric_dtype(df[c])
                and "margin" in str(c).lower()
                and "%" not in str(c).lower()
            ),
            None,
        ),
        "margin_pct": best_match(("margin", "%"), True),
        "dau": best_match(("dau",), True),
    }


# PNL source columns are summed as **Indian rupees**; “lac” in the UI = ÷ 1,00,000.
PNL_RUPEES_PER_LAC = 100_000.0

# Embedded in every PNL AI (non-SQL) request — must stay aligned with _mobility_pnl_metrics_for_frame.
PNL_AI_SYSTEM_PERSONA = textwrap.dedent(
    """
    Act as a **Financial Data Analyst** for Mobility P&L. Table name in SQL is **`data`** (DuckDB).

    ### Units
    - Column sums are treated as **₹ (rupees)** unless the user says otherwise.
    - Report **Value in lacs** as `amount / 100000.0` (1 lac = ₹1,00,000).
    - **RPO** and **CPO** are **₹ per order** (divide rupee totals by **OPD** = `SUM(orders)` — do not convert RPO/CPO to lac).

    ### Mobility OPD
    - **OPD** = `SUM(orders)` over the filtered period.
    - **RPO:** `GR / OPD`. **CPO:** `(SUM(base_pay) + SUM(wait_time_cost) + SUM(incentive_pay)) / OPD` (excludes MG).

    ### Revenue (aligns with “Gross = uber + mgmt + incentive + Wait”)
    - **Gross revenue (GR):** `SUM(uber_rev_net) + SUM(wait_trip) + SUM(management_fee) + SUM(incentive_pay)` (COALESCE per field).
    - **Matrix / split lines:** uber-only, trip-side wait-only, management-only, and incentive_pay each have their own **₹/order** (month ÷ Σ orders, day ÷ Day OPD).
    - **Trip revenue line (detail):** uber rev net only; **Wait time (trip)** is separate.

    ### Rider-side costs
    - Net rider magnitude (CM bridge) = order pay (base + rider wait) + MG + incentive; display net rider as **negative**.
    - **Matrix (₹/order):** Net Rider Cost row uses **signed** net rider (negative); Order pay and Incentive Cost rows are **negative**; **Gross + Net Rider = CM** per order. **CPO** stays positive (base+wait+incentive stack). **Day** = ÷ Day OPD.
    - **Detail P&L order pay line** may still show base + rider wait for the lac bridge.
    - **CM:** `GR + Net_rider_cost` (algebraic; net rider negative). Matrix **CM** row = period **CM** ÷ Σ orders (month) or ÷ Day OPD (day).

    ### Platform fee (debits)
    - If `SUM(platform_fee) >= 0` in data, treat as debit → **negative** P&L line `-ABS(SUM(...))`; if already negative, keep the sum.

    ### SFX
    - `SFX = 0.8 * ABS(SUM(platform_fee)) + SUM(rev_additional)`; **CM1** = CM + signed platform line + SFX.

    ### Margin % — `100.0 * CM1 / NULLIF(GR, 0)`

    ### Output
    - When asked for a P&L summary, give a compact table: **Metric | Value (lac) | Per order (₹)** where appropriate.
    - Prefer **`sql: SELECT ... FROM data ...`** (one line) when you need aggregates from the dataset.
    """
).strip()


def _pnl_fmt_lac_value(x: object) -> str:
    """Format rupee totals as lac (÷ 1,00,000), one decimal — matches monthly P&L sheet style."""
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "—"
        xf = float(x) / PNL_RUPEES_PER_LAC
        if pd.isna(xf):
            return "—"
        return f"{xf:,.1f}"
    except (TypeError, ValueError):
        return "—"


def _pnl_fmt_num(x: object, *, pct: bool = False) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "—"
        xf = float(x)
        if pd.isna(xf):
            return "—"
        if pct:
            return f"{xf:.2f}%"
        if abs(xf) >= 1e6:
            return f"{xf:,.2f}"
        return f"{xf:,.1f}"
    except (TypeError, ValueError):
        return "—"


def _pnl_fmt_order_count(x: object) -> str:
    """Mobility OPD = sum of orders column (display as count, no lac)."""
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "—"
        xf = float(x)
        if pd.isna(xf):
            return "—"
        return f"{xf:,.0f}"
    except (TypeError, ValueError):
        return "—"


def _pnl_fmt_per_order(val: object, opd: object) -> str:
    """Format lac per order (val ÷ sum of orders), 2 decimals — matches bridge math."""
    try:
        o = float(opd)
        v = float(val)
        if pd.isna(o) or o <= 0 or pd.isna(v):
            return "—"
        return f"{round(v / o, 2):,.2f}"
    except (TypeError, ValueError):
        return "—"


def _pnl_fmt_per_order_from_ratio(ratio: object) -> str:
    """Format a precomputed per-order ratio (already val/opd), 2 decimals."""
    try:
        r = float(ratio)
        if pd.isna(r):
            return "—"
        return f"{round(r, 2):,.2f}"
    except (TypeError, ValueError):
        return "—"


def _pnl_fmt_inr_per_order_ratio(ratio: object) -> str:
    """Rupees per order (÷ OPD), one decimal — matches RPO/CPO on monthly summary."""
    try:
        r = float(ratio)
        if pd.isna(r):
            return "—"
        return f"{round(r, 1):,.1f}"
    except (TypeError, ValueError):
        return "—"


_PNL_MOM_LAC_RUPEE_KEYS = frozenset(
    {
        "gross_rev",
        "uber_trip_rev",
        "wait_trip_rev",
        "management_fee_only",
        "incentive_revenue",
        "net_rider_cost",
        "base_wait",
        "mg_cost",
        "incentive_cost",
        "cm",
        "platform_fee",
        "sfx",
        "cm1",
    }
)


def _pnl_fmt_compare_val(key: str, v: object) -> str:
    """MoM / DoD cell: lac, ₹/order, OPD count, or % as appropriate."""
    if key == "margin_pct":
        return _pnl_fmt_num(v, pct=True)
    if key == "opd":
        return _pnl_fmt_order_count(v)
    if key in ("rpo", "cpo"):
        return _pnl_fmt_inr_per_order_ratio(v)
    if key in ("base_wait", "mg_cost", "incentive_cost"):
        if v == v and pd.notna(v):
            return _pnl_fmt_lac_value(-float(v))
        return "—"
    if key in _PNL_MOM_LAC_RUPEE_KEYS:
        return _pnl_fmt_lac_value(v)
    return _pnl_fmt_num(v)


def _pnl_fmt_compare_delta(key: str, dv: object) -> str:
    """MoM / DoD Δ column (same units as values)."""
    if key == "margin_pct":
        return _pnl_fmt_num(dv, pct=True)
    if key == "opd":
        return _pnl_fmt_order_count(dv)
    if key in ("rpo", "cpo"):
        return _pnl_fmt_inr_per_order_ratio(dv)
    if key in ("base_wait", "mg_cost", "incentive_cost"):
        if dv == dv and pd.notna(dv):
            return _pnl_fmt_lac_value(-float(dv))
        return "—"
    if key in _PNL_MOM_LAC_RUPEE_KEYS:
        return _pnl_fmt_lac_value(dv)
    return _pnl_fmt_num(dv)


def _pnl_sum_num(df: pd.DataFrame, col: Optional[str]) -> float:
    if not col or col not in df.columns:
        return float("nan")
    return float(pd.to_numeric(df[col], errors="coerce").sum())


def _pnl_sum_cols(df: pd.DataFrame, cols: List[Optional[str]]) -> float:
    t = 0.0
    ok = False
    for c in cols:
        if c and c in df.columns:
            t += _pnl_sum_num(df, c)
            ok = True
    return t if ok else float("nan")


def _pnl_nz(x: float) -> float:
    return float(x) if pd.notna(x) else 0.0


def _pnl_compute_mobility_opd(df: pd.DataFrame, m: Dict[str, Optional[str]]) -> float:
    """Mobility OPD for RPO/CPO: SUM(orders)."""
    oc = m.get("orders")
    if not oc or oc not in df.columns:
        return float("nan")
    return float(pd.to_numeric(df[oc], errors="coerce").sum())


def _mobility_pnl_metrics_for_frame(
    df: pd.DataFrame,
    m: Dict[str, Optional[str]],
) -> Dict[str, float]:
    """
    Mobility P&L (internal amounts in **rupees**; UI shows lac = ÷ 1,00,000).
    Trip block = uber_rev_net + Wait_Time (trip); gross = that + management_fee + incentive_pay.
    Also expose uber-only, wait-only, management-only, and incentive_pay sum for matrix / split lines.
    Order pay = base_pay + Wait_Time (cost); net rider magnitude adds MG + incentive.
    OPD = SUM(orders). RPO uses gross/OPD. **CPO** = SUM(base_pay + wait cost + incentive_pay) / OPD (excludes MG). CM = gross + net_rider (negative).
    SFX = 0.8×|platform| + rev_additional; CM1 = CM + platform_signed + SFX.
    Margin % = CM1 / gross_revenue
    """
    out: Dict[str, float] = {}
    opd = _pnl_compute_mobility_opd(df, m)
    out["opd"] = opd

    u, wtt = m.get("uber_rev_net"), m.get("wait_time_trip")
    trip = _pnl_sum_cols(df, [u, wtt])
    out["trip_rev"] = trip

    mgmt, incp = m.get("management_fee"), m.get("incentive_pay")
    mi = _pnl_sum_cols(df, [mgmt, incp])
    out["mgmt_incentive"] = mi

    if pd.isna(trip) and pd.isna(mi):
        out["gross_rev"] = float("nan")
    else:
        out["gross_rev"] = _pnl_nz(trip) + _pnl_nz(mi)
    gr = out["gross_rev"]

    bp, wtc = m.get("base_pay"), m.get("wait_time_cost")
    base_wait = _pnl_sum_cols(df, [bp, wtc])
    out["base_wait"] = base_wait
    out["base_pay_sum"] = _pnl_sum_num(df, bp) if bp and bp in df.columns else float("nan")

    mg_cost = _pnl_sum_cols(df, [m.get("daily_mg_pay"), m.get("dinner_mg_pay"), m.get("lunch_mg_pay")])
    out["mg_cost"] = mg_cost

    inc_cost = _pnl_sum_num(df, incp) if incp and incp in df.columns else float("nan")
    out["incentive_cost"] = inc_cost
    out["incentive_revenue"] = inc_cost

    out["uber_trip_rev"] = _pnl_sum_num(df, u) if u and u in df.columns else float("nan")
    out["wait_trip_rev"] = _pnl_sum_num(df, wtt) if wtt and wtt in df.columns else float("nan")
    out["management_fee_only"] = (
        _pnl_sum_num(df, mgmt) if mgmt and mgmt in df.columns else float("nan")
    )

    _parts_nrc: List[float] = []
    for x in (base_wait, mg_cost, inc_cost):
        if x == x and pd.notna(x):
            _parts_nrc.append(float(x))
    if _parts_nrc:
        nrc_mag = float(sum(_parts_nrc))
    else:
        nrc_mag = float("nan")

    if nrc_mag == nrc_mag:  # not nan
        out["nrc_mag"] = float(nrc_mag)
        out["net_rider_cost"] = -float(nrc_mag)
    else:
        out["nrc_mag"] = float("nan")
        out["net_rider_cost"] = float("nan")

    if gr == gr and nrc_mag == nrc_mag:
        out["cm"] = float(gr) - float(nrc_mag)
    elif m.get("margin") and m["margin"] in df.columns:
        out["cm"] = _pnl_sum_num(df, m["margin"])
    else:
        out["cm"] = float("nan")

    pcol = m.get("platform_fee")
    pfs_raw = _pnl_sum_num(df, pcol) if pcol and pcol in df.columns else float("nan")
    if pfs_raw == pfs_raw:
        pr = float(pfs_raw)
        # Positive in file = debit outflow → negative line; already-negative stays as-is.
        pfs_signed = -abs(pr) if pr >= 0 else pr
        out["platform_fee"] = float(pfs_signed)
        p_abs = abs(pr)
    else:
        out["platform_fee"] = float("nan")
        p_abs = float("nan")

    radd = m.get("rev_additional")
    rev_add = _pnl_sum_num(df, radd) if radd and radd in df.columns else float("nan")
    if p_abs == p_abs:
        out["sfx"] = float(0.8 * float(p_abs) + _pnl_nz(rev_add))
    elif rev_add == rev_add:
        out["sfx"] = float(rev_add)
    else:
        out["sfx"] = float("nan")

    cm = out.get("cm", float("nan"))
    pfs = out.get("platform_fee", float("nan"))
    if cm == cm:
        cm1 = float(cm)
        if pfs == pfs:
            cm1 += float(pfs)
        sfxv = out.get("sfx", float("nan"))
        if sfxv == sfxv:
            cm1 += float(sfxv)
        out["cm1"] = cm1
    else:
        out["cm1"] = float("nan")

    cm1 = out.get("cm1", float("nan"))
    if cm1 == cm1 and gr == gr and gr not in (0, float("nan")):
        out["margin_pct"] = 100.0 * float(cm1) / float(gr)
    else:
        out["margin_pct"] = float("nan")

    # Matrix summary (user definitions): PFD lac = −Σ platform_fee; SFX lac = Σ platform_fee + Σ rev_additional;
    # CM1 (matrix) = CM + PFD (rupees) + SFX (rupees); Margin % (matrix) = CM1 / Gross Revenue.
    if pfs_raw == pfs_raw:
        out["platform_fee_debit_matrix_rupees"] = float(-pfs_raw)
    else:
        out["platform_fee_debit_matrix_rupees"] = float("nan")
    if pfs_raw == pfs_raw and rev_add == rev_add:
        sfx_matrix_rupees = float(pfs_raw) + float(rev_add)
    elif pfs_raw == pfs_raw:
        sfx_matrix_rupees = float(pfs_raw)
    elif rev_add == rev_add:
        sfx_matrix_rupees = float(rev_add)
    else:
        sfx_matrix_rupees = float("nan")
    out["sfx_matrix_rupees"] = sfx_matrix_rupees
    cm1_matrix_rupees = float("nan")
    cmv = out.get("cm", float("nan"))
    pfd_m = out.get("platform_fee_debit_matrix_rupees", float("nan"))
    if cmv == cmv:
        cm1_matrix_rupees = float(cmv)
        if pfd_m == pfd_m:
            cm1_matrix_rupees += float(pfd_m)
        if sfx_matrix_rupees == sfx_matrix_rupees:
            cm1_matrix_rupees += float(sfx_matrix_rupees)
    out["cm1_matrix_rupees"] = cm1_matrix_rupees
    if (
        cm1_matrix_rupees == cm1_matrix_rupees
        and gr == gr
        and gr not in (0, float("nan"))
    ):
        out["margin_matrix_pct"] = 100.0 * float(cm1_matrix_rupees) / float(gr)
    else:
        out["margin_matrix_pct"] = float("nan")

    if opd == opd and opd > 0 and gr == gr:
        out["rpo"] = float(gr) / float(opd)
    else:
        out["rpo"] = float("nan")

    # CPO (sheet): base_pay + Wait_Time (cost) + incentive_pay — not MG; ₹ per order vs Σ orders.
    cpo_cost_sum = _pnl_sum_cols(df, [bp, wtc, incp])
    out["cpo_cost_sum"] = cpo_cost_sum
    if opd == opd and opd > 0 and cpo_cost_sum == cpo_cost_sum:
        out["cpo"] = float(cpo_cost_sum) / float(opd)
    else:
        out["cpo"] = float("nan")

    return out


def _pnl_validate_mobility_orders(m: Dict[str, Optional[str]]) -> Tuple[bool, str]:
    if not m.get("orders"):
        return (False, "Select an **Orders** column for OPD and RPO/CPO.")
    return True, ""


def _mobility_pnl_period_display_rows(met: Dict[str, float]) -> List[Tuple[str, str, str, str, str]]:
    """
    One row per P&L line: (row_key, label, value_lac_str, per_order_str, css_class).
    Mirrors the former full summary table logic.
    """
    opd_v = met.get("opd", float("nan"))
    gr = met.get("gross_rev", float("nan"))
    uber_u = met.get("uber_trip_rev", float("nan"))
    wtt_u = met.get("wait_trip_rev", float("nan"))
    mgmt_u = met.get("management_fee_only", float("nan"))
    inc_u = met.get("incentive_revenue", float("nan"))
    nrc = met.get("net_rider_cost", float("nan"))
    mgc = met.get("mg_cost", float("nan"))
    inc = met.get("incentive_cost", float("nan"))
    cm = met.get("cm", float("nan"))
    pfs = met.get("platform_fee", float("nan"))
    sfxv = met.get("sfx", float("nan"))
    cm1 = met.get("cm1", float("nan"))
    nrm = met.get("nrc_mag", float("nan"))
    bw_line = met.get("base_wait", float("nan"))

    def neg_if_num(x: float) -> float:
        return -float(x) if x == x and pd.notna(x) else float("nan")

    bw_neg = neg_if_num(bw_line)
    mg_neg = neg_if_num(mgc)
    inc_neg = neg_if_num(inc)

    opd_f = (
        float(opd_v)
        if opd_v == opd_v and pd.notna(opd_v) and float(opd_v) > 0
        else float("nan")
    )

    if opd_f == opd_f and gr == gr:
        g_po_r2 = round(gr / opd_f, 1)
    else:
        g_po_r2 = float("nan")
    u_po_r2 = round(uber_u / opd_f, 1) if opd_f == opd_f and uber_u == uber_u else float("nan")
    w_po_r2 = round(wtt_u / opd_f, 1) if opd_f == opd_f and wtt_u == wtt_u else float("nan")
    mg_po_r2 = round(mgmt_u / opd_f, 1) if opd_f == opd_f and mgmt_u == mgmt_u else float("nan")
    if (
        opd_f == opd_f
        and gr == gr
        and g_po_r2 == g_po_r2
        and u_po_r2 == u_po_r2
        and w_po_r2 == w_po_r2
        and mg_po_r2 == mg_po_r2
    ):
        in_po_r2 = round(g_po_r2 - u_po_r2 - w_po_r2 - mg_po_r2, 1)
    elif opd_f == opd_f and inc_u == inc_u:
        in_po_r2 = round(inc_u / opd_f, 1)
    else:
        in_po_r2 = float("nan")

    if opd_f == opd_f and nrc == nrc:
        n_po_r2 = round(nrc / opd_f, 1)
    else:
        n_po_r2 = float("nan")
    if opd_f == opd_f and nrc == nrc and bw_line == bw_line and mgc == mgc and inc == inc:
        mg_po_r2 = round(mg_neg / opd_f, 1)
        inc_po_r2 = round(inc_neg / opd_f, 1)
        bw_po_r2 = round(n_po_r2 - mg_po_r2 - inc_po_r2, 1)
    else:
        bw_po_r2 = round(bw_neg / opd_f, 1) if opd_f == opd_f and bw_line == bw_line else float("nan")
        mg_po_r2 = round(mg_neg / opd_f, 1) if opd_f == opd_f and mgc == mgc else float("nan")
        inc_po_r2 = round(inc_neg / opd_f, 1) if opd_f == opd_f and inc == inc else float("nan")

    if opd_f == opd_f and cm == cm:
        cm_po_r2 = round(cm / opd_f, 1)
    else:
        cm_po_r2 = float("nan")

    rpo_disp = g_po_r2 if g_po_r2 == g_po_r2 else round(met.get("rpo", float("nan")), 1)
    _cpo_m = met.get("cpo", float("nan"))
    cpo_disp = round(_cpo_m, 1) if _cpo_m == _cpo_m else float("nan")
    cpo_sum = met.get("cpo_cost_sum", float("nan"))
    lac_rpo_period = _pnl_fmt_lac_value(gr) if gr == gr and pd.notna(gr) else "—"
    lac_cpo_period = (
        _pnl_fmt_lac_value(cpo_sum) if cpo_sum == cpo_sum and pd.notna(cpo_sum) else "—"
    )

    if opd_f == opd_f and cm1 == cm1 and cm == cm and pfs == pfs and sfxv == sfxv:
        cm1_po_r2 = round(cm1 / opd_f, 1)
        pf_po_r2 = round(pfs / opd_f, 1)
        sfx_po_r2 = round(cm1_po_r2 - cm_po_r2 - pf_po_r2, 1)
    else:
        cm1_po_r2 = round(cm1 / opd_f, 1) if opd_f == opd_f and cm1 == cm1 else float("nan")
        pf_po_r2 = round(pfs / opd_f, 1) if opd_f == opd_f and pfs == pfs else float("nan")
        sfx_po_r2 = round(sfxv / opd_f, 1) if opd_f == opd_f and sfxv == sfxv else float("nan")

    return [
        ("opd", "Mobility OPD (Σ orders)", _pnl_fmt_order_count(opd_v), "—", "pnl-mob-tr-hl"),
        (
            "rpo",
            "RPO (gross ÷ Σ orders)",
            lac_rpo_period,
            _pnl_fmt_inr_per_order_ratio(rpo_disp),
            "",
        ),
        (
            "cpo",
            "CPO (base+wait+incentive ÷ Σ orders)",
            lac_cpo_period,
            _pnl_fmt_inr_per_order_ratio(cpo_disp),
            "",
        ),
        (
            "gross_rev",
            "Gross Revenue (+)",
            _pnl_fmt_lac_value(gr),
            _pnl_fmt_inr_per_order_ratio(g_po_r2),
            "pnl-mob-tr-rev",
        ),
        (
            "uber_trip_rev",
            "Trip Revenue (+) — uber rev net",
            _pnl_fmt_lac_value(uber_u),
            _pnl_fmt_inr_per_order_ratio(u_po_r2),
            "pnl-mob-tr-rev",
        ),
        (
            "wait_trip_rev",
            "Wait time (trip) (+)",
            _pnl_fmt_lac_value(wtt_u),
            _pnl_fmt_inr_per_order_ratio(w_po_r2),
            "pnl-mob-tr-rev",
        ),
        (
            "management_fee_only",
            "Management Fee (+)",
            _pnl_fmt_lac_value(mgmt_u),
            _pnl_fmt_inr_per_order_ratio(mg_po_r2),
            "pnl-mob-tr-rev",
        ),
        (
            "incentive_revenue",
            "Incentive (+)",
            _pnl_fmt_lac_value(inc_u),
            _pnl_fmt_inr_per_order_ratio(in_po_r2),
            "pnl-mob-tr-rev",
        ),
        (
            "net_rider_cost",
            "Net Rider Cost (-)",
            _pnl_fmt_lac_value(nrc),
            _pnl_fmt_inr_per_order_ratio(n_po_r2),
            "pnl-mob-tr-cost",
        ),
        (
            "order_pay",
            "Order pay (-)",
            _pnl_fmt_lac_value(bw_neg),
            _pnl_fmt_inr_per_order_ratio(bw_po_r2),
            "pnl-mob-tr-cost",
        ),
        (
            "mg_cost",
            "MG cost (-)",
            _pnl_fmt_lac_value(mg_neg),
            _pnl_fmt_inr_per_order_ratio(mg_po_r2),
            "pnl-mob-tr-cost",
        ),
        (
            "incentive_cost",
            "Incentive Cost (-)",
            _pnl_fmt_lac_value(inc_neg),
            _pnl_fmt_inr_per_order_ratio(inc_po_r2),
            "pnl-mob-tr-cost",
        ),
        ("cm", "CM", _pnl_fmt_lac_value(cm), _pnl_fmt_inr_per_order_ratio(cm_po_r2), "pnl-mob-tr-cm"),
        (
            "platform_fee",
            "Platform Fee Debits (-)",
            _pnl_fmt_lac_value(pfs),
            _pnl_fmt_inr_per_order_ratio(pf_po_r2),
            "",
        ),
        (
            "sfx",
            "SFX Commission and Recovery (+)",
            _pnl_fmt_lac_value(sfxv),
            _pnl_fmt_inr_per_order_ratio(sfx_po_r2),
            "",
        ),
        (
            "cm1",
            "CM 1",
            _pnl_fmt_lac_value(cm1),
            _pnl_fmt_inr_per_order_ratio(cm1_po_r2),
            "pnl-mob-tr-cm",
        ),
        (
            "margin_pct",
            "Margin % (CM1 ÷ gross revenue)",
            _pnl_fmt_num(met.get("margin_pct"), pct=True),
            "—",
            "pnl-mob-tr-hl",
        ),
    ]


def _pnl_mob_order_date_span_inclusive_days(dfx: pd.DataFrame) -> int:
    """
    Calendar days from min order date to max order date (inclusive).
    Same calendar day → 1. Example: Mon..Tue → 2 (matches 19,280 ÷ 2 = 9,640 style OPD).
    """
    if dfx.empty or "_dt" not in dfx.columns:
        return 0
    d0 = pd.Timestamp(dfx["_dt"].min()).normalize()
    d1 = pd.Timestamp(dfx["_dt"].max()).normalize()
    return int((d1 - d0).days) + 1


def _pnl_mob_month_day_inr_per_order(
    rupees_total: float, sum_o: float, span_days: int
) -> Tuple[float, float]:
    """Month: total ÷ Σ orders. Day: total ÷ (Σ orders ÷ span) — same as RPO/CPO day scaling."""
    if not (rupees_total == rupees_total and sum_o == sum_o and float(sum_o) > 0):
        return float("nan"), float("nan")
    month_r = float(rupees_total) / float(sum_o)
    if span_days > 0:
        day_r = float(rupees_total) / (float(sum_o) / float(span_days))
    else:
        day_r = float("nan")
    return month_r, day_r


def _pnl_negate_cost_magnitude(v: float) -> float:
    """Positive cost sum in file → negative for (−) P&L lines; NaN unchanged."""
    return -float(v) if v == v and pd.notna(v) else float("nan")


def _pnl_mob_opd_month_and_day_totals(
    dfx: pd.DataFrame, m: Dict[str, Optional[str]]
) -> Tuple[float, int, float]:
    """
    Month OPD = Σ orders column; Day OPD = Σ orders ÷ inclusive day span (min→max order date).
    Returns (sum_orders, span_days, day_opd_rate). day_opd is nan if span==0 or sum missing.
    """
    oc = m.get("orders")
    if not oc or oc not in dfx.columns:
        sum_o = float("nan")
    else:
        sum_o = float(pd.to_numeric(dfx[oc], errors="coerce").sum())
    span = _pnl_mob_order_date_span_inclusive_days(dfx)
    if span > 0 and sum_o == sum_o and pd.notna(sum_o):
        day_r = float(sum_o) / float(span)
    else:
        day_r = float("nan")
    return sum_o, span, day_r


def _pnl_mob_cpo_month_and_day(
    met: Dict[str, float], sum_o: float, span_days: int
) -> Tuple[float, float]:
    """
    CPO cost stack = SUM(base_pay + wait cost + incentive_pay) → met['cpo_cost_sum'].
    Month CPO = cpo_cost_sum ÷ Σ orders.
    Day CPO = cpo_cost_sum ÷ (Σ orders ÷ span) — denominator = Day OPD (same as RPO scaling).
    """
    cs = met.get("cpo_cost_sum", float("nan"))
    if not (cs == cs and sum_o == sum_o and float(sum_o) > 0):
        return float("nan"), float("nan")
    month_c = float(cs) / float(sum_o)
    if span_days > 0:
        day_c = float(cs) / (float(sum_o) / float(span_days))
    else:
        day_c = float("nan")
    return month_c, day_c


def _pnl_mob_rpo_month_and_day(
    met: Dict[str, float], sum_o: float, span_days: int
) -> Tuple[float, float]:
    """
    Gross revenue = uber_rev_net + wait (trip) + management_fee + incentive_pay → met['gross_rev'].
    Month RPO = gross ÷ Σ orders.
    Day RPO = gross ÷ (Σ orders ÷ span) = same scaling as Day OPD on the denominator (orders per calendar day).
    """
    gr = met.get("gross_rev", float("nan"))
    if not (gr == gr and sum_o == sum_o and float(sum_o) > 0):
        return float("nan"), float("nan")
    month_r = float(gr) / float(sum_o)
    if span_days > 0:
        day_r = float(gr) / (float(sum_o) / float(span_days))
    else:
        day_r = float("nan")
    return month_r, day_r


def _render_mobility_pnl_views(df: pd.DataFrame) -> None:
    st.markdown("### Mobility P&L")
    st.caption(
        "Column sums are **₹**; **Value (lac)** = ÷ 1,00,000; **Per order (₹)** = ÷ **Σ orders**. "
        "Platform debits (positive in file) show **negative** lac."
    )
    guess = _pnl_guess_mobility_columns(df)
    all_cols = ["—"] + [str(c) for c in df.columns]

    def _col_idx(g: Optional[str]) -> int:
        if g and g in all_cols:
            return all_cols.index(g)
        return 0

    with st.expander("Column mapping (auto-detected — adjust to match your file)", expanded=False):
        st.caption(
            "Source numbers are **₹ (rupees)** per row; metric view shows **lac** (÷ 1,00,000) and **₹ per order** where applicable. "
            "Trip revenue = uber rev net + wait (trip). Net rider cost = **order pay** (base pay + wait on cost side) + MG + incentive pay — aligns with April sheet column **R** (R11–R13, R10)."
        )
        c1, c2 = st.columns(2)
        with c1:
            dc = st.selectbox(
                "Date column",
                all_cols,
                index=_col_idx(guess["date"]),
                help="Required for metrics, MoM, and DoD.",
            )
            oid = st.selectbox(
                "Order ID (optional)",
                all_cols,
                index=_col_idx(guess["order_id"]),
                help="Optional reference column. P&L OPD uses the **Orders** column; **Day OPD** = Σ orders ÷ inclusive days (min→max **Date**).",
            )
            oc = st.selectbox(
                "Orders column (required for OPD)",
                all_cols,
                index=_col_idx(guess["orders"]),
                help="Mobility OPD = **sum** of this column. All per-order lac figures divide by this total.",
            )
        with c2:
            urev = st.selectbox(
                "Uber rev net (trip)",
                all_cols,
                index=_col_idx(guess["uber_rev_net"]),
            )
            wtt = st.selectbox(
                "Wait time — trip revenue side",
                all_cols,
                index=_col_idx(guess.get("wait_time_trip")),
            )
            wtc = st.selectbox(
                "Wait time — rider cost side",
                all_cols,
                index=_col_idx(guess.get("wait_time_cost")),
            )
        c3, c4 = st.columns(2)
        with c3:
            mgmt = st.selectbox(
                "Management fee",
                all_cols,
                index=_col_idx(guess["management_fee"]),
            )
            incp = st.selectbox(
                "Incentive pay",
                all_cols,
                index=_col_idx(guess["incentive_pay"]),
            )
            bp = st.selectbox(
                "Order pay (base pay)",
                all_cols,
                index=_col_idx(guess["base_pay"]),
                help="Sheet row **Order pay (-)**; with rider wait (cost) it is base_pay + wait on cost side.",
            )
        with c4:
            dmp = st.selectbox(
                "Daily MG pay",
                all_cols,
                index=_col_idx(guess["daily_mg_pay"]),
            )
            dnp = st.selectbox(
                "Dinner MG pay",
                all_cols,
                index=_col_idx(guess["dinner_mg_pay"]),
            )
            lmp = st.selectbox(
                "Lunch MG pay",
                all_cols,
                index=_col_idx(guess["lunch_mg_pay"]),
            )
        c5, c6 = st.columns(2)
        with c5:
            pf = st.selectbox(
                "Platform fee",
                all_cols,
                index=_col_idx(guess["platform_fee"]),
            )
            rad = st.selectbox(
                "Rev additional",
                all_cols,
                index=_col_idx(guess["rev_additional"]),
                help="Used in SFX as **0.8×|platform| + rev additional**.",
            )
        with c6:
            mar = st.selectbox(
                "Margin column (fallback if CM cannot be built)",
                all_cols,
                index=_col_idx(guess["margin"]),
            )

    def _m() -> Dict[str, Optional[str]]:
        def p(x: str) -> Optional[str]:
            return None if x == "—" else x

        return {
            "date": p(dc),
            "order_id": p(oid),
            "orders": p(oc),
            "uber_rev_net": p(urev),
            "wait_time_trip": p(wtt),
            "wait_time_cost": p(wtc),
            "management_fee": p(mgmt),
            "incentive_pay": p(incp),
            "base_pay": p(bp),
            "daily_mg_pay": p(dmp),
            "dinner_mg_pay": p(dnp),
            "lunch_mg_pay": p(lmp),
            "platform_fee": p(pf),
            "rev_additional": p(rad),
            "margin": p(mar),
        }

    m = _m()
    mode = st.radio(
        "View",
        ["Metric (one by one)", "Daily detail (source)", "Month on month", "Day on day"],
        horizontal=True,
        key="pnl_view_mode",
    )

    if mode == "Daily detail (source)":
        if m["date"]:
            dfp = df.copy()
            dfp["_d"] = pd.to_datetime(dfp[m["date"]], errors="coerce")
            dfp = dfp.sort_values("_d")
            show_c = [c for c in dfp.columns if c != "_d"]
            st.dataframe(dfp[show_c].head(500), use_container_width=True, height=440)
            st.caption("Up to **500** rows, sorted by date.")
        else:
            st.dataframe(df.head(500), use_container_width=True, height=440)
        return

    if not m.get("date"):
        st.warning("Pick a **Date** column for metrics, MoM, and DoD.")
        st.dataframe(df.head(200), use_container_width=True)
        return

    ok_opd, msg_opd = _pnl_validate_mobility_orders(m)
    if not ok_opd:
        st.warning(msg_opd)
        st.dataframe(df.head(200), use_container_width=True)
        return

    dfx = df.copy()
    dfx["_dt"] = pd.to_datetime(dfx[m["date"]], errors="coerce")
    dfx = dfx[dfx["_dt"].notna()]
    if dfx.empty:
        st.error("No valid dates in the selected date column.")
        return

    dfx["_pm"] = dfx["_dt"].dt.to_period("M")

    def _met(sub: pd.DataFrame) -> Dict[str, float]:
        return _mobility_pnl_metrics_for_frame(sub, m)

    mom_dod_keys = [
        "opd",
        "rpo",
        "cpo",
        "gross_rev",
        "uber_trip_rev",
        "wait_trip_rev",
        "management_fee_only",
        "incentive_revenue",
        "net_rider_cost",
        "base_wait",
        "mg_cost",
        "incentive_cost",
        "cm",
        "platform_fee",
        "sfx",
        "cm1",
        "margin_pct",
    ]
    mom_dod_labels = {
        "opd": "Mobility OPD (Σ orders)",
        "rpo": "RPO (gross ÷ Σ orders)",
        "cpo": "CPO (base+wait+incentive ÷ Σ orders)",
        "gross_rev": "Gross Revenue (+)",
        "uber_trip_rev": "Trip Revenue (+) — uber rev net",
        "wait_trip_rev": "Wait time (trip) (+)",
        "management_fee_only": "Management Fee (+)",
        "incentive_revenue": "Incentive (+)",
        "net_rider_cost": "Net Rider Cost (-)",
        "base_wait": "Order pay (-)",
        "mg_cost": "MG cost",
        "incentive_cost": "Incentive Cost",
        "cm": "CM",
        "platform_fee": "Platform Fee Debits",
        "sfx": "SFX (0.8×PF + rev add)",
        "cm1": "CM 1",
        "margin_pct": "Margin %",
    }

    if mode == "Month on month":
        months = sorted(dfx["_pm"].unique())
        if len(months) < 2:
            st.warning("Need at least **2 calendar months** of rows for month-on-month.")
            return
        p0, p1 = months[-2], months[-1]
        d0 = dfx[dfx["_pm"] == p0]
        d1 = dfx[dfx["_pm"] == p1]
        a0, a1 = _met(d0), _met(d1)
        st.markdown(f"**Months:** {p0} vs **{p1}** (latest)")
        thead = "<tr><th>Metric</th><th>Prior</th><th>Latest</th><th>Δ</th><th>Δ %</th></tr>"
        tbody = ""
        for k in mom_dod_keys:
            v0, v1 = a0.get(k, float("nan")), a1.get(k, float("nan"))
            dv = v1 - v0 if pd.notna(v0) and pd.notna(v1) else float("nan")
            dp = (dv / v0 * 100.0) if pd.notna(v0) and v0 not in (0, float("nan")) and pd.notna(dv) else float("nan")
            tbody += (
                f"<tr><td>{html.escape(mom_dod_labels[k])}</td>"
                f'<td class="num">{_pnl_fmt_compare_val(k, v0)}</td>'
                f'<td class="num">{_pnl_fmt_compare_val(k, v1)}</td>'
                f'<td class="num">{_pnl_fmt_compare_delta(k, dv)}</td>'
                f'<td class="num">{_pnl_fmt_num(dp, pct=True)}</td></tr>'
            )
        st.markdown(
            f'<div class="pnl-mob-wrap"><table class="pnl-mob-table">{thead}{tbody}</table></div>',
            unsafe_allow_html=True,
        )
        return

    months_list = sorted(dfx["_pm"].dropna().unique())
    if not months_list:
        st.warning("No calendar months found in the date range.")
        return
    _pm_ix = len(months_list) - 1
    sel_pm = st.selectbox(
        "Calendar month (matrix & day-on-day use this month only)",
        options=months_list,
        index=_pm_ix,
        format_func=lambda p: str(p),
        key="pnl_mob_focus_month",
    )
    dfx_work = dfx[dfx["_pm"] == sel_pm].copy()
    if dfx_work.empty:
        st.warning("No rows in the selected month.")
        return

    if mode == "Metric (one by one)":
        met = _met(dfx_work)
        rows = _mobility_pnl_period_display_rows(met)
        row_keys = [r[0] for r in rows]
        labels_by_key = {r[0]: r[1] for r in rows}

        st.markdown(
            f"Period: **{dfx_work['_dt'].min().date()}** → **{dfx_work['_dt'].max().date()}** · calendar month **{sel_pm}** · data rows **{len(dfx_work):,}**"
        )
        sum_o, span_days, day_opd = _pnl_mob_opd_month_and_day_totals(dfx_work, m)
        _, day_rpo = _pnl_mob_rpo_month_and_day(met, sum_o, span_days)
        _, day_cpo = _pnl_mob_cpo_month_and_day(met, sum_o, span_days)

        grx = met.get("gross_rev", float("nan"))
        _, d_gross = _pnl_mob_month_day_inr_per_order(grx, sum_o, span_days)
        _, d_uber = _pnl_mob_month_day_inr_per_order(
            met.get("uber_trip_rev", float("nan")), sum_o, span_days
        )
        _, d_wait = _pnl_mob_month_day_inr_per_order(
            met.get("wait_trip_rev", float("nan")), sum_o, span_days
        )
        _, d_mgmt = _pnl_mob_month_day_inr_per_order(
            met.get("management_fee_only", float("nan")), sum_o, span_days
        )
        _, d_inc = _pnl_mob_month_day_inr_per_order(
            met.get("incentive_revenue", float("nan")), sum_o, span_days
        )
        # Full P&L net rider (negative) so Gross (+) + Net Rider = CM algebraically in ₹/order.
        _, d_nrc = _pnl_mob_month_day_inr_per_order(
            met.get("net_rider_cost", float("nan")), sum_o, span_days
        )
        _, d_bpy = _pnl_mob_month_day_inr_per_order(
            _pnl_negate_cost_magnitude(met.get("base_pay_sum", float("nan"))),
            sum_o,
            span_days,
        )
        _, d_icc = _pnl_mob_month_day_inr_per_order(
            _pnl_negate_cost_magnitude(met.get("incentive_cost", float("nan"))),
            sum_o,
            span_days,
        )
        _, d_cm = _pnl_mob_month_day_inr_per_order(
            met.get("cm", float("nan")), sum_o, span_days
        )
        _, d_pfd = _pnl_mob_month_day_inr_per_order(
            met.get("platform_fee_debit_matrix_rupees", float("nan")),
            sum_o,
            span_days,
        )
        _, d_sfx_m = _pnl_mob_month_day_inr_per_order(
            met.get("sfx_matrix_rupees", float("nan")), sum_o, span_days
        )
        _, d_cm1_m = _pnl_mob_month_day_inr_per_order(
            met.get("cm1_matrix_rupees", float("nan")), sum_o, span_days
        )

        def _mtx_lac_period(v: float) -> str:
            return _pnl_fmt_lac_value(v) if v == v and pd.notna(v) else "—"

        lac_opd = (
            _pnl_fmt_order_count(sum_o)
            if sum_o == sum_o and pd.notna(sum_o)
            else "—"
        )
        lac_rpo = _mtx_lac_period(grx)
        lac_cpo = _mtx_lac_period(met.get("cpo_cost_sum", float("nan")))
        lac_gross = _mtx_lac_period(grx)
        lac_uber = _mtx_lac_period(met.get("uber_trip_rev", float("nan")))
        lac_wait = _mtx_lac_period(met.get("wait_trip_rev", float("nan")))
        lac_mgmt = _mtx_lac_period(met.get("management_fee_only", float("nan")))
        lac_inc_p = _mtx_lac_period(met.get("incentive_revenue", float("nan")))
        lac_nrc = _mtx_lac_period(met.get("net_rider_cost", float("nan")))
        lac_bpy = _mtx_lac_period(_pnl_negate_cost_magnitude(met.get("base_pay_sum", float("nan"))))
        lac_icc = _mtx_lac_period(_pnl_negate_cost_magnitude(met.get("incentive_cost", float("nan"))))
        lac_cm = _mtx_lac_period(met.get("cm", float("nan")))
        lac_pfd = _mtx_lac_period(met.get("platform_fee_debit_matrix_rupees", float("nan")))
        lac_sfx_m = _mtx_lac_period(met.get("sfx_matrix_rupees", float("nan")))
        lac_cm1_m = _mtx_lac_period(met.get("cm1_matrix_rupees", float("nan")))
        margin_m = met.get("margin_matrix_pct", float("nan"))
        margin_m_str = _pnl_fmt_num(margin_m, pct=True)

        day_opd_cell = f"{day_opd:,.1f}" if day_opd == day_opd and span_days > 0 else "—"
        st.markdown("#### Matrix summary")
        mtx_thead = (
            "<tr><th>Metric</th><th>Lac (period)</th>"
            "<th>₹/order (day)</th></tr>"
        )
        mtx_body = (
            f'<tr class="pnl-mob-tr-hl"><td>{html.escape("OPD")}</td>'
            f'<td class="num">{lac_opd}</td>'
            f'<td class="num">{day_opd_cell}</td></tr>'
            f'<tr><td>{html.escape("RPO (₹ / order)")}</td>'
            f'<td class="num">{lac_rpo}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(day_rpo)}</td></tr>'
            f'<tr><td>{html.escape("CPO (₹ / order)")}</td>'
            f'<td class="num">{lac_cpo}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(day_cpo)}</td></tr>'
            f'<tr class="pnl-mob-tr-rev"><td>{html.escape("Gross Revenue (+) (₹ / order)")}</td>'
            f'<td class="num">{lac_gross}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(d_gross)}</td></tr>'
            f'<tr class="pnl-mob-tr-rev"><td>{html.escape("Trip Revenue — uber net (₹ / order)")}</td>'
            f'<td class="num">{lac_uber}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(d_uber)}</td></tr>'
            f'<tr class="pnl-mob-tr-rev"><td>{html.escape("Wait time — trip (₹ / order)")}</td>'
            f'<td class="num">{lac_wait}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(d_wait)}</td></tr>'
            f'<tr class="pnl-mob-tr-rev"><td>{html.escape("Management Fee (+) (₹ / order)")}</td>'
            f'<td class="num">{lac_mgmt}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(d_mgmt)}</td></tr>'
            f'<tr class="pnl-mob-tr-rev"><td>{html.escape("Incentive (+) (₹ / order)")}</td>'
            f'<td class="num">{lac_inc_p}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(d_inc)}</td></tr>'
            f'<tr class="pnl-mob-tr-cost"><td>{html.escape("Net Rider Cost (-) (₹ / order)")}</td>'
            f'<td class="num">{lac_nrc}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(d_nrc)}</td></tr>'
            f'<tr class="pnl-mob-tr-cost"><td>{html.escape("Order pay (-) (₹ / order)")}</td>'
            f'<td class="num">{lac_bpy}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(d_bpy)}</td></tr>'
            f'<tr class="pnl-mob-tr-cost"><td>{html.escape("Incentive Cost (-) (₹ / order)")}</td>'
            f'<td class="num">{lac_icc}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(d_icc)}</td></tr>'
            f'<tr class="pnl-mob-tr-cm"><td>{html.escape("CM (₹ / order)")}</td>'
            f'<td class="num">{lac_cm}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(d_cm)}</td></tr>'
            f'<tr class="pnl-mob-tr-cost"><td>{html.escape("Platform Fee Debits (-) (₹ / order)")}</td>'
            f'<td class="num">{lac_pfd}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(d_pfd)}</td></tr>'
            f'<tr class="pnl-mob-tr-rev"><td>{html.escape("SFX Commission and Recovery (+) (₹ / order)")}</td>'
            f'<td class="num">{lac_sfx_m}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(d_sfx_m)}</td></tr>'
            f'<tr class="pnl-mob-tr-cm"><td>{html.escape("CM1 (₹ / order)")}</td>'
            f'<td class="num">{lac_cm1_m}</td>'
            f'<td class="num">{_pnl_fmt_inr_per_order_ratio(d_cm1_m)}</td></tr>'
            f'<tr class="pnl-mob-tr-cm"><td>{html.escape("Margin % (CM1 / Gross Revenue)")}</td>'
            f'<td class="num">{margin_m_str}</td>'
            f'<td class="num">{margin_m_str}</td></tr>'
        )
        st.markdown(
            f'<div class="pnl-mob-wrap"><table class="pnl-mob-table">{mtx_thead}{mtx_body}</table></div>',
            unsafe_allow_html=True,
        )
        st.caption(
            "**Lac (period)** = **₹ ÷ 1,00,000** for each line (**not** ÷ orders); **OPD** row shows **Σ orders** (total orders) in this column. "
            "**₹/order (day)** = line total ÷ **Day OPD** (Σ orders ÷ inclusive days, min→max **Date** **within the selected calendar month**). "
            "**Gross Revenue** row uses full gross (uber + trip wait + mgmt + incentive). **Trip / Wait / Mgmt / Incentive** rows use each column sum. "
            "**Net Rider Cost** matrix = full **Net Rider Cost** (signed **negative**), incl. MG + wait — so **Gross + Net Rider = CM** in ₹/order. "
            "**CPO** row stays the sheet stack (base+wait+incentive, **positive** ₹/order). "
            "**Order pay** / **Incentive Cost** matrix = **negative** ₹/order (payouts). "
            "**Platform Fee Debits** lac = **−Σ platform_fee**; **SFX** lac = **Σ platform_fee + Σ rev_additional** (matrix definition; MoM **SFX** line may use 0.8×|platform| + rev). "
            "**CM1** (matrix) = **CM + Platform Fee Debits + SFX** (same rupee bases as those rows). **Margin %** = **CM1 ÷ Gross Revenue**. "
            f"**Days** = **{span_days}** ({dfx_work['_dt'].min().date()} → {dfx_work['_dt'].max().date()}) **in month {sel_pm}**."
        )

        pick = st.selectbox(
            "Metric",
            row_keys,
            format_func=lambda k: labels_by_key.get(k, k),
            key="pnl_mob_metric_pick",
        )
        sel = next(x for x in rows if x[0] == pick)
        _, lab, lac_s, po_s, cls = sel
        _mtx_metric_keys = frozenset(
            {
                "opd",
                "rpo",
                "cpo",
                "gross_rev",
                "uber_trip_rev",
                "wait_trip_rev",
                "management_fee_only",
                "incentive_revenue",
                "net_rider_cost",
                "order_pay",
                "incentive_cost",
                "cm",
                "platform_fee",
                "sfx",
                "cm1",
                "margin_pct",
            }
        )
        if pick in _mtx_metric_keys:
            st.info(
                "**Month vs Day** **₹/order** lines for OPD, RPO, CPO, revenue, rider-cost, **CM**, **platform / SFX / CM1 / margin** are in the **Matrix summary** above."
            )
        else:
            thead = "<tr><th>Metric</th><th>Value (lac)</th><th>Per order (₹)</th></tr>"
            tbody = (
                f'<tr class="{cls}"><td>{html.escape(lab)}</td>'
                f'<td class="num">{lac_s}</td><td class="num">{po_s}</td></tr>'
            )
            st.markdown(
                f'<div class="pnl-mob-wrap"><table class="pnl-mob-table">{thead}{tbody}</table></div>',
                unsafe_allow_html=True,
            )
        return

    elif mode == "Day on day":
        dfx_work["_day"] = dfx_work["_dt"].dt.normalize()
        days = sorted(dfx_work["_day"].unique())
        if len(days) < 2:
            st.warning(
                f"Need at least **2 distinct days** in **{sel_pm}** for day-on-day (same month as matrix)."
            )
            return
        d0, d1 = days[-2], days[-1]
        dd0 = dfx_work[dfx_work["_day"] == d0]
        dd1 = dfx_work[dfx_work["_day"] == d1]
        a0, a1 = _met(dd0), _met(dd1)
        st.markdown(
            f"**Days:** {d0.date()} vs **{d1.date()}** (latest) · calendar month **{sel_pm}** "
            "(prior vs latest day **within this month only**; not month-on-month)."
        )
        thead = "<tr><th>Metric</th><th>Prior day</th><th>Latest day</th><th>Δ</th><th>Δ %</th></tr>"
        tbody = ""
        for k in mom_dod_keys:
            v0, v1 = a0.get(k, float("nan")), a1.get(k, float("nan"))
            dv = v1 - v0 if pd.notna(v0) and pd.notna(v1) else float("nan")
            dp = (dv / v0 * 100.0) if pd.notna(v0) and v0 not in (0, float("nan")) and pd.notna(dv) else float("nan")
            tbody += (
                f"<tr><td>{html.escape(mom_dod_labels[k])}</td>"
                f'<td class="num">{_pnl_fmt_compare_val(k, v0)}</td>'
                f'<td class="num">{_pnl_fmt_compare_val(k, v1)}</td>'
                f'<td class="num">{_pnl_fmt_compare_delta(k, dv)}</td>'
                f'<td class="num">{_pnl_fmt_num(dp, pct=True)}</td></tr>'
            )
        st.markdown(
            f'<div class="pnl-mob-wrap"><table class="pnl-mob-table">{thead}{tbody}</table></div>',
            unsafe_allow_html=True,
        )


def render_pnl_ai_panel(
    work_df: pd.DataFrame,
    *,
    ai_provider: str,
    gemini_api_key: str,
    gemini_model_name: str,
    ollama_base_url: str,
    ollama_model: str,
) -> None:
    """AI + DuckDB SQL for the PNL tab (separate chat from Dashboard)."""
    st.divider()
    st.markdown("### PNL AI assistant")
    st.caption(
        "Ask about **margins**, **RPO/CPO**, **trends**, or run **`sql:`** on table **`data`** (DuckDB). "
        "Uses this tab’s loaded file only. Each answer uses the **same business rules** as the Mobility P&L table above."
    )
    with st.expander("PNL AI — business rules (copy for external tools)", expanded=False):
        st.markdown(
            "The prompt below is **automatically** sent with every PNL chat message. Copy it if you configure another assistant. "
            "**Variance vs target** (e.g. April actual column **R** vs target **S**) is not a built-in view yet — use **Month on month** or `sql:` to compare periods or columns."
        )
        st.code(PNL_AI_SYSTEM_PERSONA, language="text")
    c1, c2 = st.columns([1, 4])
    with c1:
        if st.button("Clear PNL chat", key="btn_clear_pnl_chat"):
            st.session_state.pnl_messages = []
            st.rerun()
    with c2:
        st.caption("Example: `sql: SELECT DATE_TRUNC('month', order_date) AS m, SUM(orders) FROM data GROUP BY 1`")

    _uo = st.session_state.get(_k_upload_orig("PNL"))
    base_for_sql = _uo if _uo is not None else work_df.copy()

    for m in st.session_state.pnl_messages:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])
            if m.get("df_preview") is not None:
                st.dataframe(m["df_preview"], use_container_width=True)

    ut = st.chat_input(
        "Ask about P&L… or `sql: SELECT … FROM data …`",
        key="pnl_chat_input",
    )
    if not ut:
        return

    st.session_state.pnl_messages.append({"role": "user", "content": ut})
    assistant_content = ""
    df_preview = None
    sql_used = None
    rerun_after_sql = False

    with st.chat_message("assistant"):
        try:
            if ai_provider == "Gemini API" and not gemini_api_key:
                raise ValueError("Add a Gemini API key in the sidebar (or use Local Ollama).")
            if ut.strip().lower().startswith("sql:"):
                sql_used = ut.split(":", 1)[1].strip()
                with st.spinner("Running SQL on PNL data…"):
                    result_df = run_sql_on_dataframe(base_for_sql, sql_used)
                # Preview only — base data unchanged
                assistant_content = f"SQL returned **{len(result_df):,}** rows (preview below — base data unchanged)."
                st.code(sql_used, language="sql")
                df_preview = result_df.head(500)
                st.dataframe(df_preview, use_container_width=True)
            else:
                sample_cols = ", ".join([str(c) for c in work_df.columns])
                sample_preview = work_df.head(200).to_string(index=False)
                stats_preview = work_df.describe(include="all").fillna("").to_string()
                sample_prompt = textwrap.dedent(
                    f"""
                    {PNL_AI_SYSTEM_PERSONA}

                    ---
                    Answer ONLY from this dataset. Be concise; use numbers when helpful.
                    If the user needs aggregation, reply with ONE line:
                    sql: SELECT ... FROM data ...;

                    Columns in `data`: {sample_cols}

                    Summary stats:
                    {stats_preview}

                    Sample rows:
                    {sample_preview}

                    User question:
                    {ut}
                    """
                ).strip()
                with st.spinner("Analyzing…"):
                    assistant_content = call_ai_text_unified(
                        sample_prompt,
                        ai_provider,
                        gemini_api_key,
                        gemini_model_name,
                        ollama_base_url,
                        ollama_model,
                    )
                st.markdown(assistant_content or "OK.")
                extracted_sql = extract_sql_from_text(assistant_content)
                if extracted_sql:
                    try:
                        with st.spinner("Running SQL from AI reply…"):
                            result_df = run_sql_on_dataframe(base_for_sql, extracted_sql)
                        # Preview only — base data unchanged
                        sql_used = extracted_sql
                        assistant_content = (
                            (assistant_content or "")
                            + f"\n\n**Executed SQL** → **{len(result_df):,}** rows (preview, base data unchanged)."
                        )
                        df_preview = result_df.head(500)
                        st.success(f"Executed query: **{len(result_df):,}** rows.")
                        st.code(extracted_sql, language="sql")
                        st.dataframe(df_preview, use_container_width=True)
                    except Exception as sql_err:
                        st.warning(f"Could not run SQL from reply: {sql_err}")
                        df_preview = work_df.head(300)
                        st.dataframe(df_preview, use_container_width=True)
                else:
                    df_preview = work_df.head(300)
                    st.dataframe(df_preview, use_container_width=True)
        except Exception as e:
            st.error(str(e))
            assistant_content = f"Error: {e}"

    st.session_state.pnl_messages.append(
        {
            "role": "assistant",
            "content": assistant_content or "OK.",
            "sql": sql_used,
            "df_preview": df_preview,
        }
    )
    if rerun_after_sql:
        st.rerun()


def render_pnl_tab(
    db_type: str,
    other_sql_data: Optional[pd.DataFrame],
    *,
    ai_provider: str,
    gemini_api_key: str,
    gemini_model_name: str,
    ollama_base_url: str,
    ollama_model: str,
) -> None:
    """PNL tab: Mobility-style P&L + dedicated AI (per-tab data)."""
    st.markdown(
        f"""
        <div class="pnl-card" style="margin-bottom:0.5rem;">
          <p class="pnl-card-meta" style="margin:0;font-size:0.88rem;">
            <strong>Source:</strong> {html.escape(str(db_type))}
            <span style="opacity:0.85;"> · Workspace: <strong>PNL</strong></span>
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    uber_df = _resolve_tab_work_df(db_type, other_sql_data, "PNL")
    if uber_df is not None and isinstance(uber_df, pd.DataFrame) and not uber_df.empty:
        _render_mobility_pnl_views(uber_df)
        work_view = st.session_state.get(_k_last("PNL"))
        if not isinstance(work_view, pd.DataFrame) or work_view.empty:
            work_view = uber_df
        render_pnl_ai_panel(
            work_view,
            ai_provider=ai_provider,
            gemini_api_key=gemini_api_key,
            gemini_model_name=gemini_model_name,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )
    else:
        st.markdown(
            """
            <div class="pnl-card" style="text-align:center;padding:1.75rem 1rem;">
              <div style="font-size:2.25rem;line-height:1;margin-bottom:0.5rem;">📑</div>
              <p class="pnl-card-title" style="margin:0;font-weight:600;font-size:1.05rem;">PNL — no dataset loaded</p>
              <p class="pnl-card-desc" style="margin:0.5rem 0 0 0;font-size:0.92rem;max-width:28rem;margin-left:auto;margin-right:auto;">
                Use <strong>Select Database Type</strong> and load data for this tab — then return here.
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_dashboard_tab(
    db_type: str,
    other_sql_data: Optional[pd.DataFrame],
    *,
    ai_provider: str,
    gemini_api_key: str,
    gemini_model_name: str,
    ollama_base_url: str,
    ollama_model: str,
) -> None:
    """Dashboard tab: same loaded dataset as **Select Database Type**; charts + embedded AI (schema-agnostic)."""
    st.markdown(
        f"""
        <div class="pnl-card" style="margin-bottom:0.5rem;">
          <p class="pnl-card-meta" style="margin:0;font-size:0.88rem;">
            <strong>Source:</strong> {html.escape(str(db_type))}
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    uber_df = _resolve_tab_work_df(db_type, other_sql_data, "Dashboard")
    if uber_df is not None and isinstance(uber_df, pd.DataFrame) and not uber_df.empty:
        render_uber_ops_dashboard(uber_df)
        work_view = st.session_state.get(_k_last("Dashboard"))
        if not isinstance(work_view, pd.DataFrame) or work_view.empty:
            work_view = uber_df
        render_dashboard_ai_panel(
            work_view,
            ai_provider=ai_provider,
            gemini_api_key=gemini_api_key,
            gemini_model_name=gemini_model_name,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )
    else:
        st.markdown(
            """
            <div class="pnl-card" style="text-align:center;padding:1.75rem 1rem;">
              <div style="font-size:2.25rem;line-height:1;margin-bottom:0.5rem;">📊</div>
              <p class="pnl-card-title" style="margin:0;font-weight:600;font-size:1.05rem;">No dataset loaded yet</p>
              <p class="pnl-card-desc" style="margin:0.5rem 0 0 0;font-size:0.92rem;max-width:28rem;margin-left:auto;margin-right:auto;">
                Load a sample or run SQL from the sidebar (BigQuery), upload a file or sheet, or use
                <strong>Fetch SQL Data</strong> for Other SQL — then come back here.
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )


# --- Streamlit UI ---

if "auth_ok" not in st.session_state:
    st.session_state.auth_ok = False
if "auth_user" not in st.session_state:
    st.session_state.auth_user = ""
if "auth_role" not in st.session_state:
    st.session_state.auth_role = "user"
if "auth_pages" not in st.session_state:
    st.session_state.auth_pages = []
if "app_theme" not in st.session_state:
    st.session_state.app_theme = "dark"

if not st.session_state.auth_ok:
    _load_auth_users()
    st.markdown(_LOGIN_SCREEN_CSS, unsafe_allow_html=True)
    _c1, _c2, _c3 = st.columns([1, 2, 1])
    with _c2:
        st.title("Operation Dashboard")
        st.markdown(
            '<p style="text-align:center;color:#94a3b8;font-size:0.9rem;margin:-0.5rem 0 1rem 0;">'
            "Sign in to access analytics</p>",
            unsafe_allow_html=True,
        )
        sign_up_mode = st.toggle("New account? Sign up", key="signup_toggle")
        if not sign_up_mode:
            user_id = st.text_input("User ID", placeholder="Enter your user ID")
            password = st.text_input("Password", type="password", placeholder="••••••")
            if st.button("Login", type="primary", use_container_width=True, key="login_submit"):
                ok, msg = _auth_try_login(user_id, password)
                if ok:
                    _uid = (user_id or "").strip()
                    _udata = _auth_get_user(_uid) or {}
                    st.session_state.auth_ok = True
                    st.session_state.auth_user = _uid
                    st.session_state.auth_role = _udata.get("role", "user")
                    st.session_state.auth_pages = _udata.get("pages", [])
                    st.success("Login successful! Redirecting…")
                    st.rerun()
                else:
                    st.error(msg)
        else:
            new_id = st.text_input("Choose a user ID", placeholder="e.g., john_doe")
            new_pw = st.text_input("Choose a password", type="password", placeholder="••••••")
            confirm_pw = st.text_input("Confirm password", type="password", placeholder="••••••")
            if st.button("Create account", type="primary", use_container_width=True, key="signup_submit"):
                ok, msg = _auth_register(new_id, new_pw, confirm_pw)
                if ok:
                    st.success("Account created! Turn off **New account? Sign up** and sign in.")
                    st.session_state.signup_toggle = False
                    st.rerun()
                else:
                    st.error(msg)
    st.stop()

st.markdown(_PNL_FULL_SITE_CSS, unsafe_allow_html=True)
st.markdown(_build_theme_override_css(st.session_state.app_theme == "dark"), unsafe_allow_html=True)

# Hide dataframe download toolbar for non-admin users
if st.session_state.get("auth_role") != "admin":
    st.markdown(
        "<style>"
        "[data-testid='stElementToolbar'] { display: none !important; }"
        "button[title='Download'], [aria-label='Download'] { display: none !important; }"
        "</style>",
        unsafe_allow_html=True,
    )

# --- Sidebar: vertical nav layout (brand, search, nav items, AI, data, footer) ---
if "main_view" not in st.session_state:
    st.session_state.main_view = "AI Chat"

st.sidebar.markdown(
    '<div class="sb-brand"><span class="sb-logo">OP</span><span class="sb-title">Operation Dashboard</span></div>',
    unsafe_allow_html=True,
)
st.sidebar.text_input(
    "Search",
    placeholder="Search…",
    label_visibility="collapsed",
    key="sidebar_search",
)
st.sidebar.markdown('<p class="sb-nav-label">Navigation</p>', unsafe_allow_html=True)

_is_admin = st.session_state.get("auth_role") == "admin"
_allowed_pages = st.session_state.get("auth_pages", [])

mv = st.session_state.main_view

# Redirect to a valid page if current view is not accessible
_all_accessible = list(_allowed_pages) + (["Admin"] if _is_admin else [])
if mv not in _all_accessible and _all_accessible:
    st.session_state.main_view = _all_accessible[0]
    mv = st.session_state.main_view

nav_dash = (
    st.sidebar.button(
        "🏠  Dashboard",
        help="KPIs & charts (Uber-style ops view)",
        use_container_width=True,
        key="nav_btn_dashboard",
        type="primary" if mv == "Dashboard" else "secondary",
    )
    if "Dashboard" in _allowed_pages
    else False
)
nav_pnl = (
    st.sidebar.button(
        "📑  PNL",
        help="P&L workspace (same data source as Operations)",
        use_container_width=True,
        key="nav_btn_pnl",
        type="primary" if mv == "PNL" else "secondary",
    )
    if "PNL" in _allowed_pages
    else False
)
nav_chat = (
    st.sidebar.button(
        "💬  AI Chat",
        help="Ask questions about your loaded data",
        use_container_width=True,
        key="nav_btn_aichat",
        type="primary" if mv == "AI Chat" else "secondary",
    )
    if "AI Chat" in _allowed_pages
    else False
)
nav_admin = (
    st.sidebar.button(
        "🛡  Admin",
        help="Manage users, roles, and page access",
        use_container_width=True,
        key="nav_btn_admin",
        type="primary" if mv == "Admin" else "secondary",
    )
    if _is_admin
    else False
)

if nav_dash:
    st.session_state.main_view = "Dashboard"
    st.rerun()
if nav_pnl:
    st.session_state.main_view = "PNL"
    st.rerun()
if nav_chat:
    st.session_state.main_view = "AI Chat"
    st.rerun()
if nav_admin:
    st.session_state.main_view = "Admin"
    st.rerun()

st.sidebar.divider()
_theme_is_dark = st.session_state.app_theme == "dark"
_theme_label = "🌙  Dark mode" if _theme_is_dark else "☀️  Light mode"
if st.sidebar.button(
    _theme_label,
    key="btn_toggle_theme",
    use_container_width=True,
    help="Switch between dark and light mode (applies to sidebar + main together)",
):
    st.session_state.app_theme = "light" if _theme_is_dark else "dark"
    st.rerun()

if st.sidebar.button("Log out", key="btn_auth_logout", use_container_width=True):
    st.session_state.auth_ok = False
    st.session_state.auth_user = ""
    st.session_state.auth_role = "user"
    st.session_state.auth_pages = []
    st.rerun()
if st.session_state.get("auth_user"):
    st.sidebar.caption(f"Signed in as **{st.session_state.auth_user}**")
st.sidebar.divider()
st.sidebar.subheader("AI assistant")
# Prefer free/local by default
ai_provider = st.sidebar.selectbox("AI provider", ["Gemini API", "Local (Ollama)"], index=0)

ollama_base_url = "http://localhost:11434"
ollama_model = "deepseek-r1:7b"
if ai_provider == "Local (Ollama)":
    with st.sidebar.expander("Ollama settings"):
        ollama_base_url = st.text_input("Ollama URL", value=ollama_base_url)
        ollama_model = st.text_input(
            "Ollama model",
            value=ollama_model,
            help="Pull once: ollama pull deepseek-r1:7b",
        )
        st.checkbox(
            "Force CPU for Ollama (num_gpu=0 — avoids ‘runner terminated’ on small GPUs)",
            value=True,
            key="ollama_force_cpu",
        )
        st.caption(
            "7B models often crash on 4GB VRAM; leave this **on** or use `qwen2.5:3b-instruct`. "
            "`ollama pull deepseek-r1:7b` then run Ollama."
        )

gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
if ai_provider == "Gemini API":
    _saved_api_keys = _apikeys_load()
    if _saved_api_keys:
        # Dropdown for everyone — names only, raw key never shown
        _key_options = [k["name"] for k in _saved_api_keys]
        _sel_key_name = st.sidebar.selectbox(
            "Select API key",
            _key_options,
            key="sb_apikey_sel",
            help="Saved keys are managed by admin in Admin → API Key Vault.",
        )
        _matched = next((k for k in _saved_api_keys if k["name"] == _sel_key_name), None)
        if _matched:
            gemini_api_key = _matched["key"]
    else:
        # No keys saved yet
        if _is_admin:
            st.sidebar.info("No API keys saved. Go to **Admin → 🔑 API Key Vault** to add one.")
        else:
            st.sidebar.warning("No API key available. Ask your admin to add one.")

gemini_model_name = "gemini-2.5-flash"
if ai_provider == "Gemini API":
    gemini_model_name = st.sidebar.selectbox(
        "Gemini model",
        [
            "gemini-2.5-flash",
            "gemini-2.5-flash-lite",
            "gemini-2.5-pro",
            "gemini-3-flash",
            "gemini-3.1-flash",
        ],
        index=0,
        help="Select the Gemini model to use. Newer models (3.x) require access via ai.google.dev.",
    )

st.sidebar.divider()
st.sidebar.subheader("Data source")
st.sidebar.caption(
    "Each tab (**Dashboard**, **PNL**, **AI Chat**) keeps its own loaded data — switch tab, then load."
)
db_type = st.sidebar.selectbox(
    "Select Database Type",
    [
        "BigQuery",
        "Upload CSV / Excel",
        "Google Sheet (CSV link)",
        "Google Drive (pick a Sheet)",
        "Apps Script (Private Sheet)",
        "Other SQL (Postgres/MySQL)",
    ],
)

# Shared session state (BigQuery + file + sheet modes)
if "messages" not in st.session_state:
    st.session_state.messages = []
if "last_df" not in st.session_state:
    st.session_state.last_df = None
if "last_sql" not in st.session_state:
    st.session_state.last_sql = None
if "sample_df" not in st.session_state:
    st.session_state.sample_df = None
if "sample_df_raw" not in st.session_state:
    st.session_state.sample_df_raw = None
if "file_source_label" not in st.session_state:
    st.session_state.file_source_label = None
if "upload_df_original" not in st.session_state:
    st.session_state.upload_df_original = None
if "dashboard_messages" not in st.session_state:
    st.session_state.dashboard_messages = []
if "pnl_messages" not in st.session_state:
    st.session_state.pnl_messages = []

_migrate_legacy_shared_dfs()
for _v in ("Dashboard", "PNL", "AI Chat"):
    if _k_last(_v) not in st.session_state:
        st.session_state[_k_last(_v)] = None
    if _k_upload_orig(_v) not in st.session_state:
        st.session_state[_k_upload_orig(_v)] = None
    if _k_sample(_v) not in st.session_state:
        st.session_state[_k_sample(_v)] = None
    if _k_sample_raw(_v) not in st.session_state:
        st.session_state[_k_sample_raw(_v)] = None
    if _k_file_label(_v) not in st.session_state:
        st.session_state[_k_file_label(_v)] = None
    if _k_other_sql(_v) not in st.session_state:
        st.session_state[_k_other_sql(_v)] = None

main_view = st.session_state.main_view

_render_app_hero(main_view)

# ── Access guard ────────────────────────────────────────────────────────────
_cur_is_admin = st.session_state.get("auth_role") == "admin"
_cur_pages = st.session_state.get("auth_pages", [])

if main_view == "Admin":
    if not _cur_is_admin:
        st.error("You do not have permission to access the **Admin** panel.")
        st.stop()
    render_admin_panel()
    st.stop()
elif main_view not in _cur_pages:
    _accessible = list(_cur_pages) + (["Admin"] if _cur_is_admin else [])
    if _accessible:
        st.warning(
            f"You do not have access to **{main_view}**. "
            f"Accessible pages: {', '.join(_accessible)}."
        )
    else:
        st.warning("Your account has no pages assigned. Contact an admin.")
    st.stop()

if db_type == "BigQuery":
    st.sidebar.subheader("BigQuery auth")
    # Prefer auto-detecting project from credentials / gcloud config.
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
    show_project_override = st.sidebar.checkbox("Override GCP Project ID (advanced)", value=False)

    sa_file = st.sidebar.file_uploader(
        "Service account JSON (optional)",
        type=["json"],
        help="If you don't upload this, the app will use Application Default Credentials (e.g., GOOGLE_APPLICATION_CREDENTIALS).",
    )
    service_account_info_json = None
    if sa_file is not None:
        try:
            service_account_info_json = sa_file.getvalue().decode("utf-8")
            json.loads(service_account_info_json)  # validate early for clearer errors
            st.sidebar.success("Service account JSON loaded.")
        except Exception as e:
            st.sidebar.error(f"Invalid JSON: {e}")
            service_account_info_json = None

    # Auto-detect project if not provided via env/override.
    try:
        if not project_id:
            project_id = get_bigquery_client("", service_account_info_json).project
    except Exception:
        # We'll validate later and show a clear error.
        pass

    if show_project_override:
        project_id = st.sidebar.text_input("GCP Project ID", value=project_id).strip()
    else:
        if project_id:
            st.sidebar.caption(f"Project: `{project_id}`")
        else:
            st.sidebar.warning("Could not auto-detect project. Enable override to enter it manually.")

    # Query jobs run under this project (needs bigquery.jobs.create).
    query_project_id = st.sidebar.text_input(
        "Query Runner Project ID",
        value=os.getenv("BQ_QUERY_PROJECT_ID", project_id or "hyperlocal-team"),
        help="BigQuery query jobs are created in this project. Use one where you have bigquery.jobs.create.",
    ).strip()
    chat_mode = st.sidebar.radio(
        "Chat mode",
        ["SQL mode (needs bigquery.jobs.create)", "No-job sample mode"],
        index=0,
        help="No-job mode reads sample rows directly and chats on that sample only.",
    )
    if st.sidebar.button("Test BigQuery Permission"):
        with st.spinner("Testing BigQuery job permission..."):
            ok, msg = test_bigquery_permissions(query_project_id, service_account_info_json)
        if ok:
            st.sidebar.success(msg)
        else:
            if "bigquery.jobs.create" in msg:
                st.sidebar.error(
                    "Permission missing: bigquery.jobs.create is not granted in this Query Runner Project ID. "
                    "Use a different runner project (for example: hyperlocal-team) or ask admin for BigQuery Job User role."
                )
            else:
                st.sidebar.error(f"Permission test failed: {msg}")

    st.sidebar.subheader("Default table for chat")
    manual_table_entry = st.sidebar.checkbox("Type dataset/table manually", value=False)

    default_dataset_id = os.getenv("BQ_DATASET_ID", "")
    default_table_id = os.getenv("BQ_TABLE_ID", "")

    if manual_table_entry or not project_id:
        default_dataset_id = st.sidebar.text_input("Dataset ID", value=default_dataset_id)
        default_table_id = st.sidebar.text_input("Table ID", value=default_table_id)
    else:
        try:
            datasets = list_bq_datasets(project_id, service_account_info_json)
            if not datasets:
                st.sidebar.warning("No datasets found (or you don't have permission to list).")
                default_dataset_id = st.sidebar.text_input("Dataset ID", value=default_dataset_id)
                default_table_id = st.sidebar.text_input("Table ID", value=default_table_id)
            else:
                ds_index = datasets.index(default_dataset_id) if default_dataset_id in datasets else 0
                default_dataset_id = st.sidebar.selectbox("Dataset", datasets, index=ds_index)

                tables = list_bq_tables(project_id, default_dataset_id, service_account_info_json)
                if not tables:
                    st.sidebar.warning("No tables found in this dataset (or you don't have permission to list).")
                    default_table_id = st.sidebar.text_input("Table ID", value=default_table_id)
                else:
                    table_filter = st.sidebar.text_input("Filter tables", value="", placeholder="type to filter…")
                    filtered_tables = (
                        [t for t in tables if table_filter.lower() in t.lower()] if table_filter else tables
                    )
                    if not filtered_tables:
                        st.sidebar.warning("No tables match your filter.")
                        filtered_tables = tables

                    tb_index = tables.index(default_table_id) if default_table_id in tables else 0
                    # If a filter is applied, keep the selection stable when possible.
                    if default_table_id in filtered_tables:
                        tb_index = filtered_tables.index(default_table_id)
                    else:
                        tb_index = 0
                    default_table_id = st.sidebar.selectbox("Table", filtered_tables, index=tb_index)
        except Exception as e:
            st.sidebar.warning(f"Could not list datasets/tables: {e}")
            default_dataset_id = st.sidebar.text_input("Dataset ID", value=default_dataset_id)
            default_table_id = st.sidebar.text_input("Table ID", value=default_table_id)

    if project_id and default_dataset_id and default_table_id:
        st.sidebar.caption(f"Using: `{project_id}.{default_dataset_id}.{default_table_id}`")
        with st.sidebar.expander("Preview schema"):
            try:
                client = get_bigquery_client(project_id, service_account_info_json)
                table_ref = f"{project_id}.{default_dataset_id}.{default_table_id}"
                table = client.get_table(table_ref)
                st.code(_schema_to_text(table))
            except Exception as e:
                st.sidebar.warning(f"Could not load schema: {e}")

    if chat_mode == "No-job sample mode":
        st.sidebar.subheader("No-job sample settings")
        st.sidebar.caption(
            "This is still your **live** table — new rows land in BigQuery as usual. "
            "Here we **do not** run SQL, so we only pull the **next N rows as stored** (not “latest by time”). "
            "Use **SQL mode** to query by date / “most recent” across the full table."
        )
        sample_rows = st.sidebar.slider(
            "Sample rows (storage order, not date-sorted)",
            min_value=100,
            max_value=100000,
            value=5000,
            step=500,
            help="Larger values use more RAM. Full-table date filters need SQL mode + job permission.",
        )
        if st.sidebar.button("Load sample data"):
            try:
                table_ref = f"{project_id}.{default_dataset_id}.{default_table_id}"
                with st.spinner("Loading sample rows from table..."):
                    sample_df = load_bq_table_sample(
                        table_ref=table_ref,
                        project_id=project_id,
                        service_account_info_json=service_account_info_json,
                        row_limit=sample_rows,
                    )
                st.session_state[_k_sample_raw(main_view)] = sample_df
                st.session_state[_k_sample(main_view)] = sample_df
                st.session_state[_k_last(main_view)] = sample_df
                st.sidebar.success(
                    f"Loaded {len(sample_df):,} sample rows into **{main_view}** tab."
                )
            except Exception as e:
                st.sidebar.error(f"Could not load sample data: {e}")

        if st.session_state.get(_k_sample_raw(main_view)) is not None:
            raw = st.session_state[_k_sample_raw(main_view)]
            with st.sidebar.expander("Date range on loaded sample", expanded=False):
                st.caption(
                    "Filters rows **in memory** on whatever you loaded. "
                    "It does **not** scan the full table by date in BigQuery (that needs SQL + job permission)."
                )
                date_cols = [c for c in raw.columns if pd.api.types.is_datetime64_any_dtype(raw[c])]
                if not date_cols:
                    for c in raw.columns:
                        t = pd.to_datetime(raw[c].head(500), errors="coerce")
                        if t.notna().any():
                            date_cols.append(c)
                if not date_cols:
                    date_cols = list(raw.columns)
                _nvk = _tab_suf(main_view)
                date_col = st.selectbox(
                    "Date / time column", date_cols, key=f"nojob_date_col_{_nvk}"
                )
                st.info(sample_column_datetime_summary(raw, date_col))
                st.caption(
                    "If you pick a date range and get **0 rows**, your range does not overlap the dates "
                    "that happen to appear in this sample (rows are not loaded in date order). "
                    "Reload a larger sample or use **SQL mode** to filter the full table by date."
                )
                c1, c2 = st.columns(2)
                with c1:
                    d_start = st.date_input("From", value=None, key=f"nojob_d_start_{_nvk}")
                with c2:
                    d_end = st.date_input("To", value=None, key=f"nojob_d_end_{_nvk}")
                if st.button("Apply date filter", key=f"nojob_apply_date_{_nvk}"):
                    try:
                        start_d = d_start if d_start else None
                        end_d = d_end if d_end else None
                        if start_d is None and end_d is None:
                            st.session_state[_k_sample(main_view)] = raw.copy()
                            st.session_state[_k_last(main_view)] = st.session_state[_k_sample(main_view)]
                            st.sidebar.info("No date range set — using full loaded sample.")
                        else:
                            filt = filter_dataframe_by_date_range(raw, date_col, start_d, end_d)
                            st.session_state[_k_sample(main_view)] = filt
                            st.session_state[_k_last(main_view)] = filt
                            if len(filt) == 0:
                                st.sidebar.warning(
                                    f"**0 rows** in range — none of the **{len(raw):,}** loaded rows have "
                                    f"`{date_col}` on that day in UTC. See min/max above, or use SQL mode for full-table date filter."
                                )
                            else:
                                st.sidebar.success(
                                    f"Filtered to **{len(filt):,}** rows (from {len(raw):,} loaded)."
                                )
                    except Exception as e:
                        st.sidebar.error(str(e))
                if st.button("Reset to full loaded sample", key=f"nojob_reset_date_{_nvk}"):
                    st.session_state[_k_sample(main_view)] = raw.copy()
                    st.session_state[_k_last(main_view)] = st.session_state[_k_sample(main_view)]
                    st.sidebar.success("Reset to full loaded sample.")

    try:
        if project_id:
            _ = get_bigquery_client(project_id, service_account_info_json)
    except Exception as e:
        st.sidebar.error(f"BigQuery auth error: {e}")

    if main_view == "AI Chat":
        if chat_mode == "No-job sample mode":
            st.markdown(
                "No-job sample mode: **Load sample data** first, then optionally use **Date range on loaded sample** "
                "to keep only rows in a date window (pandas filter on what you loaded). "
                "For **full warehouse data** filtered by date, use **SQL mode** with a `WHERE` on your date column "
                "(needs `bigquery.jobs.create`)."
            )
        else:
            st.markdown(
                "Ask questions in chat. The assistant will generate **BigQuery SQL**, run it, and summarize results. "
                "Tip: start your message with `sql:` to run raw SQL directly."
            )
    
        _panel_df = st.session_state.get(_k_last("AI Chat"))
        if _panel_df is None and chat_mode == "No-job sample mode":
            _panel_df = st.session_state.get(_k_sample("AI Chat"))
        if isinstance(_panel_df, pd.DataFrame) and not _panel_df.empty:
            render_loaded_data_panel(
                _panel_df,
                key_prefix="bq",
                ai_provider=ai_provider,
                gemini_api_key=gemini_api_key,
                gemini_model_name=gemini_model_name,
                ollama_base_url=ollama_base_url,
                ollama_model=ollama_model,
            )
    
        for m in st.session_state.messages:
            with st.chat_message(m["role"]):
                st.markdown(m["content"])
                if m.get("sql"):
                    with st.expander("SQL used"):
                        st.code(m["sql"], language="sql")
                if m.get("df_preview") is not None:
                    st.dataframe(m["df_preview"], use_container_width=True)
    
        chat_disabled = not bool(project_id and default_dataset_id and default_table_id)
        if chat_mode == "No-job sample mode" and st.session_state.get(_k_sample("AI Chat")) is None:
            chat_disabled = True
    
        user_text = st.chat_input(
            "Ask about your data… (e.g., 'last 30 days PnL by city')",
            disabled=chat_disabled,
        )
    
        def _make_tool_prompt(question: str, table_ref: str, schema_text: str) -> str:
            return textwrap.dedent(
                f"""
                You are a data assistant connected to BigQuery.
    
                You can choose one action:
                - run_sql: provide Standard SQL to execute in BigQuery.
                - answer: if no query is needed, answer directly.
    
                Return ONLY valid JSON with this exact shape:
                {{
                  "action": "run_sql" | "answer",
                  "sql": "<SQL or empty string>",
                  "final": "<what you will say to the user>"
                }}
    
                BigQuery table you MUST use for queries:
                `{table_ref}`
    
                Table schema:
                {schema_text}
    
                Rules:
                - If the user asks anything about data, choose action=run_sql.
                - Use BigQuery Standard SQL.
                - Prefer explicit columns (avoid SELECT *).
                - Add a reasonable LIMIT if returning raw rows.
                - If a date/time column exists and user says "recent/last", add a filter (30/90 days).
                - Never include markdown fences. JSON only.
    
                User message:
                {question}
                """
            ).strip()
    
        if user_text:
            st.session_state.messages.append({"role": "user", "content": user_text})
    
            table_ref = f"{project_id}.{default_dataset_id}.{default_table_id}"
            assistant_content = ""
            assistant_sql = ""
            df_preview = None
    
            with st.chat_message("assistant"):
                try:
                    client = get_bigquery_client(project_id, service_account_info_json)
    
                    if chat_mode == "No-job sample mode":
                        sample_df = st.session_state.get(_k_sample("AI Chat"))
                        if sample_df is None or sample_df.empty:
                            raise ValueError(
                                "No sample loaded for **AI Chat**. Switch to **AI Chat**, then click **Load sample data**."
                            )
                        if user_text.strip().lower().startswith("sql:"):
                            st.warning("SQL execution is disabled in No-job sample mode.")
                            assistant_content = "SQL execution is disabled in No-job sample mode."
                        else:
                            sample_cols = ", ".join([str(c) for c in sample_df.columns])
                            sample_preview = sample_df.head(200).to_string(index=False)
                            stats_preview = sample_df.describe(include="all").fillna("").to_string()
                            sample_prompt = textwrap.dedent(
                                f"""
                                You are a data analyst. Answer ONLY from this sample dataset.
                                If the sample is not enough for certainty, clearly say it is sample-based.
    
                                Columns:
                                {sample_cols}
    
                                Sample stats:
                                {stats_preview}
    
                                Sample rows:
                                {sample_preview}
    
                                User question:
                                {user_text}
                                """
                            ).strip()
                            with st.spinner("Analyzing sample data..."):
                                assistant_content = _call_ai_text(sample_prompt)
                            st.markdown(assistant_content or "OK.")
                            df_preview = sample_df.head(500)
                            st.dataframe(df_preview, use_container_width=True)
                            st.session_state[_k_last("AI Chat")] = sample_df
                            st.session_state.last_sql = None
    
                    elif user_text.strip().lower().startswith("sql:"):
                        assistant_sql = user_text.split(":", 1)[1].strip()
                        with st.spinner("Running SQL in BigQuery..."):
                            data = query_bigquery(assistant_sql, query_project_id, service_account_info_json)
                        st.session_state[_k_last("AI Chat")] = data
                        st.session_state.last_sql = assistant_sql
                        assistant_content = f"Done. Returned **{len(data):,}** rows."
                        st.markdown(assistant_content)
                        df_preview = data.head(500)
                        st.dataframe(df_preview, use_container_width=True)
                    else:
                        table = client.get_table(table_ref)
                        schema_text = _schema_to_text(table)
    
                        prompt = _make_tool_prompt(user_text, table_ref, schema_text)
    
                        with st.spinner("Thinking…"):
                            raw = call_ai_text_unified(
                                prompt,
                                ai_provider,
                                gemini_api_key,
                                gemini_model_name,
                                ollama_base_url,
                                ollama_model,
                            )
    
                        plan = extract_first_json_object(raw)
    
                        action = plan.get("action")
                        assistant_content = (plan.get("final") or "").strip()
                        assistant_sql = (plan.get("sql") or "").strip()
    
                        if action == "run_sql":
                            if not assistant_sql:
                                raise ValueError("AI chose run_sql but returned empty SQL.")
                            with st.spinner("Running query in BigQuery…"):
                                data = query_bigquery(assistant_sql, query_project_id, service_account_info_json)
                            st.session_state[_k_last("AI Chat")] = data
                            st.session_state.last_sql = assistant_sql
    
                            st.markdown(assistant_content or f"Returned **{len(data):,}** rows.")
                            with st.expander("SQL used"):
                                st.code(assistant_sql, language="sql")
                            df_preview = data.head(500)
                            st.dataframe(df_preview, use_container_width=True)
                        else:
                            st.markdown(assistant_content or "OK.")
    
                except Exception as e:
                    err_text = str(e)
                    if "bigquery.jobs.create" in err_text:
                        st.error(
                            "Permission error: you cannot create BigQuery jobs in the current query project. "
                            "Set 'Query Runner Project ID' to a project where your account has bigquery.jobs.create "
                            "(for example: hyperlocal-team)."
                        )
                    else:
                        st.error(f"Error: {e}")
                    assistant_content = f"Error: {e}"
    
            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "content": assistant_content or "OK.",
                    "sql": assistant_sql or None,
                    "df_preview": df_preview,
                }
            )
    
    if main_view == "Dashboard":
        render_dashboard_tab(
            db_type,
            None,
            ai_provider=ai_provider,
            gemini_api_key=gemini_api_key,
            gemini_model_name=gemini_model_name,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )
    elif main_view == "PNL":
        render_pnl_tab(
            db_type,
            None,
            ai_provider=ai_provider,
            gemini_api_key=gemini_api_key,
            gemini_model_name=gemini_model_name,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )

elif db_type == "Upload CSV / Excel":
    st.sidebar.subheader("Upload CSV / Excel")
    st.sidebar.caption(f"File loads into the **{main_view}** tab only.")
    _uk = _tab_suf(main_view)
    uploaded = st.sidebar.file_uploader(
        "Choose file", type=["csv", "xlsx", "xls"], key=f"local_file_upload_{_uk}"
    )
    if st.sidebar.button("Load file into app", key=f"btn_upload_file_{_uk}") and uploaded is not None:
        try:
            df_up = read_uploaded_table_file(uploaded)
            st.session_state[_k_upload_orig(main_view)] = df_up.copy()
            st.session_state[_k_last(main_view)] = df_up
            st.session_state[_k_sample(main_view)] = df_up
            st.session_state[_k_sample_raw(main_view)] = df_up.copy()
            st.session_state[_k_file_label(main_view)] = uploaded.name
            st.sidebar.success(
                f"Loaded **{len(df_up):,}** rows from `{uploaded.name}` into **{main_view}**."
            )
        except Exception as e:
            st.sidebar.error(str(e))

    # No reset button needed — SQL results are preview-only and never replace the loaded data.

    if main_view == "AI Chat":
        _chat_ldf = st.session_state.get(_k_last("AI Chat"))
        _has_data = isinstance(_chat_ldf, pd.DataFrame) and not _chat_ldf.empty

        if _has_data:
            render_loaded_data_panel(
                _chat_ldf,
                key_prefix="upload",
                ai_provider=ai_provider,
                gemini_api_key=gemini_api_key,
                gemini_model_name=gemini_model_name,
                ollama_base_url=ollama_base_url,
                ollama_model=ollama_model,
            )
            st.caption("💡 Data loaded — ask questions about your data, or type `sql:` to run a query. Or just chat freely.")
        else:
            st.caption("💬 Ask me anything — load a file from the sidebar to also analyse data.")

        # Render chat history
        for m in st.session_state.messages:
            with st.chat_message(m["role"]):
                st.markdown(m["content"])
                if m.get("sql"):
                    with st.expander("SQL used"):
                        st.code(m["sql"], language="sql")
                if m.get("df_preview") is not None:
                    st.dataframe(m["df_preview"], use_container_width=True)

        ut = st.chat_input(
            "Ask about your data… (e.g., 'last 30 days PnL by city')",
            key="chat_upload",
        )
        if ut:
            st.session_state.messages.append({"role": "user", "content": ut})
            work = st.session_state.get(_k_last("AI Chat"))
            _uo = st.session_state.get(_k_upload_orig("AI Chat"))
            base_df = _uo if _uo is not None else work
            assistant_content = ""
            df_preview = None
            sql_used = None
            extracted_sql = None
            with st.chat_message("assistant"):
                try:
                    # ── SQL shortcut: user typed sql: directly ──────────────
                    if ut.strip().lower().startswith("sql:") and base_df is not None:
                        sql_used = ut.split(":", 1)[1].strip()
                        with st.spinner("Running SQL…"):
                            result_df = run_sql_on_dataframe(base_df, sql_used)
                        assistant_content = f"SQL returned **{len(result_df):,}** rows (preview — base data unchanged)."
                        st.code(sql_used, language="sql")
                        df_preview = result_df
                        st.dataframe(df_preview, use_container_width=True)

                    # ── Data loaded: ask AI about the data ─────────────────
                    elif base_df is not None and not base_df.empty:
                        sample_cols = ", ".join([str(c) for c in work.columns])
                        sample_preview = work.head(200).to_string(index=False)
                        stats_preview = work.describe(include="all").fillna("").to_string()
                        sample_prompt = textwrap.dedent(
                            f"""
                            You are a helpful data analyst assistant.
                            The user has loaded a dataset. Answer their question using the data below.
                            If a pivot/aggregation is needed, suggest a SQL query using table name `data`.

                            Columns: {sample_cols}
                            Summary stats:
                            {stats_preview}
                            Sample rows:
                            {sample_preview}

                            User question: {ut}
                            """
                        ).strip()
                        with st.spinner("Thinking…"):
                            assistant_content = call_ai_text_unified(
                                sample_prompt, ai_provider, gemini_api_key,
                                gemini_model_name, ollama_base_url, ollama_model,
                            )
                        st.markdown(assistant_content or "OK.")
                        extracted_sql = extract_sql_from_text(assistant_content)
                        if extracted_sql:
                            try:
                                with st.spinner("Running SQL from AI reply…"):
                                    result_df = run_sql_on_dataframe(base_df, extracted_sql)
                                sql_used = extracted_sql
                                assistant_content = (
                                    (assistant_content or "")
                                    + f"\n\n*Executed SQL → **{len(result_df):,}** rows (preview).*"
                                )
                                df_preview = result_df
                                st.code(extracted_sql, language="sql")
                                st.dataframe(df_preview, use_container_width=True)
                            except Exception as sql_err:
                                st.warning(f"Could not run SQL: {sql_err}")

                    # ── No data: pure conversational ChatGPT-style ─────────
                    else:
                        # Build conversation history for context
                        history_text = "\n".join(
                            f"{m['role'].capitalize()}: {m['content']}"
                            for m in st.session_state.messages[:-1]
                            if m["role"] in ("user", "assistant")
                        )
                        free_prompt = (
                            f"{history_text}\nUser: {ut}" if history_text else ut
                        )
                        with st.spinner("Thinking…"):
                            assistant_content = call_ai_text_unified(
                                free_prompt, ai_provider, gemini_api_key,
                                gemini_model_name, ollama_base_url, ollama_model,
                            )
                        st.markdown(assistant_content or "")

                except Exception as e:
                    st.error(str(e))
                    assistant_content = f"Error: {e}"

            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "content": assistant_content or "OK.",
                    "sql": sql_used,
                    "df_preview": df_preview,
                }
            )
            if rerun_after_sql:
                st.rerun()
    
    if main_view == "Dashboard":
        render_dashboard_tab(
            db_type,
            st.session_state.get(_k_other_sql("Dashboard")),
            ai_provider=ai_provider,
            gemini_api_key=gemini_api_key,
            gemini_model_name=gemini_model_name,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )
    elif main_view == "PNL":
        render_pnl_tab(
            db_type,
            st.session_state.get(_k_other_sql("PNL")),
            ai_provider=ai_provider,
            gemini_api_key=gemini_api_key,
            gemini_model_name=gemini_model_name,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )

elif db_type == "Google Sheet (CSV link)":
    st.sidebar.subheader("Google Sheet")
    st.sidebar.caption(
        "Paste a **Share** link (`.../spreadsheets/d/...`) or a CSV export URL. "
        "The sheet must open without signing in (e.g. **Anyone with the link can view**), "
        "or publish: **File → Share → Publish to web → CSV**."
    )
    st.sidebar.caption(f"Sheet loads into the **{main_view}** tab only.")
    _suk = _tab_suf(main_view)
    sheet_url = st.sidebar.text_area(
        "Sheet URL or CSV export link", height=100, key=f"sheet_url_input_{_suk}"
    )
    if st.sidebar.button("Load sheet into app", key=f"btn_sheet_load_{_suk}"):
        try:
            export_url = google_sheet_to_csv_export_url(sheet_url)
            if not export_url:
                raise ValueError("Enter a Google Sheets URL.")
            req = urlrequest.Request(
                export_url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; StreamlitDataApp/1.0)"},
            )
            with urlrequest.urlopen(req, timeout=120) as resp:
                body = resp.read()
            df_sheet = pd.read_csv(io.BytesIO(body))
            st.session_state[_k_upload_orig(main_view)] = df_sheet.copy()
            st.session_state[_k_last(main_view)] = df_sheet
            st.session_state[_k_sample(main_view)] = df_sheet
            st.session_state[_k_sample_raw(main_view)] = df_sheet.copy()
            st.session_state[_k_file_label(main_view)] = "Google Sheet"
            st.sidebar.success(
                f"Loaded **{len(df_sheet):,}** rows from sheet into **{main_view}**."
            )
        except Exception as e:
            st.sidebar.error(
                f"{e} — If the sheet is private, share as viewable link or export CSV and use Upload instead."
            )

    # No reset button needed — SQL results are preview-only and never replace the loaded data.

    if main_view == "AI Chat":
        st.markdown(
            "Load a public/published Google Sheet as CSV. Use **`sql:`** for pivot/summary (table name **`data`**). "
            "`pip install duckdb` required for SQL."
        )
    
        _gs_ldf = st.session_state.get(_k_last("AI Chat"))
        if isinstance(_gs_ldf, pd.DataFrame) and not _gs_ldf.empty:
            render_loaded_data_panel(
                _gs_ldf,
                key_prefix="gsheet",
                ai_provider=ai_provider,
                gemini_api_key=gemini_api_key,
                gemini_model_name=gemini_model_name,
                ollama_base_url=ollama_base_url,
                ollama_model=ollama_model,
            )
    
        for m in st.session_state.messages:
            with st.chat_message(m["role"]):
                st.markdown(m["content"])
                if m.get("df_preview") is not None:
                    st.dataframe(m["df_preview"], use_container_width=True)
    
        ut2 = st.chat_input(
            "Ask about your sheet…",
            disabled=st.session_state.get(_k_last("AI Chat")) is None,
            key="chat_sheet",
        )
        if ut2:
            st.session_state.messages.append({"role": "user", "content": ut2})
            work = st.session_state.get(_k_last("AI Chat"))
            _uo = st.session_state.get(_k_upload_orig("AI Chat"))
            base_df = _uo if _uo is not None else work
            assistant_content = ""
            df_preview = None
            sql_used = None
            rerun_after_sql = False
            with st.chat_message("assistant"):
                try:
                    if work is None or work.empty:
                        raise ValueError("Load a sheet first.")
                    if ut2.strip().lower().startswith("sql:"):
                        sql_used = ut2.split(":", 1)[1].strip()
                        with st.spinner("Running SQL on your data…"):
                            result_df = run_sql_on_dataframe(base_df, sql_used)
                        # Preview only — base data is never replaced
                        assistant_content = f"SQL returned **{len(result_df):,}** rows (preview below — base data unchanged)."
                        st.code(sql_used, language="sql")
                        df_preview = result_df
                        st.dataframe(df_preview, use_container_width=True)
                    else:
                        sample_cols = ", ".join([str(c) for c in work.columns])
                        sample_preview = work.head(200).to_string(index=False)
                        stats_preview = work.describe(include="all").fillna("").to_string()
                        sample_prompt = textwrap.dedent(
                            f"""
                            You are a data analyst. Answer ONLY from this dataset.
                            If the user wants a pivot or GROUP BY summary, reply with ONE line:
                            sql: SELECT ... FROM data ...;
                            Table name is `data` (columns: {sample_cols}).

                            Columns:
                            {sample_cols}

                            Summary stats:
                            {stats_preview}

                            Sample rows:
                            {sample_preview}

                            User question:
                            {ut2}
                            """
                        ).strip()
                        with st.spinner("Analyzing…"):
                            assistant_content = call_ai_text_unified(
                                sample_prompt,
                                ai_provider,
                                gemini_api_key,
                                gemini_model_name,
                                ollama_base_url,
                                ollama_model,
                            )
                        st.markdown(assistant_content or "OK.")
                        extracted_sql = extract_sql_from_text(assistant_content)
                        if extracted_sql:
                            try:
                                with st.spinner("Running SQL from AI reply…"):
                                    result_df = run_sql_on_dataframe(base_df, extracted_sql)
                                # Preview only — base data unchanged
                                sql_used = extracted_sql
                                assistant_content = (
                                    (assistant_content or "")
                                    + f"\n\n**Executed SQL** → **{len(result_df):,}** rows (preview, base data unchanged)."
                                )
                                df_preview = result_df
                                st.success(f"Executed query: **{len(result_df):,}** rows.")
                                st.code(extracted_sql, language="sql")
                                st.dataframe(df_preview, use_container_width=True)
                            except Exception as sql_err:
                                st.warning(f"Could not run SQL from reply: {sql_err}")
                                df_preview = work.head(500)
                                st.dataframe(df_preview, use_container_width=True)
                        else:
                            df_preview = work.head(500)
                            st.dataframe(df_preview, use_container_width=True)
                except Exception as e:
                    st.error(str(e))
                    assistant_content = f"Error: {e}"
            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "content": assistant_content or "OK.",
                    "sql": sql_used,
                    "df_preview": df_preview,
                }
            )
            if rerun_after_sql:
                st.rerun()
    
    if main_view == "Dashboard":
        render_dashboard_tab(
            db_type,
            st.session_state.get(_k_other_sql("Dashboard")),
            ai_provider=ai_provider,
            gemini_api_key=gemini_api_key,
            gemini_model_name=gemini_model_name,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )
    elif main_view == "PNL":
        render_pnl_tab(
            db_type,
            st.session_state.get(_k_other_sql("PNL")),
            ai_provider=ai_provider,
            gemini_api_key=gemini_api_key,
            gemini_model_name=gemini_model_name,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )

elif db_type == "Google Drive (pick a Sheet)":
    st.sidebar.subheader("Google Drive — Sheets picker")
    _gd_tab = _tab_suf(main_view)

    # ── Auth method: Google Account (OAuth2) or Service Account JSON ────────
    _gd_auth_method = st.sidebar.radio(
        "Sign in with",
        ["Google Account (OAuth2)", "Service Account JSON"],
        key=f"gdrive_auth_method_{_gd_tab}",
        horizontal=True,
    )
    st.sidebar.caption(f"Sheet loads into the **{main_view}** tab only.")

    _gd_oauth_token: Optional[str] = st.session_state.get(f"gdrive_oauth_token_{_gd_tab}")
    _gd_sa_info: Optional[dict] = None

    if _gd_auth_method == "Google Account (OAuth2)":

        # Load saved app credentials so user doesn't re-enter each time
        _saved_oauth_app = _gdrive_oauth_load_app_creds()

        if not _gd_oauth_token:
            with st.sidebar.expander("⚙️ Setup (one-time only)", expanded=not bool(_saved_oauth_app)):
                st.markdown(
                    "**One-time Google Cloud setup:**\n"
                    "1. Go to [console.cloud.google.com](https://console.cloud.google.com)\n"
                    "2. Create a project (or pick existing)\n"
                    "3. Enable **Google Drive API** + **Google Sheets API**\n"
                    "4. Go to **APIs & Services → Credentials → Create Credentials → OAuth 2.0 Client ID**\n"
                    "5. Choose **Web application**\n"
                    "6. Under *Authorized redirect URIs* add: `http://localhost:8501`\n"
                    "7. Copy the **Client ID** and **Client Secret** below and click Save"
                )
                _gd_client_id = st.text_input(
                    "Client ID",
                    value=_saved_oauth_app.get("client_id", ""),
                    key=f"gdrive_cid_{_gd_tab}",
                    placeholder="xxxx.apps.googleusercontent.com",
                )
                _gd_client_secret = st.text_input(
                    "Client Secret",
                    value=_saved_oauth_app.get("client_secret", ""),
                    key=f"gdrive_csec_{_gd_tab}",
                    type="password",
                )
                _gd_redirect = st.text_input(
                    "Redirect URI",
                    value=_saved_oauth_app.get("redirect_uri", "http://localhost:8501"),
                    key=f"gdrive_redir_{_gd_tab}",
                    help="Must exactly match what you added in Google Cloud Console.",
                )
                if st.button("💾 Save setup", key=f"gdrive_save_creds_{_gd_tab}", use_container_width=True):
                    if _gd_client_id and _gd_client_secret:
                        _gdrive_oauth_save_app_creds(_gd_client_id, _gd_client_secret, _gd_redirect)
                        st.success("Saved! Now click Sign in with Google below.")
                        st.rerun()
                    else:
                        st.error("Enter Client ID and Client Secret first.")
        else:
            _gd_client_id = _saved_oauth_app.get("client_id", "")
            _gd_client_secret = _saved_oauth_app.get("client_secret", "")
            _gd_redirect = _saved_oauth_app.get("redirect_uri", "http://localhost:8501")

        # Use saved creds if not overridden above
        if not locals().get("_gd_client_id"):
            _gd_client_id = _saved_oauth_app.get("client_id", "")
        if not locals().get("_gd_client_secret"):
            _gd_client_secret = _saved_oauth_app.get("client_secret", "")
        if not locals().get("_gd_redirect"):
            _gd_redirect = _saved_oauth_app.get("redirect_uri", "http://localhost:8501")

        if _gd_client_id and _gd_client_secret:
            # Handle OAuth callback code in URL
            _qp = st.query_params
            _oauth_code = _qp.get("code", "")
            _oauth_state = _qp.get("state", "")
            _expected_state = st.session_state.get(f"gdrive_oauth_state_{_gd_tab}", "")
            if _oauth_code and (not _expected_state or _oauth_state == _expected_state):
                if not _gd_oauth_token:
                    try:
                        with st.spinner("Connecting to Google…"):
                            _tok = _gdrive_oauth_exchange_code(
                                _gd_client_id, _gd_client_secret, _gd_redirect, _oauth_code
                            )
                        _gd_oauth_token = _tok.get("access_token", "")
                        st.session_state[f"gdrive_oauth_token_{_gd_tab}"] = _gd_oauth_token
                        st.query_params.clear()
                        st.sidebar.success("✅ Google account connected!")
                        st.rerun()
                    except Exception as _oe:
                        st.sidebar.error(f"Sign-in failed: {_oe}")

            if not _gd_oauth_token:
                _auth_url, _state = _gdrive_oauth_auth_url(_gd_client_id, _gd_client_secret, _gd_redirect)
                st.session_state[f"gdrive_oauth_state_{_gd_tab}"] = _state
                st.sidebar.link_button(
                    "🔑  Sign in with Google",
                    _auth_url,
                    use_container_width=True,
                    type="primary",
                )
                st.sidebar.caption("Opens Google sign-in → approve access → returns here automatically.")
            else:
                st.sidebar.success("✅ Signed in with Google")
                if st.sidebar.button("🔓 Disconnect", key=f"btn_gdrive_disco_{_gd_tab}"):
                    st.session_state.pop(f"gdrive_oauth_token_{_gd_tab}", None)
                    st.rerun()
        else:
            st.sidebar.info("Complete the ⚙️ Setup above first.")

    else:
        # ── Service Account JSON path ────────────────────────────────────────
        _gd_sa_file = st.sidebar.file_uploader(
            "Service account JSON",
            type=["json"],
            key=f"gdrive_sa_{_gd_tab}",
        )
        if _gd_sa_file:
            try:
                _gd_sa_info = json.load(_gd_sa_file)
                st.sidebar.caption(f"Account: `{_gd_sa_info.get('client_email','?')}`")
            except Exception as _gd_e:
                st.sidebar.error(f"Bad JSON: {_gd_e}")
                _gd_sa_info = None

    # ── Sheets listing (works for both auth methods) ─────────────────────────
    _gd_ready = bool(_gd_oauth_token) or bool(_gd_sa_info)
    if _gd_ready:
        if st.sidebar.button("🔄  List my Google Sheets", key=f"btn_gdrive_list_{_gd_tab}", use_container_width=True):
            try:
                with st.spinner("Fetching sheet list from Drive…"):
                    if _gd_oauth_token:
                        _sheets = _gdrive_list_sheets_oauth(_gd_oauth_token)
                    else:
                        _sheets = _gdrive_list_sheets(_gd_sa_info)  # type: ignore[arg-type]
                st.session_state[f"gdrive_sheets_{_gd_tab}"] = _sheets
                if not _sheets:
                    st.sidebar.warning("No sheets found. Make sure you have at least one Google Sheet in your Drive.")
            except Exception as _gd_e:
                st.sidebar.error(str(_gd_e))

        _gdrive_sheets = st.session_state.get(f"gdrive_sheets_{_gd_tab}", [])
        if _gdrive_sheets:
            _sheet_names = [s["name"] for s in _gdrive_sheets]
            _picked_name = st.sidebar.selectbox(
                "Select a Google Sheet",
                _sheet_names,
                key=f"gdrive_pick_sheet_{_gd_tab}",
            )
            _picked_file = next((s for s in _gdrive_sheets if s["name"] == _picked_name), None)
            if _picked_file:
                if st.sidebar.button("Load tabs", key=f"btn_gdrive_tabs_{_gd_tab}", use_container_width=True):
                    try:
                        with st.spinner(f"Loading tabs for '{_picked_name}'…"):
                            if _gd_oauth_token:
                                _tabs = _gdrive_sheet_tabs_oauth(_gd_oauth_token, _picked_file["id"])
                            else:
                                _tabs = _gdrive_sheet_tabs(_gd_sa_info, _picked_file["id"])  # type: ignore[arg-type]
                        st.session_state[f"gdrive_tabs_{_gd_tab}"] = _tabs
                    except Exception as _te:
                        st.sidebar.error(str(_te))

                _gdrive_tabs = st.session_state.get(f"gdrive_tabs_{_gd_tab}", [])
                if _gdrive_tabs:
                    _tab_titles = [t["title"] for t in _gdrive_tabs]
                    _picked_tab = st.sidebar.selectbox(
                        "Select worksheet tab",
                        _tab_titles,
                        key=f"gdrive_pick_tab_{_gd_tab}",
                    )
                    if st.sidebar.button(
                        f"📥  Load '{_picked_tab}' into {main_view}",
                        type="primary",
                        key=f"btn_gdrive_load_{_gd_tab}",
                        use_container_width=True,
                    ):
                        try:
                            with st.spinner(f"Loading '{_picked_tab}' from '{_picked_name}'…"):
                                if _gd_oauth_token:
                                    _gd_df = _gdrive_load_sheet_tab_oauth(
                                        _gd_oauth_token, _picked_file["id"], _picked_tab
                                    )
                                else:
                                    _gd_df = _gdrive_load_sheet_tab(
                                        _gd_sa_info, _picked_file["id"], _picked_tab  # type: ignore[arg-type]
                                    )
                            if _gd_df.empty:
                                st.sidebar.warning("Sheet/tab appears empty.")
                            else:
                                st.session_state[_k_upload_orig(main_view)] = _gd_df.copy()
                                st.session_state[_k_last(main_view)] = _gd_df
                                st.session_state[_k_sample(main_view)] = _gd_df
                                st.session_state[_k_sample_raw(main_view)] = _gd_df.copy()
                                st.session_state[_k_file_label(main_view)] = f"{_picked_name} › {_picked_tab}"
                                st.sidebar.success(
                                    f"Loaded **{len(_gd_df):,}** rows · **{len(_gd_df.columns)}** cols "
                                    f"from **{_picked_name} › {_picked_tab}** into **{main_view}**."
                                )
                        except Exception as _le:
                            st.sidebar.error(str(_le))

    if main_view == "Dashboard":
        render_dashboard_tab(
            db_type,
            st.session_state.get(_k_last("Dashboard")),
            ai_provider=ai_provider,
            gemini_api_key=gemini_api_key,
            gemini_model_name=gemini_model_name,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )
    elif main_view == "PNL":
        render_pnl_tab(
            db_type,
            st.session_state.get(_k_last("PNL")),
            ai_provider=ai_provider,
            gemini_api_key=gemini_api_key,
            gemini_model_name=gemini_model_name,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )
    elif main_view == "AI Chat":
        _gd_chat_df = st.session_state.get(_k_last("AI Chat"))
        if isinstance(_gd_chat_df, pd.DataFrame) and not _gd_chat_df.empty:
            render_loaded_data_panel(
                _gd_chat_df,
                key_prefix="gdrive_chat",
                ai_provider=ai_provider,
                gemini_api_key=gemini_api_key,
                gemini_model_name=gemini_model_name,
                ollama_base_url=ollama_base_url,
                ollama_model=ollama_model,
            )
        else:
            st.info("Upload a service-account JSON, list sheets, pick one, select a tab, and click **Load**.")

elif db_type == "Apps Script (Private Sheet)":
    st.sidebar.caption(f"Fetches into the **{main_view}** tab.")
    _as_k = _tab_suf(main_view)
    _as_saved_sources = _ascript_sources_load()

    if _is_admin:
        # ── ADMIN: full configuration panel ──────────────────────────────
        with st.sidebar.expander("📋 How to set up (one time)", expanded=False):
            st.markdown(
                "1. Open your Google Sheet\n"
                "2. **Extensions → Apps Script**\n"
                "3. Paste the script shown on the main screen\n"
                "4. **Deploy → New deployment** → Web app\n"
                "   - Execute as: **Me** | Who has access: **Anyone**\n"
                "5. Copy the Web App URL\n"
                "6. Save it in **Admin → 📄 Apps Script Data Sources** with a name\n"
                "   — users will only see the name, never the URL"
            )

        # Admin can pick a saved source OR paste a URL directly
        _as_url = ""
        if _as_saved_sources:
            _admin_src_options = ["— paste URL directly —"] + [s["name"] for s in _as_saved_sources]
            _admin_src_sel = st.sidebar.selectbox("Saved source", _admin_src_options, key=f"as_admin_sel_{_as_k}")
            if _admin_src_sel != "— paste URL directly —":
                _matched_src = next((s for s in _as_saved_sources if s["name"] == _admin_src_sel), None)
                if _matched_src:
                    _as_url = _matched_src["url"]

        if not _as_url:
            _as_url = st.sidebar.text_input(
                "Web App URL (admin only)",
                key=f"appscript_url_{_as_k}",
                placeholder="https://script.google.com/macros/s/…/exec",
            )

        _as_sheets: List[str] = st.session_state.get(f"appscript_sheets_{_as_k}", [])
        if _as_url:
            if st.sidebar.button("🔄 List tabs", key=f"btn_as_list_{_as_k}", use_container_width=True):
                try:
                    with st.spinner("Connecting…"):
                        _as_sheets = _appscript_list_sheets(_as_url)
                    st.session_state[f"appscript_sheets_{_as_k}"] = _as_sheets
                except Exception as _ae:
                    st.sidebar.error(f"Error: {_ae}")
            if _as_sheets:
                _as_tab = st.sidebar.selectbox("Tab", _as_sheets, key=f"appscript_tab_{_as_k}")
                if st.sidebar.button("⬇️ Load data", key=f"btn_as_load_{_as_k}", use_container_width=True):
                    try:
                        with st.spinner(f"Loading '{_as_tab}'…"):
                            _as_df = _appscript_fetch(_as_url, _as_tab)
                        if _as_df.empty:
                            st.sidebar.warning("Sheet appears empty.")
                        else:
                            st.session_state[_k_last(main_view)] = _as_df
                            st.session_state[_k_upload_orig(main_view)] = _as_df.copy()
                            st.session_state[_k_sample(main_view)] = _as_df
                            st.session_state[_k_sample_raw(main_view)] = _as_df.copy()
                            st.session_state[_k_file_label(main_view)] = f"{_admin_src_sel if _admin_src_sel != '— paste URL directly —' else 'Apps Script'} › {_as_tab}"
                            st.sidebar.success(f"Loaded **{len(_as_df):,}** rows")
                    except Exception as _ae:
                        st.sidebar.error(f"Load failed: {_ae}")

    else:
        # ── REGULAR USER: only sees saved sources by name, no URLs ───────
        _cur_user_id = st.session_state.get("auth_user", "")
        if not _as_saved_sources:
            st.sidebar.warning("No data sources available. Ask your admin to add one.")
        else:
            _user_src_names = [s["name"] for s in _as_saved_sources]
            _user_src_sel = st.sidebar.selectbox("Data source", _user_src_names, key=f"as_user_sel_{_as_k}")
            _user_src = next((s for s in _as_saved_sources if s["name"] == _user_src_sel), None)
            _as_url = _user_src["url"] if _user_src else ""
            _as_sheets_u: List[str] = st.session_state.get(f"appscript_sheets_{_as_k}", [])

            if _as_url:
                if st.sidebar.button("🔄 List tabs", key=f"btn_as_list_u_{_as_k}", use_container_width=True):
                    try:
                        with st.spinner("Connecting…"):
                            _all_tabs_u = _appscript_list_sheets(_as_url)
                        # Filter to only permitted tabs for this user
                        _allowed_u = _ascript_get_allowed_tabs(
                            _user_src, _cur_user_id, False, _all_tabs_u
                        )
                        st.session_state[f"appscript_sheets_{_as_k}"] = _allowed_u
                        _as_sheets_u = _allowed_u
                        if not _allowed_u:
                            st.sidebar.warning("You don't have access to any tab in this source. Contact admin.")
                    except Exception as _ae:
                        st.sidebar.error(f"Error: {_ae}")

                if _as_sheets_u:
                    _as_tab_u = st.sidebar.selectbox("Tab", _as_sheets_u, key=f"appscript_tab_u_{_as_k}")
                    if st.sidebar.button("⬇️ Load data", key=f"btn_as_load_u_{_as_k}", use_container_width=True, type="primary"):
                        try:
                            with st.spinner("Loading…"):
                                _as_df_u = _appscript_fetch(_as_url, _as_tab_u)
                            if _as_df_u.empty:
                                st.sidebar.warning("Sheet appears empty.")
                            else:
                                st.session_state[_k_last(main_view)] = _as_df_u
                                st.session_state[_k_upload_orig(main_view)] = _as_df_u.copy()
                                st.session_state[_k_sample(main_view)] = _as_df_u
                                st.session_state[_k_sample_raw(main_view)] = _as_df_u.copy()
                                st.session_state[_k_file_label(main_view)] = f"{_user_src_sel} › {_as_tab_u}"
                                st.sidebar.success(f"Loaded **{len(_as_df_u):,}** rows")
                        except Exception as _ae:
                            st.sidebar.error(f"Load failed: {_ae}")

    # ── Main area ─────────────────────────────────────────────────────────
    if main_view in ("Dashboard", "PNL", "AI Chat"):
        _as_loaded = st.session_state.get(_k_last(main_view))
        if _as_loaded is None or (isinstance(_as_loaded, pd.DataFrame) and _as_loaded.empty):
            if _is_admin:
                st.info("👈 Select a saved source or paste a URL in the sidebar, list tabs and load data.")
                st.subheader("📋 Apps Script code — paste this into your Google Sheet")
                st.code(_APPS_SCRIPT_CODE, language="javascript")
            else:
                st.info("👈 Select a data source and click **Load data** in the sidebar.")
        else:
            if main_view == "Dashboard":
                render_dashboard_tab(db_type, _as_loaded, ai_provider=ai_provider,
                    gemini_api_key=gemini_api_key, gemini_model_name=gemini_model_name,
                    ollama_base_url=ollama_base_url, ollama_model=ollama_model)
            elif main_view == "PNL":
                render_pnl_tab(db_type, _as_loaded, ai_provider=ai_provider,
                    gemini_api_key=gemini_api_key, gemini_model_name=gemini_model_name,
                    ollama_base_url=ollama_base_url, ollama_model=ollama_model)
            elif main_view == "AI Chat":
                render_loaded_data_panel(_as_loaded, key_prefix="appscript",
                    ai_provider=ai_provider, gemini_api_key=gemini_api_key,
                    gemini_model_name=gemini_model_name, ollama_base_url=ollama_base_url,
                    ollama_model=ollama_model)

elif db_type == "Other SQL (Postgres/MySQL)":
    st.sidebar.caption(f"Fetch applies to the **{main_view}** tab only.")
    _osqlk = _tab_suf(main_view)
    conn_str = st.sidebar.text_input(
        "Connection String (e.g., postgresql://user:pass@localhost/db)",
        key=f"conn_str_{_osqlk}",
    )
    sql_query = st.sidebar.text_area("Enter SQL Query", key=f"sql_query_{_osqlk}")
    if st.sidebar.button("Fetch SQL Data", key=f"btn_fetch_sql_{_osqlk}") and conn_str and sql_query:
        with st.spinner("Fetching data..."):
            st.session_state[_k_other_sql(main_view)] = query_sql_db(conn_str, sql_query)

    _sql_chat_df = st.session_state.get(_k_other_sql("AI Chat"))

    if main_view == "AI Chat":
        if _sql_chat_df is not None:
            st.success("Data loaded successfully!")
            with st.expander("View Raw Data"):
                st.dataframe(_sql_chat_df)
        else:
            st.write("👈 Enter connection string and SQL in the sidebar, then **Fetch SQL Data**.")

    if main_view == "Dashboard":
        render_dashboard_tab(
            db_type,
            st.session_state.get(_k_other_sql("Dashboard")),
            ai_provider=ai_provider,
            gemini_api_key=gemini_api_key,
            gemini_model_name=gemini_model_name,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )
    elif main_view == "PNL":
        render_pnl_tab(
            db_type,
            st.session_state.get(_k_other_sql("PNL")),
            ai_provider=ai_provider,
            gemini_api_key=gemini_api_key,
            gemini_model_name=gemini_model_name,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )
