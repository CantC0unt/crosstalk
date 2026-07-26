"""Observer dashboard implementation boundary."""

import argparse
import calendar
import html
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
import os
import sqlite3
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import sys
import webbrowser
import secrets
import queue
import threading
import time
from typing import List, Optional
from pathlib import Path
from urllib.parse import parse_qs, urlparse


DEFAULT_POLL_INTERVAL_SECONDS = 0.5


@dataclass(frozen=True)
class ObserverOptions:
    """Validated command-line configuration for the local observer."""

    silent: bool
    port: Optional[int]
    poll_interval: float
    groups_dir: Optional[str]


def _port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("port must be an integer between 1 and 65535") from error
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be an integer between 1 and 65535")
    return port


def _poll_interval(value: str) -> float:
    try:
        interval = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("poll interval must be a finite positive number of seconds") from error
    if not math.isfinite(interval) or interval <= 0:
        raise argparse.ArgumentTypeError("poll interval must be a finite positive number of seconds")
    return interval


def parse_arguments(arguments: Optional[List[str]] = None) -> ObserverOptions:
    """Parse observer-only command-line options without starting the server."""
    parser = argparse.ArgumentParser(
        prog="crosstalk-mcp observe",
        description="Open Crosstalk's local observability dashboard.",
    )
    parser.add_argument("--silent", action="store_true", help="do not open a browser automatically")
    parser.add_argument("--port", type=_port, metavar="PORT", help="bind exactly this loopback port")
    parser.add_argument(
        "--poll-interval",
        type=_poll_interval,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        metavar="SECONDS",
        help="observer refresh interval in seconds (default: %(default)s)",
    )
    parser.add_argument("--groups-dir", metavar="PATH", help="directory containing Crosstalk group databases")
    parsed = parser.parse_args(arguments)
    return ObserverOptions(
        silent=parsed.silent,
        port=parsed.port,
        poll_interval=parsed.poll_interval,
        groups_dir=parsed.groups_dir,
    )


def resolve_groups_directory(options: ObserverOptions, environment: Optional[dict] = None) -> Path:
    """Resolve the observer's read-only groups directory without creating it."""
    environment = os.environ if environment is None else environment
    if options.groups_dir is not None:
        return Path(options.groups_dir)
    if environment.get("CROSSTALK_GROUPS_DIR"):
        return Path(environment["CROSSTALK_GROUPS_DIR"])
    return Path.home() / ".cache" / "crosstalk"


def open_read_only_database(database_path: Path) -> sqlite3.Connection:
    """Open an existing SQLite database without any ability to mutate it."""
    uri = database_path.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=0.1)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


GROUP_DATABASE_PATTERN = re.compile(r"grp_[0-9a-f]{32}\.sqlite3$")
DEFAULT_MESSAGE_PAGE_SIZE = 100
MAX_MESSAGE_PAGE_SIZE = 500
OVERVIEW_EVENT_LIMIT = 500
ERROR_CATEGORY_LABELS = {
    "validation": "Validation Error",
    "not_found": "Not Found Error",
    "authorization": "Authorization Error",
    "sqlite_busy": "Database Busy Error",
    "internal": "Internal Error",
}
ANALYTICS_OUTCOME_OPTIONS = (
    ("success", "Success"),
    ("error", "Error"),
    *(("error:" + category, label) for category, label in ERROR_CATEGORY_LABELS.items()),
)
OBSERVER_STATIC_ASSETS = {
    "/static/observer.css": (
        "text/css; charset=utf-8",
        b""":root{color-scheme:light;--canvas:#f6f7fb;--surface:#fff;--surface-alt:#f8faff;--border:#e2e7f0;--text:#18243a;--muted:#66748a;--accent:#2868d8;--accent-soft:#e9f1ff;--warning:#a45d00;--warning-soft:#fff4df;--danger:#b42331;--danger-soft:#fff0f1;--shadow:0 12px 30px rgba(35,55,88,.08)}:root[data-theme=dark]{color-scheme:dark;--canvas:#101522;--surface:#171e2d;--surface-alt:#1c2637;--border:#2b374d;--text:#edf2fa;--muted:#a4b0c3;--accent:#8bb9ff;--accent-soft:#1d365d;--warning:#ffd08a;--warning-soft:#49351d;--danger:#ffabb4;--danger-soft:#4a2630;--shadow:0 12px 30px rgba(0,0,0,.22)}*{box-sizing:border-box}body{margin:0;min-height:100vh;background:var(--canvas);color:var(--text);font:15px/1.5 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}button,input{font:inherit}button{border:0;background:none;color:inherit;cursor:pointer}button:focus-visible,input:focus-visible{outline:3px solid color-mix(in srgb,var(--accent) 35%,transparent);outline-offset:2px}.shell{max-width:1240px;margin:auto;padding:28px 24px 56px}.app-header{display:flex;align-items:center;justify-content:space-between;gap:24px;padding-bottom:22px}.eyebrow{margin:0 0 3px;color:var(--accent);font-size:.72rem;font-weight:750;letter-spacing:.11em;text-transform:uppercase}.app-header h1{margin:0;font-size:1.65rem;letter-spacing:-.035em}.app-header p{margin:3px 0 0;color:var(--muted);font-size:.9rem}.header-actions{display:flex;align-items:center;gap:12px}.live-status{color:var(--muted);font-size:.82rem}.live-status:not(:empty):before{display:inline-block;width:7px;height:7px;margin-right:6px;border-radius:50%;background:var(--accent);content:""}.theme-toggle{padding:8px 10px;border:1px solid var(--border);border-radius:8px;color:var(--muted)}.primary-nav{display:flex;gap:4px;margin-bottom:20px;padding:5px;background:var(--surface);border:1px solid var(--border);border-radius:12px;box-shadow:var(--shadow)}.primary-nav button{flex:0 0 auto;padding:8px 13px;border-radius:8px;color:var(--muted);font-weight:650}.primary-nav button:hover,.primary-nav button.is-active{background:var(--accent-soft);color:var(--accent)}#chat-panel{min-width:0}.view{padding:26px;background:var(--surface);border:1px solid var(--border);border-radius:16px;box-shadow:var(--shadow)}.view-header{display:flex;align-items:start;justify-content:space-between;gap:18px;margin-bottom:22px}.view-header h2{margin:0;font-size:1.3rem;letter-spacing:-.025em}.view-header p{margin:4px 0 0;color:var(--muted)}.notice{margin:0;color:var(--muted)}.notice.error{color:var(--danger)}.metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:22px}.metric{min-height:92px;padding:14px;border:1px solid var(--border);border-radius:11px;background:var(--surface-alt)}.metric strong,.metric span{display:block}.metric strong{color:var(--muted);font-size:.7rem;letter-spacing:.08em;text-transform:uppercase}.metric span{margin-top:9px;font-size:1.45rem;font-weight:720;letter-spacing:-.035em}.attention-grid,.overview-columns{display:grid;grid-template-columns:1.15fr .85fr;gap:16px}.card{padding:18px;border:1px solid var(--border);border-radius:12px;background:var(--surface-alt)}.card h3{margin:0 0 12px;font-size:.9rem}.attention-list,.activity-list,.overview ul,.chat-list{padding:0;margin:0;list-style:none}.attention-list li,.activity-list li,.overview li{padding:11px 0;border-bottom:1px solid var(--border)}.attention-list li:last-child,.activity-list li:last-child,.overview li:last-child{border-bottom:0}.attention-list li{display:flex;align-items:center;justify-content:space-between;gap:12px}.attention-list strong{font-size:.9rem}.attention-list small,.activity-list small,.overview small{display:block;color:var(--muted)}.pill{display:inline-block;padding:3px 7px;border-radius:999px;background:var(--warning-soft);color:var(--warning);font-size:.72rem;font-weight:700;white-space:nowrap}.pill.danger{background:var(--danger-soft);color:var(--danger)}.breakdowns{display:grid;grid-template-columns:1fr 1fr;gap:16px}.chats-layout{display:grid;grid-template-columns:260px minmax(0,1fr);gap:22px}.chat-picker{padding-right:18px;border-right:1px solid var(--border)}.chat-picker h3{margin:0 0 10px;font-size:.82rem;color:var(--muted);letter-spacing:.08em;text-transform:uppercase}.chat-picker button{display:block;width:100%;padding:10px;border-radius:8px;text-align:left}.chat-picker button:hover,.chat-picker button.is-active{background:var(--accent-soft);color:var(--accent)}.chat{min-width:0}.chat>aside{float:right;width:215px;margin:0 0 16px 22px;padding:14px;border:1px solid var(--border);border-radius:10px;background:var(--surface-alt)}.chat aside ul{padding:0;margin:0;list-style:none}.chat aside li{padding:7px 0;border-bottom:1px solid var(--border)}.chat aside li:last-child{border-bottom:0}.message{position:relative;margin:10px 0;padding:14px 15px 13px 18px;border:1px solid var(--border);border-radius:10px;background:var(--surface-alt)}.message:before{position:absolute;top:0;bottom:0;left:0;width:3px;border-radius:3px 0 0 3px;background:var(--accent);content:""}.message header,.message footer{display:flex;justify-content:space-between;gap:12px;color:var(--muted);font-size:.78rem}.message p{margin:10px 0;white-space:pre-wrap;overflow-wrap:anywhere}.message footer{padding-top:8px;border-top:1px solid var(--border)}.filters{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-bottom:20px}.filters label{color:var(--muted);font-size:.75rem;font-weight:650;text-transform:capitalize}.filters input{display:block;width:100%;margin-top:5px;padding:8px 9px;border:1px solid var(--border);border-radius:8px;background:var(--surface);color:var(--text)}.filters button{align-self:end;padding:9px 12px;border-radius:8px;background:var(--accent);color:#fff;font-weight:700;text-align:center}.echarts-gallery{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;margin-bottom:20px}.echarts-card{min-width:0;padding:16px;border:1px solid var(--border);border-radius:12px;background:var(--surface-alt)}.echarts-card:first-child{grid-column:1/-1}.echarts-card h4{margin:0;font-size:.92rem}.echarts-card p{margin:2px 0 0;color:var(--muted);font-size:.8rem}.echart{height:250px;margin-top:10px}.analytics{overflow:hidden}.analytics table{width:100%;border:1px solid var(--border);border-collapse:separate;border-spacing:0;border-radius:10px;overflow:hidden}.analytics th,.analytics td{padding:10px 11px;border-bottom:1px solid var(--border);text-align:left}.analytics th{background:var(--surface-alt);color:var(--muted);font-size:.7rem;letter-spacing:.06em;text-transform:uppercase}.analytics tr:last-child td{border-bottom:0}.storage-details{display:grid;grid-template-columns:180px 1fr;margin:20px 0;border:1px solid var(--border);border-radius:10px;overflow:hidden}.storage-details dt,.storage-details dd{padding:10px 12px;border-bottom:1px solid var(--border)}.storage-details dt{background:var(--surface-alt);color:var(--muted);font-weight:650}.storage-details dd{margin:0}.storage-details dt:nth-last-of-type(1),.storage-details dd:last-child{border-bottom:0}button.danger{margin-top:8px;padding:9px 12px;border-radius:8px;background:var(--danger);color:#fff;font-weight:700}@media(max-width:800px){.shell{padding:20px 14px 40px}.metrics{grid-template-columns:repeat(2,minmax(0,1fr))}.attention-grid,.overview-columns,.chats-layout{grid-template-columns:1fr}.chat-picker{padding:0 0 14px;border-right:0;border-bottom:1px solid var(--border)}.chat-picker .chat-list{display:flex;gap:6px;overflow:auto}.chat-picker button{white-space:nowrap}.echarts-gallery{grid-template-columns:1fr}.echarts-card:first-child{grid-column:auto}}@media(max-width:560px){.app-header{align-items:start;flex-direction:column}.header-actions{width:100%;justify-content:space-between}.primary-nav{overflow:auto}.view{padding:18px}.metrics,.filters,.breakdowns{grid-template-columns:1fr}.chat>aside{float:none;width:auto;margin:0 0 16px}.message header,.message footer{align-items:start;flex-direction:column;gap:2px}.analytics{overflow-x:auto}.analytics table{min-width:680px}.storage-details{grid-template-columns:1fr}.storage-details dt{border-bottom:0}}
""",
    ),
}

# Workspace overrides keep the observer compact without changing its dependency-free asset model.
OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
:root[data-theme=dark]{--canvas:#0b0d12;--surface:#11141b;--surface-alt:#161a23;--surface-hover:#1d2330;--border:#272c37;--text:#eef1f7;--muted:#8992a3;--accent:#7aa2ff;--accent-soft:#17284d;--danger:#ff8b98;--danger-soft:#3a1e28;--warning:#f5c975;--warning-soft:#352b17}body{background:var(--canvas);font-size:14px}.app-shell{display:grid;grid-template-columns:220px minmax(0,1fr);min-height:100vh}.sidebar{position:sticky;top:0;display:flex;flex-direction:column;height:100vh;padding:14px 10px;border-right:1px solid var(--border);background:var(--surface)}.brand{display:flex;align-items:center;gap:9px;padding:8px 9px 20px;font-weight:750;letter-spacing:-.02em}.brand-mark{display:grid;width:24px;height:24px;place-items:center;border-radius:7px;background:var(--accent);color:#0b0d12;font-size:11px}.sidebar-nav{display:grid;gap:3px}.sidebar-nav button{display:flex;align-items:center;gap:10px;padding:8px 9px;border-radius:6px;color:var(--muted);font-weight:620;text-align:left}.sidebar-nav button:hover,.sidebar-nav button.is-active{background:var(--surface-hover);color:var(--text)}.nav-icon{width:18px;color:var(--accent);font-size:11px;text-align:center}.sidebar-footer{margin-top:auto;padding:10px 5px 2px;border-top:1px solid var(--border)}.sidebar-footer button{width:100%;padding:7px 4px;color:var(--muted);font-size:.78rem;text-align:left}.sidebar-status{display:flex;align-items:center;gap:7px;padding:0 4px 10px;color:var(--muted);font-size:.77rem}.sidebar-status:before{width:6px;height:6px;border-radius:50%;background:#7dd3a7;content:""}.workspace{min-width:0;padding:18px 22px 22px}.workspace-bar{display:flex;align-items:center;justify-content:space-between;min-height:32px;margin-bottom:14px}.workspace-title{margin:0;font-size:1rem;letter-spacing:-.02em}.workspace-meta{color:var(--muted);font-size:.78rem}.view{padding:0;background:transparent;border:0;border-radius:0;box-shadow:none}.view-header{margin:0 0 14px}.view-header h2{font-size:1.05rem}.view-header p{font-size:.82rem}.eyebrow{display:none}.metrics{display:flex;gap:0;margin:0 0 14px;border:1px solid var(--border);border-radius:8px;background:var(--surface)}.metric{flex:1;min-height:0;padding:10px 12px;border:0;border-right:1px solid var(--border);border-radius:0;background:transparent}.metric:last-child{border-right:0}.metric strong{font-size:.62rem;letter-spacing:.075em}.metric span{margin-top:3px;font-size:1.05rem;letter-spacing:-.02em}.overview-grid{display:grid;grid-template-columns:minmax(370px,.9fr) minmax(540px,1.45fr);gap:14px;align-items:start}.panel{min-width:0;border:1px solid var(--border);border-radius:8px;background:var(--surface)}.panel-header{display:flex;align-items:center;justify-content:space-between;padding:10px 12px;border-bottom:1px solid var(--border)}.panel-header h3{margin:0;font-size:.78rem}.panel-header span{color:var(--muted);font-size:.72rem}.activity-list,.attention-list{padding:0 12px;margin:0;list-style:none}.activity-list li,.attention-list li{padding:9px 0;border-bottom:1px solid var(--border);font-size:.82rem}.activity-list li:last-child,.attention-list li:last-child{border:0}.activity-list small,.attention-list small{display:inline;margin-left:6px;font-size:.75rem}.attention-panel{margin-bottom:14px}.attention-panel:empty{display:none}.group-table{width:100%;border-collapse:collapse;font-size:.81rem}.group-table th,.group-table td{padding:9px 12px;border-bottom:1px solid var(--border);text-align:left;white-space:nowrap}.group-table th{color:var(--muted);font-size:.65rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase}.group-table tr:last-child td{border-bottom:0}.group-table tr:hover td{background:var(--surface-hover)}.group-table .group-name{font-weight:650}.status{display:inline-flex;align-items:center;gap:5px;color:var(--muted);font-size:.72rem}.status:before{width:6px;height:6px;border-radius:50%;background:#7dd3a7;content:""}.status.attention:before{background:var(--warning)}.status.error:before{background:var(--danger)}.breakdown-row{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:14px}.breakdown-row .panel{padding:11px 12px}.breakdown-row h3{margin:0 0 7px;font-size:.75rem}.breakdown-row ul{display:flex;flex-wrap:wrap;gap:7px;padding:0;margin:0;list-style:none}.breakdown-row li{padding:3px 6px;border-radius:4px;background:var(--surface-alt);color:var(--muted);font-size:.72rem}.pill{border-radius:4px}.chats-layout{grid-template-columns:200px minmax(0,1fr);gap:14px}.chat-picker{padding-right:12px}.chat-picker button{padding:7px 8px;font-size:.82rem}.chat>aside{width:190px;margin-left:14px;padding:10px}.message{margin:7px 0;padding:10px 11px 10px 14px;border-radius:7px}.message p{margin:7px 0}.filters{gap:8px;margin-bottom:12px}.filters input{padding:7px 8px}.filters button{padding:8px}.echarts-gallery{gap:10px}.echarts-card{padding:11px;border-radius:7px}.echart{height:210px}.analytics th,.analytics td{padding:8px 10px}.storage-details{margin:14px 0}.sidebar.is-collapsed{grid-template-columns:58px minmax(0,1fr)}.sidebar.is-collapsed .sidebar{padding-inline:9px}.sidebar.is-collapsed .brand span,.sidebar.is-collapsed .sidebar-nav span:not(.nav-icon),.sidebar.is-collapsed .sidebar-footer span{display:none}.sidebar.is-collapsed .brand{justify-content:center;padding-inline:0}.sidebar.is-collapsed .sidebar-nav button{justify-content:center}.sidebar.is-collapsed .sidebar-footer button{text-align:center}@media(max-width:900px){.app-shell{grid-template-columns:58px minmax(0,1fr)}.sidebar{padding-inline:9px}.brand span,.sidebar-nav span:not(.nav-icon),.sidebar-footer span{display:none}.brand{justify-content:center;padding-inline:0}.sidebar-nav button{justify-content:center}.sidebar-footer button{text-align:center}.overview-grid{grid-template-columns:1fr}.workspace{padding:14px}}@media(max-width:620px){.app-shell{display:block}.sidebar{position:static;display:flex;flex-direction:row;width:100%;height:auto;padding:7px;border-right:0;border-bottom:1px solid var(--border)}.brand,.sidebar-footer,.sidebar-status{display:none}.sidebar-nav{display:flex;width:100%;justify-content:space-around}.sidebar-nav button{padding:7px}.workspace{padding:12px}.metrics{display:grid;grid-template-columns:repeat(3,1fr)}.metric{border-right:1px solid var(--border);border-bottom:1px solid var(--border)}.metric:nth-child(3n){border-right:0}.overview-grid{display:block}.panel{margin-bottom:12px;overflow:auto}.group-table{min-width:560px}.breakdown-row{grid-template-columns:1fr}.chats-layout{grid-template-columns:1fr}.chat-picker{border-bottom:1px solid var(--border);border-right:0}.chat-picker .chat-list{display:flex;overflow:auto}.chat>aside{float:none;width:auto;margin:0 0 12px}}
""",)
OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.app-shell.is-collapsed{grid-template-columns:58px minmax(0,1fr)}.app-shell.is-collapsed .sidebar{padding-inline:9px}.app-shell.is-collapsed .brand span,.app-shell.is-collapsed .sidebar-nav span:not(.nav-icon),.app-shell.is-collapsed .sidebar-footer span{display:none}.app-shell.is-collapsed .brand{justify-content:center;padding-inline:0}.app-shell.is-collapsed .sidebar-nav button{justify-content:center}.app-shell.is-collapsed .sidebar-footer button{text-align:center}
""",)

# Cockpit overrides are deliberately appended: the observer remains dependency-free and
# older destination fragments can keep their compact shared primitives.
OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.cockpit{min-height:calc(100vh - 112px)}.cockpit-grid{display:grid;grid-template-columns:minmax(250px,.8fr) minmax(430px,1.35fr) minmax(250px,.8fr);gap:12px;height:calc(100vh - 238px);min-height:440px}.cockpit-pane{display:flex;min-height:0;flex-direction:column;overflow:hidden}.cockpit-pane .panel-header{flex:0 0 auto}.activity-list,.failure-list{min-height:0;overflow:auto}.activity-list{padding:0 12px}.event{display:grid;grid-template-columns:54px 1fr;gap:9px;padding:10px 0;border-bottom:1px solid var(--border);font-size:.79rem}.event:last-child{border:0}.event-kind{align-self:start;padding:2px 4px;border-radius:4px;background:var(--accent-soft);color:var(--accent);font-size:.62rem;font-weight:750;text-align:center;text-transform:uppercase}.event p{margin:2px 0;line-height:1.35;overflow-wrap:anywhere}.event small,.failure small{display:block;color:var(--muted);font-size:.7rem}.event-group{color:var(--muted);font-size:.73rem}.event-wakeup .event-kind,.event.is-attention .event-kind{background:var(--warning-soft);color:var(--warning)}.event-group .event-kind{background:var(--surface-alt);color:var(--muted)}.event-success{opacity:.72}.event-error{background:var(--danger-soft);margin:0 -12px;padding-inline:12px}.event-error .event-kind{background:var(--danger);color:#fff}.table-scroll{min-height:0;overflow:auto}.group-table a{color:var(--text);text-decoration:none}.group-table a:hover{color:var(--accent);text-decoration:underline}.reliability-metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;border-bottom:1px solid var(--border);background:var(--border)}.reliability-metrics span{padding:10px 8px;background:var(--surface);color:var(--muted);font-size:.67rem}.reliability-metrics b{display:block;color:var(--text);font-size:.92rem}.failure-list{padding:0 12px;margin:0;list-style:none}.failure{padding:9px 0;border-bottom:1px solid var(--border)}.failure strong,.failure span{display:block}.failure strong{color:var(--danger);font-size:.78rem}.failure span{color:var(--muted);font-size:.72rem}.empty-row{padding:14px;color:var(--muted);list-style:none}.panel-header a{color:var(--accent);font-size:.72rem;cursor:pointer}@media(max-width:1120px){.cockpit-grid{grid-template-columns:minmax(0,1fr) minmax(300px,.9fr);height:auto}.activity-pane{grid-row:span 2;height:520px}.health-pane,.reliability-pane{height:254px}}@media(max-width:700px){.cockpit{min-height:0}.cockpit-grid{display:flex;flex-direction:column;height:auto}.cockpit-pane,.activity-pane,.health-pane,.reliability-pane{height:360px}.reliability-pane{order:-1}.metrics{overflow:auto}.metric{min-width:112px}.group-table{min-width:590px}}
""",)

# OpsCenter-inspired visual language: a quiet dark rail, high-density panels, and
# severity color reserved for information that demands an operator's attention.
OBSERVER_STATIC_ASSETS["/static/observer.css"] += ("""
:root[data-theme=dark]{--canvas:#090909;--surface:#121212;--surface-alt:#181818;--surface-hover:#202020;--border:#272727;--text:#d6d4ce;--muted:#77746e;--accent:#e5a900;--accent-soft:#332708;--warning:#e5a900;--warning-soft:#332708;--danger:#e45d55;--danger-soft:#301819;--ops-ok:#23b58a;--shadow:none}body{background:var(--canvas);font-size:12px}.ops-shell{display:grid;grid-template-columns:186px minmax(0,1fr);min-height:100vh}.ops-rail{position:sticky;top:0;display:flex;flex-direction:column;height:100vh;padding:10px 7px;border-right:1px solid #1d1d1d;background:#0c0c0c}.ops-brand{display:flex;align-items:center;gap:8px;padding:5px 7px 18px;color:#ddd9ce;font-size:12px;font-weight:750}.ops-brand b{display:grid;width:20px;height:20px;place-items:center;border-radius:5px;background:var(--accent);color:#171306;font-size:11px}.ops-brand small{margin-left:auto;color:#806b18;font:700 9px ui-monospace,SFMono-Regular,monospace}.ops-nav{display:grid;gap:2px}.ops-nav button{display:flex;align-items:center;gap:9px;width:100%;padding:8px;border:1px solid transparent;border-radius:6px;color:#86827b;font-size:12px;text-align:left}.ops-nav button:hover{background:#171717;color:var(--text)}.ops-nav button.is-active{border-color:#dedad1;background:#191919;color:#e8e4d9;box-shadow:inset 2px 0 0 var(--accent)}.ops-nav i{width:13px;color:var(--accent);font-style:normal;text-align:center}.ops-live{margin-left:auto;color:var(--ops-ok);font:700 9px ui-monospace,SFMono-Regular,monospace}.ops-rail-footer{display:grid;gap:12px;margin-top:auto;padding:10px 7px;color:#68655f;font-size:11px}.ops-rail-footer span:first-child:before{margin-right:7px;color:var(--danger);content:'●'}.ops-user{padding:8px;border-radius:5px;background:#151515;color:#c5c0b4;font-size:10px}.ops-workspace{min-width:0}.ops-topbar{display:flex;align-items:center;height:41px;padding:0 14px;border-bottom:1px solid #1e1e1e;background:#0d0d0d}.ops-topbar h1{margin:0;color:#c6c2ba;font-size:12px;font-weight:650}.ops-topbar h1:before{margin-right:10px;color:#6d6963;content:'›'}.ops-topbar .live-status{margin-left:auto;color:#6d6963;font:10px ui-monospace,SFMono-Regular,monospace}.ops-topbar .live-status:before{background:var(--ops-ok)}.ops-topbar .theme-toggle{margin-left:12px;padding:4px 6px;border-color:#292929;border-radius:4px;font-size:10px}.ops-workspace #chat-panel{padding:10px 12px}.view{padding:0;background:transparent;border:0;border-radius:0;box-shadow:none}.view-header{margin:0 0 10px}.view-header h2{font-size:13px}.view-header p{font-size:10px}.cockpit-grid{grid-template-columns:minmax(310px,1fr) minmax(310px,1fr) minmax(270px,.72fr);gap:10px;height:calc(100vh - 176px);min-height:480px}.health-pane{grid-column:1 / 3;grid-row:1;height:auto}.activity-pane{grid-column:3;grid-row:1 / 3}.reliability-pane{grid-column:1 / 3;grid-row:2;height:auto}.panel{border-color:#242424;border-radius:7px;background:#151515}.panel-header{padding:9px 12px;border-color:#222}.panel-header h3{color:#a6a29a;font:700 9px ui-monospace,SFMono-Regular,monospace;letter-spacing:.11em;text-transform:uppercase}.panel-header span{font:9px ui-monospace,SFMono-Regular,monospace}.metrics{gap:10px;margin-bottom:10px}.metric{min-height:70px;padding:11px 13px;border-color:#242424;border-radius:7px;background:#151515}.metric strong{font:700 9px ui-monospace,SFMono-Regular,monospace;letter-spacing:.12em}.metric span{margin-top:4px;color:#e1ddd3;font-size:24px;line-height:1}.event{grid-template-columns:48px 1fr;padding:9px 0;border-color:#242424;font-size:10px}.event-kind{padding:1px 3px;border-radius:2px;font:700 8px ui-monospace,SFMono-Regular,monospace}.event p{margin:3px 0;color:#aaa69e;line-height:1.4}.event small,.failure small{font:9px ui-monospace,SFMono-Regular,monospace}.event-group{color:var(--accent);font:9px ui-monospace,SFMono-Regular,monospace}.event-success{opacity:.6}.group-table{font-size:11px}.group-table th,.group-table td{padding:9px 12px;border-color:#242424}.group-table th{font:700 9px ui-monospace,SFMono-Regular,monospace}.status{font:700 9px ui-monospace,SFMono-Regular,monospace}.status:before{background:var(--ops-ok)}.group-table tr:hover td{background:#1a1a1a}.reliability-metrics span{padding:9px 12px;background:#151515;font:9px ui-monospace,SFMono-Regular,monospace}.reliability-metrics b{font:700 16px Inter,system-ui,sans-serif}.failure{padding:8px 0;border-color:#242424}.failure strong{font:700 10px ui-monospace,SFMono-Regular,monospace}.analytics .metrics{margin-top:10px}.analytics table{border-color:#242424;font-size:11px}.analytics th,.analytics td{padding:8px 10px;border-color:#242424}.analytics th{background:#151515;font:700 9px ui-monospace,SFMono-Regular,monospace}.chats-layout{gap:10px}.chat-picker{padding-right:10px;border-color:#242424}.chat-picker h3{font:700 9px ui-monospace,SFMono-Regular,monospace}.chat-picker button{margin-bottom:4px;padding:9px;border-radius:6px;background:#151515;font-size:11px}.chat>aside{background:#151515;border-color:#242424}.message{margin:0;padding:12px 14px;border-width:0 0 1px;border-radius:0;background:transparent}.message:before{display:none}.message header strong{color:#d8d3c9}.storage-details{border-color:#242424}.storage-details dt{background:#151515}.primary-nav{display:none}@media(max-width:900px){.ops-shell{grid-template-columns:56px minmax(0,1fr)}.ops-brand span,.ops-brand small,.ops-nav span,.ops-live,.ops-rail-footer{display:none}.ops-brand{justify-content:center;padding-inline:0}.ops-nav button{justify-content:center}.cockpit-grid{grid-template-columns:minmax(0,1fr) minmax(250px,.75fr)}.health-pane{grid-column:1 / 3}.activity-pane{grid-column:2}.reliability-pane{grid-column:1}}@media(max-width:650px){.ops-shell{display:block}.ops-rail{position:static;display:block;width:100%;height:auto;padding:5px;border-right:0;border-bottom:1px solid #1e1e1e}.ops-brand{display:none}.ops-nav{display:flex}.ops-nav button{justify-content:center}.ops-nav i{width:auto}.ops-workspace #chat-panel{padding:8px}.ops-topbar{height:36px}.cockpit-grid{display:flex;height:auto;min-height:0}.health-pane,.activity-pane,.reliability-pane{height:330px}.activity-pane{order:-1}.metrics{overflow:auto}.metric{min-width:132px}.chats-layout{grid-template-columns:1fr}}
""".encode(),)


OBSERVER_STATIC_ASSETS["/static/observer.css"] += ("""
/* Apply the reference's quiet, fixed-rail rhythm to the observer shell. */
.app-shell{grid-template-columns:186px minmax(0,1fr);background:#090909}.sidebar{padding:10px 7px;background:#0c0c0c;border-color:#1d1d1d}.brand{padding:5px 7px 18px;color:#ddd9ce;font-size:12px}.brand-mark{width:20px;height:20px;border-radius:5px;background:var(--accent);color:#171306}.sidebar-nav{gap:2px}.sidebar-nav button{padding:8px;border:1px solid transparent;border-radius:6px;color:#86827b;font-size:12px}.sidebar-nav button:hover{background:#171717}.sidebar-nav button.is-active{border-color:#dedad1;background:#191919;color:#e8e4d9;box-shadow:inset 2px 0 0 var(--accent)}.sidebar-status{color:var(--ops-ok);font:700 9px ui-monospace,SFMono-Regular,monospace}.sidebar-status:before{background:var(--ops-ok)}.sidebar-footer{border-color:#1d1d1d}.sidebar-footer button{font-size:10px}.workspace{padding:0}.workspace-bar{min-height:41px;margin:0;padding:0 14px;border-bottom:1px solid #1e1e1e;background:#0d0d0d}.workspace-title{font-size:12px}.workspace-title:before{margin-right:10px;color:#6d6963;content:'›'}.workspace-meta{font:10px ui-monospace,SFMono-Regular,monospace}.workspace #chat-panel{padding:10px 12px}@media(max-width:900px){.app-shell{grid-template-columns:56px minmax(0,1fr)}}
""".encode(),)

# Use the reference palette for the first paint too; a saved light preference should
# never turn the operations console into a white page.
OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
:root,:root[data-theme=light]{color-scheme:dark;--canvas:#090909;--surface:#121212;--surface-alt:#181818;--surface-hover:#202020;--border:#272727;--text:#d6d4ce;--muted:#77746e;--accent:#e5a900;--accent-soft:#332708;--warning:#e5a900;--warning-soft:#332708;--danger:#e45d55;--danger-soft:#301819;--shadow:none}html,body{background:#090909!important}.workspace,.app-shell,#chat-panel{background:#090909}.theme-toggle{background:#151515!important;color:#aaa69e!important;border-color:#303030!important}.primary-nav{display:none!important}.view,.analytics,.storage{background:transparent!important}.echarts-card{background:#151515!important;border-color:#242424!important}.filters input{background:#151515!important;color:#d6d4ce!important;border-color:#303030!important}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.analytics .view-header{display:flex;align-items:end;justify-content:space-between;padding:2px 2px 9px;border-bottom:1px solid #202020}.analytics .filters{display:grid;grid-template-columns:.65fr .9fr .9fr .75fr 1fr auto;gap:7px;margin:10px 0}.analytics .filters label{font:700 8px ui-monospace,SFMono-Regular,monospace;letter-spacing:.08em}.analytics .filters input,.analytics .filters select{width:100%;margin-top:3px;padding:7px 8px;border:1px solid #303030;border-radius:4px;background:#151515;color:#d6d4ce;font:10px ui-monospace,SFMono-Regular,monospace}.analytics .filters select{height:31px}.analytics .filters button{align-self:end;padding:7px 10px;border-radius:4px;background:#332708;color:#e5a900;font-size:10px}.analytics .metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:0 0 10px;border:0;background:transparent}.analytics .metric{min-height:76px;padding:12px;border:1px solid #242424;border-radius:7px;background:#151515}.analytics .metric.is-alert span{color:#e45d55}.chart-gallery{display:grid;grid-template-columns:1.3fr .7fr;gap:10px;margin-bottom:10px}.chart-card{min-width:0;padding:12px;border:1px solid #242424;border-radius:7px;background:#151515}.chart-card:first-child{grid-column:1/-1}.chart-heading{display:flex;align-items:start;justify-content:space-between}.chart-heading h4{margin:0;color:#cfcac0;font-size:11px}.chart-kicker,.chart-caption{margin:2px 0;color:#77746e;font:9px ui-monospace,SFMono-Regular,monospace}.chart-total{color:#77746e;font:9px ui-monospace,SFMono-Regular,monospace}.chart{display:block;width:100%;max-height:220px;margin-top:8px}.chart-grid{stroke:#252525;stroke-dasharray:3 4}.chart-axis{stroke:#343434}.chart-label{fill:#77746e;font-size:8px}.chart-area{fill:rgba(229,169,0,.12)}.chart-line{fill:none;stroke:#e5a900;stroke-width:2}.chart-point{fill:#e5a900;stroke:#151515;stroke-width:2}.bar{fill:#b17a1f}.chart-ring{fill:none;stroke-width:18;transform:rotate(-90deg);transform-origin:80px 80px}.chart-center{fill:#d6d4ce;font-size:19px;font-weight:700}.chart-center-label{fill:#77746e;font-size:8px}.chart-legend{display:grid;gap:6px;padding:0;margin:8px 0 0;list-style:none;font-size:10px}.chart-legend li{display:flex;justify-content:space-between;color:#aaa69e}.chart-legend i{display:inline-block;width:7px;height:7px;margin-right:6px;border-radius:50%}.analytics .table-wrap{overflow:auto;border:1px solid #242424;border-radius:7px}.analytics table{border:0;border-radius:0}.analytics .outcome{font:700 9px ui-monospace,SFMono-Regular,monospace}.analytics .outcome-success{color:#23b58a}.analytics .outcome-error{color:#e45d55}@media(max-width:900px){.analytics .filters{grid-template-columns:repeat(3,minmax(0,1fr)) auto}}@media(max-width:750px){.analytics .filters{grid-template-columns:repeat(2,minmax(0,1fr))}.analytics .metrics,.chart-gallery{grid-template-columns:1fr 1fr}.chart-card:first-child{grid-column:1/-1}}@media(max-width:500px){.analytics .metrics,.chart-gallery{grid-template-columns:1fr}.analytics .metric{min-height:64px}.chart-card:first-child{grid-column:auto}}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* Shared inset: content should never sit directly against a workspace or card edge. */
.workspace #chat-panel > .view{padding:12px}.cockpit .view-header{padding:2px 2px 10px}.cockpit-pane .panel-header{padding:10px 12px}.cockpit-pane .activity-list,.cockpit-pane .failure-list{padding-left:12px;padding-right:12px}.health-pane .table-scroll{padding:0 6px 6px}.reliability-pane .notice{padding:12px}.analytics .view-header{padding:4px 2px 12px}.analytics .filters{margin:12px 2px}.analytics .table-wrap{margin:0 2px 2px}.chats .chat-detail{padding:2px 4px}.chat-picker{padding:2px 10px 2px 2px}.storage .notice{padding:2px}.storage-details{margin:14px 2px}@media(max-width:650px){.workspace #chat-panel > .view{padding:9px}.analytics .metrics{gap:8px}.cockpit-pane .panel-header{padding:9px 10px}}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* Green is reserved for successful and healthy operations. */
.event-success .event-kind{background:#12382c;color:#23b58a}.event-success strong{color:#79d5b7}.event-success .event-group{color:#23b58a}.status:not(.attention):not(.error){color:#23b58a}.analytics .outcome-success{color:#23b58a!important}.analytics .outcome-error{color:#e45d55!important}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* Keep success green on its badge only; body text stays neutral like every other event. */
.event-success strong{color:var(--text)}.event-success .event-group{color:var(--muted)}.event-error{margin:0;padding:9px 0}.event-error .event-kind{background:var(--danger);color:#fff}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* Severity changes the badge and error field, never the tool or group label. */
.event-success,.event-error{opacity:1}.event-success strong,.event-error strong{color:var(--text)}.event-success .event-group,.event-error .event-group{color:var(--muted)}.event-error{margin:0 -12px;padding:9px 12px}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* Dull white keeps sender/tool and group names equally legible in every event. */
.event-success strong,.event-error strong,.event-success .event-group,.event-error .event-group{color:#d6d4ce!important}.event-error{margin:4px -12px;padding:9px 12px}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* The label rule applies to messages, wakeups, successes, and errors alike. */
.activity-list .event strong,.activity-list .event .event-group{color:#d6d4ce!important}.activity-list > .event-error{margin:4px -12px;padding:9px 12px}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* Messages are informational cyan; wakeups retain the amber attention treatment. */
.event-kind{padding:3px 5px}.event-message .event-kind{background:#16495b;color:#80e3f5}.event-wakeup .event-kind,.event.is-attention .event-kind{background:#4a3510;color:#ffd36a}.event-success .event-kind{background:#164a37;color:#6ee7b7}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* Group names in the Overview table open their selected chat. */
.group-table .group-row{cursor:pointer}.group-table .group-row:hover .group-name,.group-table .group-row:focus-visible .group-name{color:var(--accent)}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* Chat member unread counts stay separate from names and signal their state at a glance. */
.chat aside .chat-member{display:flex;align-items:center;justify-content:space-between;gap:8px}.chat aside .unread-badge{flex:0 0 auto;padding:2px 5px;border-radius:3px;font:700 8px ui-monospace,SFMono-Regular,monospace}.chat aside .unread-badge.has-unread{background:#4a2024;color:#ff9b9f}.chat aside .unread-badge.is-clear{background:#163b2d;color:#78ddb5}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* The rail toggle belongs with the brand, not in the utility footer. */
.brand{justify-content:flex-start}.brand .sidebar-toggle{display:grid;width:24px;height:22px;place-items:center;margin-left:auto;padding:0;border:1px solid #292929;border-radius:4px;color:#89857d}.brand .sidebar-toggle:hover{background:#1b1b1b;color:#d6d4ce}.brand .sidebar-toggle i{font:700 8px ui-monospace,SFMono-Regular,monospace;font-style:normal;letter-spacing:-2px}.app-shell.is-collapsed .brand .sidebar-toggle{display:grid}.app-shell.is-collapsed .brand .sidebar-toggle i{display:block}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* The entire icon-led Crosstalk row is the collapse affordance. */
button.brand{display:flex;width:100%;align-items:center;gap:9px;padding:8px 9px 20px;border:0;border-radius:5px;color:#ddd9ce;font-weight:750;text-align:left}button.brand:hover{background:#171717}button.brand:hover .brand-mark{filter:brightness(1.12)}.app-shell.is-collapsed button.brand{justify-content:center;padding-inline:0}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* Purposeful line icons replace the generic sidebar glyphs. */
.nav-icon{display:grid;place-items:center}.nav-icon svg{width:14px;height:14px;fill:none;stroke:currentColor;stroke-linecap:round;stroke-linejoin:round;stroke-width:1.7}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* Dark mode is the sole operations theme; live status belongs to Operations. */
.sidebar-footer .sidebar-status,.sidebar-footer #theme-toggle{display:none}.workspace-bar{gap:10px}.topbar-actions{display:flex;align-items:center;gap:10px;margin-left:auto}.overview-live{display:inline-flex;align-items:center;gap:5px;padding:4px 7px;border:1px solid #245342;border-radius:999px;background:#10251f;color:#23b58a;font:700 9px ui-monospace,SFMono-Regular,monospace}.overview-live:before{width:5px;height:5px;border-radius:50%;background:#23b58a;content:''}
""",)


OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* Desktop Overview fits the viewport; only the live event log scrolls. */
@media(min-width:701px){html,body,.app-shell,.workspace{height:100vh;overflow:hidden}.workspace #chat-panel{height:calc(100vh - 41px);overflow:hidden}.workspace #chat-panel > .cockpit{display:flex;height:100%;min-height:0;flex-direction:column}.cockpit .view-header,.cockpit .metrics{flex:0 0 auto}.cockpit-grid{height:auto;min-height:0;flex:1;grid-template-rows:minmax(250px,1fr) minmax(145px,.58fr)}.cockpit-pane{min-height:0}.activity-pane .activity-list{overflow:auto}.health-pane .table-scroll,.reliability-pane .failure-list{overflow:hidden}.health-pane .table-scroll{flex:1}.reliability-pane .failure-list{min-height:0}.cockpit .metrics{margin-bottom:10px}.cockpit .metric{min-height:64px}}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.panel-action{width:auto;padding:3px 7px;border:1px solid #3b3216;border-radius:6px;background:#1b170c;color:#e5a900;font:700 9px ui-monospace,SFMono-Regular,monospace}.panel-action:hover{background:#332708}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.storage button:not(.danger){display:inline-flex;width:auto;align-items:center;justify-content:center;margin:2px 12px 2px 0;padding:7px 10px;border:0;border-radius:6px;background:#1f6d49;color:#eafff3;font:inherit;font-weight:700}.storage button:not(.danger):hover{background:#2b8760}.storage button.danger{display:inline-flex;width:auto;align-items:center;justify-content:center;margin:2px 0;padding:7px 10px;border-radius:6px}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.analytics .filters{grid-template-columns:repeat(5,minmax(0,1fr))}.analytics .range-control{min-width:0}.analytics .range-composite{display:flex;width:100%;height:31px;margin-top:3px;border:1px solid #303030;border-radius:4px;background:#151515;overflow:hidden}.analytics .range-composite input[type=number]{flex:1;width:auto;min-width:96px;margin:0;padding:7px 8px;border:0;border-radius:0;background:transparent}.analytics .range-unit{display:grid;min-width:82px;place-items:center;border-left:1px solid #303030;color:#d6d4ce;font:700 9px ui-monospace,SFMono-Regular,monospace}.analytics .range-unit-stepper{display:grid;width:18px;border-left:1px solid #303030}.analytics .range-unit-stepper button{width:18px;min-width:18px;height:15px;margin:0;padding:0;border:0;border-radius:0;background:#1b1b1b;color:#aaa69e;font:700 8px/1 ui-monospace,SFMono-Regular,monospace;text-align:center}.analytics .range-unit-stepper button:hover{background:#2a2a2a;color:#e5a900}@media(max-width:900px){.analytics .filters{grid-template-columns:repeat(3,minmax(0,1fr))}}@media(max-width:750px){.analytics .filters{grid-template-columns:repeat(2,minmax(0,1fr))}}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.analytics-log{margin-top:10px}.analytics-log>header{display:flex;align-items:center;justify-content:space-between;padding:0 2px 7px}.analytics-log h3{margin:0;color:#cfcac0;font-size:11px}.analytics-log header span{color:#77746e;font:9px ui-monospace,SFMono-Regular,monospace}.analytics-log .event-log-table{overflow:hidden;border:1px solid #242424;border-radius:7px}.analytics-log .event-log-header,.analytics-log .event-log-scroll table{width:100%;margin:0;border:0;border-radius:0;table-layout:fixed}.analytics-log .event-log-header th{background:#181818;box-shadow:0 1px 0 #303030}.analytics-log .event-log-scroll{height:clamp(120px,18vh,184px);max-height:calc(100vh - 180px);overflow:auto;scrollbar-gutter:stable}.analytics-log .event-log-scroll td{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.analytics .table-wrap{max-height:360px;overflow:auto}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.chart-area{fill:rgba(85,198,193,.14)}.chart-line{stroke:#55c6c1}.chart-point{fill:#55c6c1}.bar{fill:#3b9f9b}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.chart-gallery{grid-template-columns:1fr}.chart-card:first-child{grid-column:auto}.chart-scroll{overflow-x:auto}.chart-scroll .bar-chart{display:block;margin-top:8px}.chart-card-donut .chart{display:block;width:160px;margin:8px auto 0}.chart-card-donut .chart-legend{max-width:360px;margin-left:auto;margin-right:auto}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.chart-donut-segment{}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.analytics-summary-row{display:grid;grid-template-columns:280px repeat(2,minmax(0,1fr));gap:10px;margin-bottom:10px}.analytics-summary-row .metrics{display:grid;grid-template-columns:1fr;grid-template-rows:repeat(3,minmax(0,1fr));margin:0}.analytics-summary-row .metric{min-height:0;padding:8px 10px}.analytics-summary-row .metric span{margin-top:2px;font-size:16px}.analytics-summary-row .chart-card{margin:0}.analytics-summary-row .chart-card-donut{position:relative;padding:6px}.analytics-summary-row .chart-card-donut .chart-heading{position:absolute;z-index:1;top:6px;left:6px}.analytics-summary-row .chart-card-donut .chart{width:260px;max-height:none;margin:0 auto}@media(max-width:1050px){.analytics-summary-row{grid-template-columns:280px minmax(0,1fr)}.analytics-summary-row .chart-card-donut:last-child{grid-column:1/-1}}@media(max-width:750px){.analytics-summary-row{grid-template-columns:1fr}.analytics-summary-row .metrics{min-height:180px}}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.workspace-bar{min-height:48px;padding-top:4px;padding-bottom:4px}.topbar-page{min-width:0}.topbar-page #page-title{margin:0;font-size:12px}.topbar-page #page-description{margin:1px 0 0;padding-left:14px;color:#77746e;font-size:9px;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.chats>.view-header,.storage>.view-header,.analytics form>.view-header{display:none}.cockpit .view-header{justify-content:flex-end;min-height:18px;margin:0 0 4px;padding:0}.cockpit .view-header>div{display:none}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.analytics-summary-row .chart-card-donut .doughnut-content{display:flex;align-items:center;justify-content:center;gap:10px;min-height:260px;padding:0 24px 0 0}.analytics-summary-row .chart-card-donut .chart{flex:0 0 220px;width:220px;margin:0 auto}.analytics-summary-row .chart-card-donut .chart-center{font-size:22px}.analytics-summary-row .chart-card-donut .chart-center-label{font-size:9px}.analytics-summary-row .chart-card-donut .chart-legend{flex:0 1 140px;margin:0;gap:5px}.analytics-summary-row .chart-card-donut .chart-legend li{font-size:10px}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
.echarts-host{width:100%;height:220px;margin-top:8px}.chart-card-donut .doughnut-content{display:block}.analytics-summary-row .chart-card-donut .doughnut-content{display:block;min-height:260px;padding:0 24px 0 0}.analytics-summary-row .chart-card-donut .echarts-host{height:260px;margin:0}.echarts-host canvas,.echarts-host svg{outline:none}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
@media(min-width:701px){.cockpit-grid{grid-template-rows:repeat(2,minmax(0,1fr))}.health-pane .table-scroll,.reliability-pane .failure-list{overflow:auto}.reliability-pane .failure-list{flex:1}.health-pane,.reliability-pane{height:auto}}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* Keep chat content in its own column; floated metadata allowed long lines to bleed beneath it. */
.chat{display:grid;grid-template-columns:minmax(0,1fr) 380px;gap:14px;align-items:start}.chat>.view-header{grid-column:1/-1}.chat>aside{grid-column:2;grid-row:2;float:none;width:auto;margin:0}.chat>#message-list{grid-column:1;grid-row:2;min-width:0;overflow:hidden}.messages,.message,.message p,.message header,.message footer{min-width:0}.message p,.message header>*,.message footer>*{overflow-wrap:anywhere;word-break:break-word}@media(max-width:620px){.chat{display:block}.chat>aside{width:auto;margin:0 0 12px}.chat>#message-list{overflow:visible}}
""",)

OBSERVER_STATIC_ASSETS["/static/observer.css"] += (b"""
/* Chats use the available workspace height; only conversation history scrolls. */
@media(min-width:701px){.workspace #chat-panel>.chats{display:flex;height:100%;min-height:0;flex-direction:column;overflow:hidden}.workspace #chat-panel>.chats>.chats-layout{height:100%;min-height:0;overflow:hidden;flex:1}.chats .chat-detail,.chats .chat{height:100%;min-height:0;overflow:hidden}.chats .chat{grid-template-rows:auto minmax(0,1fr)}.chats .chat>#message-list{height:100%;min-height:0;overflow-y:auto!important;overflow-x:hidden;scrollbar-gutter:stable}.chats .chat-picker{min-height:0;overflow:auto}}
""",)


class _ObserverHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        request = urlparse(self.path)
        asset = OBSERVER_STATIC_ASSETS.get(request.path)
        if asset is not None:
            content_type, *parts = asset
            body = b"\n".join(parts)
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if request.path == "/events":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            subscriber = self.server.event_hub.subscribe()
            try:
                self.wfile.write(("event: snapshot\ndata: " + json.dumps(self.server.snapshot_provider(), separators=(",", ":")) + "\n\n").encode())
                self.wfile.flush()
                while True:
                    item = subscriber.get()
                    if item is None:
                        break
                    event, payload = item
                    self.wfile.write(("event: " + event + "\ndata: " + json.dumps(payload, separators=(",", ":")) + "\n\n").encode())
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                self.server.event_hub.unsubscribe(subscriber)
            return
        if request.path == "/":
            self._send_html(render_dashboard(self.server.groups_directory, self.server.csrf_token))
            return
        if request.path == "/api/health":
            body = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        query = parse_qs(request.query)
        group_id = query.get("group_id", [None])[0]
        if request.path == "/fragments/chats":
            self._send_html(render_chats_workspace(self.server.groups_directory, group_id))
            return
        if request.path == "/fragments/chat":
            self._send_html(render_chat_panel(self.server.groups_directory, group_id))
            return
        if request.path == "/fragments/overview":
            self._send_html(render_overview(self.server.groups_directory))
            return
        if request.path == "/fragments/analytics":
            self._send_html(render_tool_analytics(self.server.groups_directory, {key: value[0] for key, value in query.items()}))
            return
        if request.path == "/fragments/storage":
            self._send_html(render_storage(self.server.groups_directory, self.server.csrf_token))
            return
        if request.path == "/fragments/messages":
            older_than = query.get("older_than_message_id", [None])[0]
            try:
                cursor = int(older_than) if older_than is not None else None
            except ValueError:
                self._send_html("<p class=\"notice error\">Invalid message cursor.</p>", 400)
                return
            self._send_html(render_message_page(self.server.groups_directory, group_id, cursor))
            return
        if request.path == "/fragments/message":
            message_id = query.get("message_id", [None])[0]
            try:
                message_id_value = int(message_id) if message_id is not None else 0
            except ValueError:
                message_id_value = 0
            self._send_html(render_visible_message(self.server.groups_directory, group_id, message_id_value), 200 if message_id_value else 400)
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:
        endpoint = urlparse(self.path).path
        if endpoint not in {"/api/storage/reclaim", "/api/storage/delete-history"}:
            self.send_response(404)
            self.end_headers()
            return
        if not self.server.owner.valid_csrf_token(self.headers.get("X-CSRF-Token")):
            self._send_html('<p class="notice error">Maintenance request rejected: invalid CSRF token.</p>', 403)
            return
        if endpoint == "/api/storage/delete-history":
            if self.headers.get("X-Crosstalk-Confirm") != "DELETE AUDIT HISTORY":
                self._send_html('<p class="notice error">Audit-history deletion requires explicit confirmation.</p>', 400)
                return
            result = delete_audit_history(self.server.groups_directory)
        else:
            result = reclaim_audit_free_space(self.server.groups_directory)
        status = 200 if result["ok"] else 409
        self._send_html('<p class="notice{}">{}</p>'.format("" if result["ok"] else " error", html.escape(result["message"])), status)

    def _send_html(self, page: str, status: int = 200) -> None:
        body = page.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


class ObserverHTTPServer:
    """Loopback-only HTTP server with graceful shutdown."""

    def __init__(self, port: int, poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS, groups_directory: Optional[Path] = None) -> None:
        self.httpd = ThreadingHTTPServer(("127.0.0.1", port), _ObserverHandler)
        self.httpd.daemon_threads = True
        self.csrf_token = secrets.token_urlsafe(32)
        self.httpd.csrf_token = self.csrf_token
        self.httpd.owner = self
        self.event_hub = EventHub()
        self.httpd.event_hub = self.event_hub
        self.httpd.snapshot_provider = lambda: observer_snapshot(groups_directory) if groups_directory else {"groups": {}, "latest_audit_id": None}
        self.httpd.groups_directory = groups_directory
        self.poll_interval = poll_interval
        self.poller = ObserverPoller(groups_directory, poll_interval, self.event_hub) if groups_directory else None
        self._poller_thread: Optional[threading.Thread] = None

    @property
    def port(self) -> int:
        return self.httpd.server_address[1]

    def serve_forever(self) -> None:
        try:
            if self.poller is not None:
                # Establish a baseline before clients can subscribe, so existing
                # records are represented by their snapshot rather than live events.
                self.poller.poll_once()
                self._poller_thread = threading.Thread(target=self.poller.run, daemon=True)
                self._poller_thread.start()
            self.httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            if self.poller is not None:
                self.poller.stop()
            if self._poller_thread is not None:
                self._poller_thread.join(timeout=max(1.0, self.poll_interval * 2))
            self.event_hub.close()
            self.httpd.server_close()

    def valid_csrf_token(self, token: Optional[str]) -> bool:
        return isinstance(token, str) and secrets.compare_digest(token, self.csrf_token)


class EventHub:
    def __init__(self) -> None:
        self.subscribers: List[queue.Queue] = []
        self._lock = threading.Lock()
        self._closed = False

    def subscribe(self) -> queue.Queue:
        subscriber: queue.Queue = queue.Queue(maxsize=32)
        with self._lock:
            if self._closed:
                subscriber.put_nowait(None)
            else:
                self.subscribers.append(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: queue.Queue) -> None:
        with self._lock:
            if subscriber in self.subscribers:
                self.subscribers.remove(subscriber)

    def publish(self, event: str, payload: dict) -> None:
        with self._lock:
            if self._closed:
                return
            subscribers = list(self.subscribers)
        for subscriber in subscribers:
            try:
                subscriber.put_nowait((event, payload))
            except queue.Full:
                self.unsubscribe(subscriber)

    def close(self) -> None:
        """Wake SSE handlers so server shutdown does not wait for browsers."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            subscribers = list(self.subscribers)
            self.subscribers.clear()
        for subscriber in subscribers:
            try:
                subscriber.put_nowait(None)
            except queue.Full:
                try:
                    subscriber.get_nowait()
                except queue.Empty:
                    pass
                try:
                    subscriber.put_nowait(None)
                except queue.Full:
                    pass


class ObserverPoller:
    """Poll compact group markers and publish only changed state."""
    def __init__(self, groups_directory: Path, interval: float, event_hub: EventHub) -> None:
        self.groups_directory, self.interval, self.event_hub = groups_directory, interval, event_hub
        self._markers: dict = {}
        self._latest_audit_id: Optional[int] = None
        self._audit_marker_known = False
        self._stopped = threading.Event()

    def poll_once(self) -> None:
        group_ids = discover_groups(self.groups_directory)
        for group_id in group_ids:
            try:
                marker = group_fingerprint(self.groups_directory, group_id)
            except (OSError, sqlite3.Error, ValueError):
                continue
            previous = self._markers.get(group_id)
            self._markers[group_id] = marker
            if previous is not None and marker[0] != previous[0]:
                self.event_hub.publish("message.created", {"group_id": group_id, "message_id": marker[0]})
            if previous is not None and marker[1] != previous[1]:
                self.event_hub.publish("group.changed", {"group_id": group_id})
            if previous is not None and marker[2] != previous[2]:
                self.event_hub.publish("member.changed", {"group_id": group_id})
            if previous is not None and marker[3] != previous[3]:
                self.event_hub.publish("wakeup.changed", {"group_id": group_id})
        # A missing directory can be transient while storage is mounted or
        # replaced. Only treat a missing individual file as group deletion.
        if self.groups_directory.is_dir():
            for group_id in set(self._markers) - set(group_ids):
                del self._markers[group_id]
                self.event_hub.publish("group.deleted", {"group_id": group_id})
        try:
            latest_audit_id = latest_audit_id_for_directory(self.groups_directory)
        except (OSError, sqlite3.Error, ValueError):
            return
        if self._audit_marker_known and latest_audit_id is not None and latest_audit_id != self._latest_audit_id:
            self.event_hub.publish("tool_call.completed", {"tool_call_id": latest_audit_id})
        self._latest_audit_id = latest_audit_id
        self._audit_marker_known = True

    def run(self) -> None:
        while not self._stopped.wait(self.interval):
            self.poll_once()

    def stop(self) -> None:
        self._stopped.set()


def create_observer_server(port: Optional[int], poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS, groups_directory: Optional[Path] = None) -> ObserverHTTPServer:
    """Bind an explicit port, or prefer 8787 before an OS-selected fallback."""
    if port is not None:
        server = ObserverHTTPServer(port, poll_interval, groups_directory)
        server.used_default_port_fallback = False
        return server
    try:
        server = ObserverHTTPServer(8787, poll_interval, groups_directory)
        server.used_default_port_fallback = False
        return server
    except OSError:
        server = ObserverHTTPServer(0, poll_interval, groups_directory)
        server.used_default_port_fallback = True
        return server


def discover_groups(groups_directory: Path) -> List[str]:
    """Return valid group IDs without creating a missing directory."""
    if not groups_directory.is_dir():
        return []
    return sorted(path.name[:-8] for path in groups_directory.iterdir() if path.is_file() and GROUP_DATABASE_PATTERN.fullmatch(path.name))


def _valid_group_id(group_id: Optional[str]) -> bool:
    return isinstance(group_id, str) and GROUP_DATABASE_PATTERN.fullmatch(group_id + ".sqlite3") is not None


def _group_database_path(groups_directory: Path, group_id: Optional[str]) -> Path:
    if not _valid_group_id(group_id):
        raise ValueError("Invalid group ID.")
    return groups_directory / (group_id + ".sqlite3")


def read_group_snapshot(groups_directory: Path, group_id: str) -> dict:
    """Read one group database without changing unread or wakeup state."""
    path = _group_database_path(groups_directory, group_id)
    connection = open_read_only_database(path)
    try:
        metadata = connection.execute("SELECT name, description, creator_context_id, created_at, updated_at FROM group_metadata WHERE id = 1").fetchone()
        members = [dict(row) | {"unread_count": len(json.loads(row["unread_message_ids"]))} for row in connection.execute("SELECT context_id, name, joined_at, unread_message_ids FROM group_members ORDER BY joined_at")]
        latest = connection.execute("SELECT id, created_at FROM messages ORDER BY id DESC LIMIT 1").fetchone()
        wakeups = [dict(row) for row in connection.execute("SELECT message_id, context_id, relevance, priority, created_at, acknowledged_at, last_notified_at FROM wakeup_events ORDER BY id")]
        return {"group_id": group_id, "metadata": dict(metadata) if metadata else None, "members": members, "latest_message_id": latest["id"] if latest else None, "latest_activity_at": latest["created_at"] if latest else None, "wakeups": wakeups}
    finally:
        connection.close()


def read_message_page(groups_directory: Path, group_id: str, older_than_message_id: Optional[int] = None, limit: int = DEFAULT_MESSAGE_PAGE_SIZE) -> dict:
    """Return a bounded chronological page, optionally before a message cursor."""
    if not isinstance(limit, int) or limit < 1 or limit > MAX_MESSAGE_PAGE_SIZE:
        raise ValueError("message page limit must be between 1 and " + str(MAX_MESSAGE_PAGE_SIZE))
    if older_than_message_id is not None and (not isinstance(older_than_message_id, int) or older_than_message_id < 1):
        raise ValueError("older message cursor must be a positive integer")
    connection = open_read_only_database(_group_database_path(groups_directory, group_id))
    try:
        if older_than_message_id is None:
            rows = connection.execute("SELECT id, sender_context_id, sender_name, content, created_at, priority, mentions, wakeup_targets, routing_reason FROM messages ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        else:
            rows = connection.execute("SELECT id, sender_context_id, sender_name, content, created_at, priority, mentions, wakeup_targets, routing_reason FROM messages WHERE id < ? ORDER BY id DESC LIMIT ?", (older_than_message_id, limit)).fetchall()
        messages = [dict(row) for row in reversed(rows)]
        return {"messages": messages, "next_older_message_id": messages[0]["id"] if len(rows) == limit else None}
    finally:
        connection.close()


def read_audit_metadata(groups_directory: Path) -> Optional[dict]:
    path = groups_directory / "observability.sqlite3"
    if not path.is_file():
        return None
    connection = open_read_only_database(path)
    try:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(metadata)")}
        selected = ["schema_version", "created_at", "audit_enabled_at", "last_retention_cleanup_at"]
        if "retention_setting" in columns:
            selected.append("retention_setting")
        row = connection.execute("SELECT " + ", ".join(selected) + " FROM metadata WHERE id = 1").fetchone()
        metadata = dict(row) if row else None
        if metadata is not None:
            metadata.setdefault("retention_setting", None)
        return metadata
    finally:
        connection.close()


def read_tool_calls(groups_directory: Path, limit: int = 100) -> List[dict]:
    """Read raw recent audit rows; analytics remain derived from raw events."""
    if not 1 <= limit <= MAX_MESSAGE_PAGE_SIZE:
        raise ValueError("tool call limit must be between 1 and " + str(MAX_MESSAGE_PAGE_SIZE))
    connection = open_read_only_database(groups_directory / "observability.sqlite3")
    try:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        query = "SELECT tool_calls.*, NULL AS group_name FROM tool_calls"
        if "tool_call_group_names" in tables:
            query = "SELECT tool_calls.*, tool_call_group_names.group_name FROM tool_calls LEFT JOIN tool_call_group_names ON tool_call_group_names.tool_call_id = tool_calls.id"
        rows = [dict(row) for row in connection.execute(query + " ORDER BY tool_calls.id DESC LIMIT ?", (limit,))]
        for row in rows:
            group_id = row.get("group_id")
            row["group_deleted"] = bool(group_id) and not (groups_directory / (group_id + ".sqlite3")).is_file()
        return rows
    finally:
        connection.close()


def read_tool_duration_values(groups_directory: Path) -> List[int]:
    connection = open_read_only_database(groups_directory / "observability.sqlite3")
    try:
        return [row[0] for row in connection.execute("SELECT duration_ms FROM tool_calls ORDER BY occurred_at")]
    finally:
        connection.close()


def latest_audit_id_for_directory(groups_directory: Path) -> Optional[int]:
    """Return the latest completed audit row without creating audit storage."""
    path = groups_directory / "observability.sqlite3"
    if not path.is_file():
        return None
    connection = open_read_only_database(path)
    try:
        row = connection.execute("SELECT MAX(id) FROM tool_calls").fetchone()
        return row[0] if row is not None else None
    finally:
        connection.close()


def group_fingerprint(groups_directory: Path, group_id: str) -> tuple:
    """Compact deterministic state marker for observer polling."""
    snapshot = read_group_snapshot(groups_directory, group_id)
    members = tuple((member["context_id"], member["unread_count"]) for member in snapshot["members"])
    wakeups = tuple((item["message_id"], item["context_id"], item["acknowledged_at"], item["last_notified_at"]) for item in snapshot["wakeups"])
    metadata = snapshot["metadata"] or {}
    return (snapshot["latest_message_id"], metadata.get("updated_at"), members, wakeups)


def observer_snapshot(groups_directory: Path) -> dict:
    groups = {}
    for group_id in discover_groups(groups_directory):
        try:
            marker = group_fingerprint(groups_directory, group_id)
            groups[group_id] = {"latest_message_id": marker[0], "member_fingerprint": repr(marker[2]), "wakeup_fingerprint": repr(marker[3])}
        except (OSError, sqlite3.Error, ValueError):
            pass
    try:
        latest_audit_id = latest_audit_id_for_directory(groups_directory)
    except (OSError, sqlite3.Error, ValueError):
        latest_audit_id = None
    return {"groups": groups, "latest_audit_id": latest_audit_id}


def _message_html(message: dict) -> str:
    """Render a single message without trusting any group database text as HTML."""
    return (
        '<article class="message" data-message-id="{id}"><header><strong>{sender}</strong>'
        '<time>{created}</time></header><p>{content}</p><footer>priority: {priority} · routing: {routing}</footer></article>'
    ).format(
        id=message["id"],
        sender=html.escape(str(message.get("sender_name") or message.get("sender_context_id") or "Unknown")),
        created=html.escape(format_timestamp(message.get("created_at"), "")),
        content=html.escape(str(message.get("content") or "")),
        priority=html.escape(str(message.get("priority") or "normal")),
        routing=html.escape(str(message.get("routing_reason") or "normal")),
    )


def format_timestamp(value: object, empty: str = "—", local_timezone: Optional[timezone] = None) -> str:
    """Render an ISO timestamp in the observer machine's local time zone."""
    if not value:
        return empty
    try:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return str(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    local = timestamp.astimezone(local_timezone) if local_timezone is not None else timestamp.astimezone()
    return local.strftime("%Y-%m-%d %H:%M:%S %Z (%z)")


def group_display_name(groups_directory: Optional[Path], group_id: Optional[str]) -> str:
    """Return a human-readable group name while preserving the ID for routing."""
    if groups_directory is None or not _valid_group_id(group_id):
        return "Untitled group"
    try:
        metadata = read_group_snapshot(groups_directory, group_id)["metadata"] or {}
    except (OSError, sqlite3.Error, ValueError):
        return "Untitled group"
    return str(metadata.get("name") or "Untitled group")


def render_message_page(groups_directory: Optional[Path], group_id: Optional[str], older_than_message_id: Optional[int] = None) -> str:
    if groups_directory is None or not _valid_group_id(group_id):
        return '<p class="notice error">Choose a valid group.</p>'
    try:
        page = read_message_page(groups_directory, group_id, older_than_message_id)
    except (OSError, sqlite3.Error, ValueError):
        return '<p class="notice error">Messages are temporarily unavailable; retry shortly.</p>'
    messages = "".join(_message_html(message) for message in page["messages"])
    older = page["next_older_message_id"]
    load_older = ""
    if older is not None:
        load_older = (
            '<button class="load-older" hx-get="/fragments/messages?group_id={group}&amp;older_than_message_id={cursor}" '
            'hx-target="this" hx-swap="outerHTML">Load older messages</button>'
        ).format(group=html.escape(group_id), cursor=older)
    return load_older + messages or '<p class="notice">No messages in this group yet.</p>'


def render_visible_message(groups_directory: Optional[Path], group_id: Optional[str], message_id: int) -> str:
    if groups_directory is None or not _valid_group_id(group_id) or message_id < 1:
        return '<p class="notice error">Message is unavailable.</p>'
    try:
        connection = open_read_only_database(_group_database_path(groups_directory, group_id))
        try:
            row = connection.execute(
                "SELECT id, sender_context_id, sender_name, content, created_at, priority, routing_reason FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
        finally:
            connection.close()
    except (OSError, sqlite3.Error, ValueError):
        return '<p class="notice error">Message is temporarily unavailable.</p>'
    return _message_html(dict(row)) if row is not None else '<p class="notice">Message is no longer available.</p>'


def render_chat_panel(groups_directory: Optional[Path], group_id: Optional[str]) -> str:
    if groups_directory is None or not _valid_group_id(group_id):
        return '<section class="empty"><h2>Choose a chat</h2><p>Select a group to view its read-only history.</p></section>'
    try:
        snapshot = read_group_snapshot(groups_directory, group_id)
    except (OSError, sqlite3.Error, ValueError):
        return '<section class="empty"><h2>Chat unavailable</h2><p>The group may be deleted or temporarily locked.</p></section>'
    metadata = snapshot["metadata"] or {}
    members = "".join(
        '<li class="chat-member"><span>{name}</span><span class="unread-badge {state}">{unread} unread</span></li>'.format(
            name=html.escape(str(member.get("name") or member["context_id"])), unread=member["unread_count"],
            state="has-unread" if member["unread_count"] else "is-clear",
        ) for member in snapshot["members"]
    ) or "<li>No members</li>"
    wakeups = "".join(
        '<li>message #{message} → {context}<small>{priority}/{relevance} · created {created} · acknowledged {acknowledged} · last notified {notified}</small></li>'.format(
            message=html.escape(str(wakeup.get("message_id") or "—")),
            context=html.escape(str(wakeup.get("context_id") or "—")),
            priority=html.escape(str(wakeup.get("priority") or "normal")),
            relevance=html.escape(str(wakeup.get("relevance") or "")),
            created=html.escape(format_timestamp(wakeup.get("created_at"))),
            acknowledged=html.escape(format_timestamp(wakeup.get("acknowledged_at"), "pending")),
            notified=html.escape(format_timestamp(wakeup.get("last_notified_at"), "never")),
        ) for wakeup in snapshot["wakeups"]
    ) or "<li>No wakeup events.</li>"
    return (
        '<section class="chat" data-group-id="{group}"><header class="view-header"><div><h2>{name}</h2><p>{description}</p></div></header>'
        '<aside><h3>Members</h3><ul>{members}</ul><h3>Wakeups</h3><ul>{wakeups}</ul></aside><div id="message-list" class="messages">{messages}</div></section>'
    ).format(
        group=html.escape(group_id), name=html.escape(str(metadata.get("name") or "Untitled group")),
        description=html.escape(str(metadata.get("description") or "")), members=members,
        wakeups=wakeups, messages=render_message_page(groups_directory, group_id),
    )


def render_chats_workspace(groups_directory: Optional[Path], group_id: Optional[str] = None) -> str:
    """Render chat browsing as a dedicated workspace with a persistent group picker."""
    groups = discover_groups(groups_directory) if groups_directory is not None else []
    selected = group_id if group_id in groups else (groups[0] if groups else None)
    picker = "".join(
        '<button class="{}" hx-get="/fragments/chats?group_id={}" hx-target="#chat-panel" hx-swap="innerHTML">{}</button><!-- title="{}" -->'.format(
            "is-active" if item == selected else "", html.escape(item),
            html.escape(group_display_name(groups_directory, item)), html.escape(item),
        ) for item in groups
    ) or '<p class="notice">No groups found. This page will update when a group database appears.</p>'
    detail = render_chat_panel(groups_directory, selected)
    return '<section class="view chats" data-chats="true" data-page-title="Chats" data-page-description="Read-only conversation history and participant state."><header class="view-header"><div><h2>Chats</h2><p>Read-only conversation history and participant state.</p></div></header><div class="chats-layout"><aside class="chat-picker"><h3>Groups</h3><div class="chat-list">{}</div></aside><div class="chat-detail">{}</div></div></section>'.format(picker, detail)


def overview_data(groups_directory: Optional[Path]) -> dict:
    """Derive small, current overview metrics directly from read-only databases."""
    totals = {"groups": 0, "contexts": 0, "messages": 0, "members": 0, "unread": 0, "pending_wakeups": 0, "wakeup_response": "—", "acknowledged_wakeups": 0, "tool_calls": "—", "error_rate": "—"}
    priorities: dict = {}
    routing: dict = {}
    activity: List[dict] = []
    contexts = set()
    wakeup_response_seconds: List[float] = []
    if groups_directory is None:
        return {"totals": totals, "priorities": priorities, "routing": routing, "activity": activity, "audit": None}
    for group_id in discover_groups(groups_directory):
        try:
            snapshot = read_group_snapshot(groups_directory, group_id)
            totals["groups"] += 1
            totals["members"] += len(snapshot["members"])
            totals["unread"] += sum(member["unread_count"] for member in snapshot["members"])
            totals["pending_wakeups"] += sum(1 for wakeup in snapshot["wakeups"] if wakeup.get("acknowledged_at") is None)
            for wakeup in snapshot["wakeups"]:
                if not wakeup.get("created_at") or not wakeup.get("acknowledged_at"):
                    continue
                try:
                    response = (datetime.fromisoformat(wakeup["acknowledged_at"]) - datetime.fromisoformat(wakeup["created_at"])).total_seconds()
                except ValueError:
                    continue
                wakeup_response_seconds.append(max(0, response))
            contexts.update(member["context_id"] for member in snapshot["members"])
            if snapshot["latest_message_id"] is not None:
                activity.append({"group_id": group_id, "group_name": str((snapshot["metadata"] or {}).get("name") or "Untitled group"), "created_at": snapshot["latest_activity_at"], "message_id": snapshot["latest_message_id"]})
            connection = open_read_only_database(_group_database_path(groups_directory, group_id))
            try:
                for priority, count in connection.execute("SELECT COALESCE(priority, 'normal'), COUNT(*) FROM messages GROUP BY priority"):
                    priorities[priority] = priorities.get(priority, 0) + count
                    totals["messages"] += count
                for reason, count in connection.execute("SELECT COALESCE(routing_reason, 'normal'), COUNT(*) FROM messages GROUP BY routing_reason"):
                    routing[reason] = routing.get(reason, 0) + count
            finally:
                connection.close()
        except (OSError, sqlite3.Error, ValueError):
            continue
    totals["contexts"] = len(contexts)
    totals["wakeup_response"] = "—" if not wakeup_response_seconds else "{:.1f}s avg".format(sum(wakeup_response_seconds) / len(wakeup_response_seconds))
    totals["acknowledged_wakeups"] = len(wakeup_response_seconds)
    activity.sort(key=lambda item: item["created_at"] or "", reverse=True)
    try:
        audit = read_audit_metadata(groups_directory)
    except (OSError, sqlite3.Error, ValueError):
        audit = None
    if audit is not None:
        try:
            connection = open_read_only_database(groups_directory / "observability.sqlite3")
            try:
                call_count, error_count = connection.execute("SELECT COUNT(*), COALESCE(SUM(outcome = 'error'), 0) FROM tool_calls").fetchone()
            finally:
                connection.close()
            totals["tool_calls"] = call_count
            totals["error_rate"] = "{:.1f}%".format(error_count * 100 / call_count) if call_count else "0.0%"
        except (OSError, sqlite3.Error, ValueError):
            pass
    return {"totals": totals, "priorities": priorities, "routing": routing, "activity": activity[:10], "audit": audit}


def _legacy_render_overview(groups_directory: Optional[Path]) -> str:
    data = overview_data(groups_directory)
    totals = data["totals"]
    metric_labels = (("Groups", "groups"), ("Active contexts", "contexts"), ("Messages", "messages"), ("Tool calls", "tool_calls"), ("Error rate", "error_rate"), ("Wakeup response", "wakeup_response"))
    metrics = "".join('<article class="metric"><strong>{}</strong><span>{}</span></article>'.format(html.escape(label), totals[key]) for label, key in metric_labels)
    def breakdown(values: dict) -> str:
        return "".join('<li>{}: <strong>{}</strong></li>'.format(html.escape(str(key)), value) for key, value in sorted(values.items())) or "<li>None yet</li>"
    activity = "".join('<li><strong>{}</strong> · message #{} <small>{}</small></li>'.format(html.escape(item["group_name"]), item["message_id"], html.escape(format_timestamp(item["created_at"], ""))) for item in data["activity"]) or "<li>No recent activity.</li>"
    audit_notice = '<p class="notice error">Audit analytics are disabled. Set <code>CROSSTALK_OBSERVABILITY_RETENTION_DAYS=inf</code> (or a positive number of days) before starting Crosstalk.</p>' if data["audit"] is None else '<p class="notice">Audit active since {}.</p>'.format(html.escape(format_timestamp(data["audit"].get("audit_enabled_at"), "")))
    responsiveness = "No acknowledged wakeups yet." if not totals["acknowledged_wakeups"] else "{} acknowledged wakeup{} included in the average.".format(totals["acknowledged_wakeups"], "" if totals["acknowledged_wakeups"] == 1 else "s")
    attention = []
    if totals["error_rate"] not in {"—", "0.0%"}:
        attention.append('<li><div><strong>Tool errors need review</strong><small>{} of recorded calls failed.</small></div><span class="pill danger">{} error rate</span></li>'.format(html.escape(totals["error_rate"]), html.escape(totals["error_rate"])))
    if totals["pending_wakeups"]:
        attention.append('<li><div><strong>Wakeups are awaiting acknowledgement</strong><small>{} pending across active groups.</small></div><span class="pill">{} pending</span></li>'.format(totals["pending_wakeups"], totals["pending_wakeups"]))
    if totals["unread"]:
        attention.append('<li><div><strong>Unread messages remain</strong><small>{} messages have not been read by their members.</small></div><span class="pill">{} unread</span></li>'.format(totals["unread"], totals["unread"]))
    if not attention:
        attention.append('<li><div><strong>Everything looks clear</strong><small>No errors, pending wakeups, or unread messages need attention.</small></div><span class="pill">Healthy</span></li>')
    return '<section class="view overview" data-overview="true"><header class="view-header"><div><p class="eyebrow">Live operations</p><h2>Overview</h2><p>What needs attention across your local Crosstalk groups.</p></div>{}</header><div class="metrics">{}</div><div class="attention-grid"><section class="card"><h3>Attention now</h3><ul class="attention-list">{}</ul></section><section class="card"><h3>Recent activity</h3><ul class="activity-list">{}</ul></section></div><div class="overview-columns"><section class="card"><h3>Message priority</h3><ul>{}</ul></section><section class="card"><h3>Routing</h3><ul>{}</ul><h3>Wakeup responsiveness</h3><p class="notice">{}</p></section></div></section>'.format(audit_notice, metrics, "".join(attention), activity, breakdown(data["priorities"]), breakdown(data["routing"]), responsiveness)


def overview_group_rows(groups_directory: Optional[Path]) -> List[dict]:
    """Read compact, current group state for the operational status table."""
    rows: List[dict] = []
    if groups_directory is None:
        return rows
    for group_id in discover_groups(groups_directory):
        try:
            snapshot = read_group_snapshot(groups_directory, group_id)
            members = snapshot["members"]
            pending = sum(1 for wakeup in snapshot["wakeups"] if wakeup.get("acknowledged_at") is None)
            unread = sum(member["unread_count"] for member in members)
            if pending:
                state, state_class = "Needs attention", "attention"
            elif unread:
                state, state_class = "Unread", "attention"
            else:
                state, state_class = "Healthy", ""
            rows.append({
                "id": group_id,
                "name": str((snapshot["metadata"] or {}).get("name") or "Untitled group"),
                "members": len(members),
                "unread": unread,
                "pending": pending,
                "latest": format_timestamp(snapshot.get("latest_activity_at"), "No activity"),
                "latest_raw": snapshot.get("latest_activity_at"),
                "state": state,
                "state_class": state_class,
            })
        except (OSError, sqlite3.Error, ValueError):
            rows.append({"id": group_id, "name": "Unavailable group", "members": "—", "unread": "—", "pending": "—", "latest": "Unavailable", "latest_raw": "", "state": "Unavailable", "state_class": "error"})
    return sorted(rows, key=lambda row: (row["state"] == "Healthy", row["name"].lower()))


def overview_events(groups_directory: Optional[Path], limit: int = OVERVIEW_EVENT_LIMIT) -> List[dict]:
    """Assemble a bounded cross-group operational stream from existing stores."""
    events: List[dict] = []
    if groups_directory is None:
        return events
    for group_id in discover_groups(groups_directory):
        try:
            snapshot = read_group_snapshot(groups_directory, group_id)
            group_name = str((snapshot["metadata"] or {}).get("name") or "Untitled group")
            page = read_message_page(groups_directory, group_id, limit=min(limit, MAX_MESSAGE_PAGE_SIZE))
            for message in page["messages"]:
                content = str(message.get("content") or "")
                events.append({"type": "message", "created_at": message.get("created_at"), "group_id": group_id,
                               "group_name": group_name, "sender": message.get("sender_name") or message.get("sender_context_id") or "Unknown",
                               "content": content[:180] + ("…" if len(content) > 180 else ""), "detail": "Message"})
            for wakeup in snapshot["wakeups"]:
                events.append({"type": "wakeup", "created_at": wakeup.get("created_at"), "group_id": group_id,
                               "group_name": group_name, "sender": wakeup.get("context_id") or "Context",
                               "content": "Wakeup for message #{}{}".format(wakeup.get("message_id") or "—", " acknowledged" if wakeup.get("acknowledged_at") else " awaiting acknowledgement"),
                               "detail": "Wakeup", "attention": wakeup.get("acknowledged_at") is None})
            updated = (snapshot["metadata"] or {}).get("updated_at")
            if updated:
                events.append({"type": "group", "created_at": updated, "group_id": group_id, "group_name": group_name,
                               "sender": "Group", "content": "Group settings or membership changed", "detail": "Group change"})
        except (OSError, sqlite3.Error, ValueError):
            continue
    try:
        for call in read_tool_calls(groups_directory, limit=limit):
            failed = call.get("outcome") == "error"
            events.append({"type": "error" if failed else "success", "created_at": call.get("occurred_at"), "group_id": call.get("group_id"),
                           "group_name": call.get("group_name") or (group_display_name(groups_directory, call.get("group_id")) if call.get("group_id") else "MCP"),
                           "sender": format_tool_name(call.get("tool_name")), "content": "{} · {} ms".format(call.get("error_category") or "Tool call failed", call.get("duration_ms") or 0) if failed else "Completed in {} ms".format(call.get("duration_ms") or 0),
                           "detail": "Tool error" if failed else "Tool success", "duration_ms": call.get("duration_ms")})
    except (OSError, sqlite3.Error, ValueError):
        pass
    return sorted(events, key=lambda item: item.get("created_at") or "", reverse=True)[:limit]


def overview_reliability(groups_directory: Optional[Path]) -> dict:
    """Summarize all audit history and retain the 100 newest failed calls."""
    if groups_directory is None or not (groups_directory / "observability.sqlite3").is_file():
        return {"available": False, "calls": 0, "error_rate": "—", "latency": "—", "failures": []}
    try:
        connection = open_read_only_database(groups_directory / "observability.sqlite3")
        try:
            calls = [dict(row) for row in connection.execute("SELECT outcome, duration_ms FROM tool_calls")]
            failures = [dict(row) for row in connection.execute(
                "SELECT occurred_at, tool_name, duration_ms, error_category FROM tool_calls WHERE outcome = 'error' ORDER BY occurred_at DESC, id DESC LIMIT 100"
            )]
        finally:
            connection.close()
    except (OSError, sqlite3.Error, ValueError):
        return {"available": False, "calls": 0, "error_rate": "—", "latency": "—", "failures": []}
    errors = [call for call in calls if call.get("outcome") == "error"]
    durations = [call.get("duration_ms") for call in calls if isinstance(call.get("duration_ms"), int)]
    return {"available": True, "calls": len(calls), "error_rate": "{:.1f}%".format(100 * len(errors) / len(calls)) if calls else "0.0%",
            "latency": "{} ms p95".format(_percentile(durations, .95)) if durations else "—", "failures": failures}


def format_error_category(category: object) -> str:
    """Return an operator-friendly label for a safe stored failure category."""
    return ERROR_CATEGORY_LABELS.get(str(category), "Unknown Error")


def format_tool_name(tool_name: object) -> str:
    """Make stored snake_case MCP tool names readable in operator-facing views."""
    return " ".join(part.capitalize() for part in str(tool_name or "tool").split("_"))


def render_overview(groups_directory: Optional[Path]) -> str:
    data = overview_data(groups_directory)
    totals = data["totals"]
    metric_labels = (("Groups", "groups"), ("Contexts", "contexts"), ("Messages", "messages"), ("Tool calls", "tool_calls"), ("Error rate", "error_rate"), ("Wakeup response", "wakeup_response"))
    metrics = "".join('<article class="metric"><strong>{}</strong><span>{}</span></article>'.format(html.escape(label), totals[key]) for label, key in metric_labels)
    events = overview_events(groups_directory)
    activity = "".join('<li class="event event-{type}{attention}"><span class="event-kind">{detail}</span><div><strong>{sender}</strong> <span class="event-group">{group}</span><p>{content}</p><small>{created}</small></div></li>'.format(
        type=html.escape(str(event["type"])), attention=" is-attention" if event.get("attention") else "",
        detail=html.escape(str(event["detail"])), sender=html.escape(str(event["sender"])), group=html.escape(str(event["group_name"])),
        content=html.escape(str(event["content"])), created=html.escape(format_timestamp(event.get("created_at"), ""))) for event in events) or '<li class="empty-row">No operational events yet.</li>'
    rows = overview_group_rows(groups_directory)
    table_rows = "".join('<tr class="group-row" role="link" tabindex="0" data-view="chats" hx-get="/fragments/chats?group_id={id}" hx-trigger="click, keyup[key==\'Enter\']" hx-target="#chat-panel" hx-swap="innerHTML"><td class="group-name">{name}</td><td>{members}</td><td>{unread}</td><td>{pending}</td><td>{latest}</td><td><span class="status {state_class}">{state}</span></td></tr>'.format(id=html.escape(str(row["id"]), quote=True), name=html.escape(str(row["name"])), members=row["members"], unread=row["unread"], pending=row["pending"], latest=html.escape(str(row["latest"])), state_class=row["state_class"], state=html.escape(str(row["state"]))) for row in rows) or '<tr><td colspan="6" class="notice">No groups found.</td></tr>'
    reliability = overview_reliability(groups_directory)
    failures = "".join('<li class="failure"><strong>{}</strong><span>{} · {} ms</span><small>{}</small></li>'.format(html.escape(format_tool_name(row.get("tool_name"))), html.escape(format_error_category(row.get("error_category"))), html.escape(str(row.get("duration_ms") or 0)), html.escape(format_timestamp(row.get("occurred_at"), ""))) for row in reliability["failures"]) or '<li class="empty-row">No recent tool failures.</li>'
    reliability_body = '<p class="notice error">Audit analytics are disabled. Audit data is unavailable; enable observability to monitor MCP reliability.</p>' if not reliability["available"] else '<div class="reliability-metrics"><span><b>{}</b> calls</span><span><b>{}</b> error rate</span><span><b>{}</b></span></div><ul class="failure-list">{}</ul>'.format(reliability["calls"], reliability["error_rate"], html.escape(reliability["latency"]), failures)
    wakeup_note = "{} acknowledged wakeup{} included".format(totals["acknowledged_wakeups"], "" if totals["acknowledged_wakeups"] == 1 else "s") if totals["acknowledged_wakeups"] else "No acknowledged wakeups yet"
    priority_note = " · ".join('{}: <strong>{}</strong>'.format(html.escape(str(priority)), count) for priority, count in sorted(data["priorities"].items()))
    return '<section class="view overview cockpit" data-overview="true" data-page-title="Overview · operational cockpit" data-page-description="Live conversation activity, group health, and MCP reliability."><header class="view-header"><div><h2>Overview · operational cockpit</h2><p>Live conversation activity, group health, and MCP reliability. {} {}</p></div><span id="activity-indicator" class="overview-live">Live</span></header><div class="metrics">{}</div><div class="cockpit-grid"><section class="panel cockpit-pane activity-pane"><header class="panel-header"><h3>Live activity</h3><span>Cross-group stream</span></header><ul class="activity-list">{}</ul></section><section class="panel cockpit-pane health-pane"><header class="panel-header"><h3>Group health <span class="visually-hidden">Group status</span></h3><button class="panel-action" data-view="chats" hx-get="/fragments/chats" hx-target="#chat-panel" hx-swap="innerHTML">View groups</button></header><div class="table-scroll"><table class="group-table"><thead><tr><th>Group</th><th>Members</th><th>Unread</th><th>Wakeups</th><th>Latest activity</th><th>Status</th></tr></thead><tbody>{}</tbody></table></div></section><section class="panel cockpit-pane reliability-pane"><header class="panel-header"><h3>Reliability</h3><button class="panel-action" data-view="analytics" hx-get="/fragments/analytics?outcome=error" hx-target="#chat-panel" hx-swap="innerHTML">View analytics</button></header>{}</section></div></section>'.format(html.escape(wakeup_note), priority_note, metrics, activity, table_rows, reliability_body)


ANALYTICS_FILTERS = ("from", "to", "group_id", "context_id", "name", "tool_name", "outcome")
ANALYTICS_RANGE_UNITS = (("s", "seconds"), ("m", "minutes"), ("h", "hours"), ("d", "days"), ("M", "months"), ("Y", "years"))
ANALYTICS_RANGE_UNIT_MAP = dict(ANALYTICS_RANGE_UNITS)


def analytics_range_start(value: object, unit: object, now: Optional[datetime] = None) -> Optional[datetime]:
    """Return the start of a valid rolling analytics range, or no range."""
    try:
        amount = int(str(value))
    except (TypeError, ValueError):
        return None
    unit = ANALYTICS_RANGE_UNIT_MAP.get(str(unit), str(unit))
    if amount < 1 or unit not in ANALYTICS_RANGE_UNIT_MAP.values():
        return None
    current = now or datetime.now(timezone.utc)
    if unit == "seconds":
        return current - timedelta(seconds=amount)
    if unit == "minutes":
        return current - timedelta(minutes=amount)
    if unit == "hours":
        return current - timedelta(hours=amount)
    if unit == "days":
        return current - timedelta(days=amount)
    months = amount if unit == "months" else amount * 12
    month_index = current.year * 12 + current.month - 1 - months
    year, month = divmod(month_index, 12)
    month += 1
    return current.replace(year=year, month=month, day=min(current.day, calendar.monthrange(year, month)[1]))


def analytics_filter_options(groups_directory: Optional[Path]) -> dict:
    """Return the stable, human-readable values available for analytics filters."""
    options = {"tool_name": [], "outcome": list(ANALYTICS_OUTCOME_OPTIONS), "name": [], "group_id": []}
    if groups_directory is None or not (groups_directory / "observability.sqlite3").is_file():
        return options
    try:
        connection = open_read_only_database(groups_directory / "observability.sqlite3")
        try:
            for field in ("tool_name", "name"):
                rows = connection.execute(
                    "SELECT DISTINCT {0} FROM tool_calls WHERE {0} IS NOT NULL AND {0} != '' ORDER BY {0}".format(field)
                )
                options[field] = [str(row[0]) for row in rows]
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            query = "SELECT tool_calls.group_id, NULL AS group_name FROM tool_calls WHERE tool_calls.group_id IS NOT NULL AND tool_calls.group_id != '' GROUP BY tool_calls.group_id"
            if "tool_call_group_names" in tables:
                query = "SELECT tool_calls.group_id, MAX(tool_call_group_names.group_name) AS group_name FROM tool_calls LEFT JOIN tool_call_group_names ON tool_call_group_names.tool_call_id = tool_calls.id WHERE tool_calls.group_id IS NOT NULL AND tool_calls.group_id != '' GROUP BY tool_calls.group_id"
            groups = []
            for group_id, stored_name in connection.execute(query):
                deleted = not (groups_directory / (str(group_id) + ".sqlite3")).is_file()
                name = str(stored_name or group_display_name(groups_directory, group_id))
                if deleted and name == "Untitled group":
                    name = "Deleted group"
                groups.append((str(group_id), name + " (deleted)" if deleted else name))
            options["group_id"] = sorted(groups, key=lambda item: item[1].lower())
        finally:
            connection.close()
    except (OSError, sqlite3.Error, ValueError):
        pass
    return options


def _analytics_interval_seconds(call_count: int) -> int:
    """Keep sparse charts precise while bounding visual density for busy periods."""
    if call_count <= 12:
        return 10
    if call_count <= 60:
        return 60
    if call_count <= 240:
        return 300
    return 3600


def _analytics_bucket(timestamp: str, interval_seconds: int) -> str:
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    bucket = datetime.fromtimestamp(int(parsed.timestamp()) // interval_seconds * interval_seconds, timezone.utc)
    if interval_seconds == 3600:
        return bucket.strftime("%Y-%m-%dT%H:00Z")
    if interval_seconds == 300:
        return bucket.strftime("%Y-%m-%dT%H:%M:00Z")
    if interval_seconds == 60:
        return bucket.strftime("%Y-%m-%dT%H:%M:00Z")
    return bucket.strftime("%Y-%m-%dT%H:%M:%SZ")


def read_tool_analytics(groups_directory: Optional[Path], filters: Optional[dict] = None) -> dict:
    """Filter raw audit calls and derive dashboard metrics without aggregate tables."""
    filters = filters or {}
    if groups_directory is None or not (groups_directory / "observability.sqlite3").is_file():
        return {"available": False, "rows": [], "by_tool": {}, "by_caller": {}, "by_time": {}, "by_outcome": {}, "durations": [], "interval_seconds": 3600}
    clauses, values = [], []
    for field in ANALYTICS_FILTERS:
        value = filters.get(field)
        if not value:
            continue
        if field == "outcome" and str(value).startswith("error:"):
            category = str(value).split(":", 1)[1]
            if category in ERROR_CATEGORY_LABELS:
                clauses.extend(("outcome = ?", "error_category = ?"))
                values.extend(("error", category))
                continue
        column = "occurred_at" if field in {"from", "to"} else field
        operator = ">=" if field == "from" else "<=" if field == "to" else "="
        clauses.append(column + " " + operator + " ?")
        values.append(value)
    query = "SELECT tool_calls.id, occurred_at, tool_name, group_id, NULL AS group_name, context_id, name, outcome, duration_ms, error_category FROM tool_calls"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY occurred_at DESC"
    try:
        connection = open_read_only_database(groups_directory / "observability.sqlite3")
        try:
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            if "tool_call_group_names" in tables:
                query = query.replace("NULL AS group_name", "tool_call_group_names.group_name").replace("FROM tool_calls", "FROM tool_calls LEFT JOIN tool_call_group_names ON tool_call_group_names.tool_call_id = tool_calls.id")
            rows = [dict(row) for row in connection.execute(query, values)]
        finally:
            connection.close()
    except (OSError, sqlite3.Error, ValueError):
        return {"available": False, "rows": [], "by_tool": {}, "by_caller": {}, "by_time": {}, "by_outcome": {}, "durations": [], "interval_seconds": 3600}
    by_tool: dict = {}
    by_caller: dict = {}
    by_time: dict = {}
    by_outcome: dict = {}
    interval_seconds = _analytics_interval_seconds(len(rows))
    for row in rows:
        group_id = row.get("group_id")
        row["group_deleted"] = bool(group_id) and not (groups_directory / (group_id + ".sqlite3")).is_file()
        item = by_tool.setdefault(row["tool_name"], {"count": 0, "errors": 0})
        item["count"] += 1
        item["errors"] += row["outcome"] == "error"
        caller = str(row.get("name") or "Unknown caller")
        by_caller[caller] = by_caller.get(caller, 0) + 1
        bucket = _analytics_bucket(row["occurred_at"], interval_seconds)
        by_time[bucket] = by_time.get(bucket, 0) + 1
        outcome_label = "Success" if row["outcome"] == "success" else format_error_category(row.get("error_category"))
        by_outcome[outcome_label] = by_outcome.get(outcome_label, 0) + 1
    return {"available": True, "rows": rows, "by_tool": by_tool, "by_caller": by_caller, "by_time": by_time, "by_outcome": by_outcome, "durations": [row["duration_ms"] for row in rows], "interval_seconds": interval_seconds}


def _percentile(values: List[int], percentile: float) -> Optional[int]:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * percentile) - 1))
    return ordered[index]


def _chart_label(value: object) -> str:
    """Make machine-style chart names readable without altering normal names."""
    text = str(value).replace("_", " ")
    return text[:1].upper() + text[1:]


def _echarts_host(kind: str, label: str, values: dict, show_legend: bool = False) -> str:
    data = [
        {"name": _chart_label(name), "value": item["count"] if isinstance(item, dict) else item}
        for name, item in sorted(values.items())
    ]
    payload = html.escape(json.dumps({"kind": kind, "label": label, "data": data, "legend": show_legend}, separators=(",", ":")), quote=True)
    return '<div class="echarts-host" data-echarts="{}" role="img" aria-label="{}"></div>'.format(payload, html.escape(label, quote=True))


def _analytics_chart(values: dict, label: str) -> str:
    if not values:
        return '<p class="notice">No calls match this range.</p>'
    total = sum(item["count"] if isinstance(item, dict) else item for item in values.values())
    return '<section class="chart-card"><header class="chart-heading"><div><p class="chart-kicker">Analytics</p><h4>{}</h4></div><span class="chart-total">{} total</span></header>{}<p class="chart-caption">Hover a bar for its exact value.</p></section>'.format(html.escape(label), total, _echarts_host("bar", label, values))


def _analytics_line_chart(values: dict, label: str) -> str:
    if not values:
        return '<p class="notice">No calls match this range.</p>'
    return '<section class="chart-card"><header class="chart-heading"><div><p class="chart-kicker">Activity</p><h4>{}</h4></div><span class="chart-total">{} points</span></header>{}<p class="chart-caption">Hover a point for its exact value.</p></section><!-- chart-line -->'.format(html.escape(label), len(values), _echarts_host("line", label, values))


def _analytics_donut_chart(values: dict, label: str, show_legend: bool = False) -> str:
    if not values:
        return '<p class="notice">No calls match this range.</p>'
    return '<section class="chart-card chart-card-donut"><header class="chart-heading"><div><p class="chart-kicker">Reliability</p><h4>{}</h4></div></header><div class="doughnut-content">{}</div></section><!-- chart-ring -->'.format(
        html.escape(label), _echarts_host("donut", label, values, show_legend),
    )


def _echarts_data(data: dict) -> str:
    """Serialize only labels and counts for client-side chart rendering."""
    outcome_total = sum(data["by_outcome"].values())
    payload = {
        "tools": [{"label": str(key), "value": value["count"]} for key, value in sorted(data["by_tool"].items())],
        "activity": [{"label": format_timestamp(key, ""), "value": value} for key, value in sorted(data["by_time"].items())],
        "outcomes": [{"name": "{}: {:.1f}%".format(key, value * 100 / outcome_total).replace(".0%", "%"), "value": value} for key, value in sorted(data["by_outcome"].items())],
    }
    return html.escape(json.dumps(payload, separators=(",", ":")), quote=True)


def render_tool_analytics(groups_directory: Optional[Path], filters: Optional[dict] = None) -> str:
    filters = dict(filters or {})
    range_start = analytics_range_start(filters.get("range_value"), filters.get("range_unit"))
    if range_start is not None:
        filters["from"] = range_start.isoformat()
    data = read_tool_analytics(groups_directory, filters)
    options = analytics_filter_options(groups_directory)
    range_value = str(filters.get("range_value") or "0")
    range_unit = str(filters.get("range_unit") or "h")
    if range_unit not in ANALYTICS_RANGE_UNIT_MAP:
        range_unit = "h"
    fields = '<label class="range-control">Time range<span class="range-composite"><input type="number" min="0" step="1" name="range_value" value="{}" aria-label="Time range amount"><span class="range-unit" data-range-unit>{}</span><input type="hidden" name="range_unit" value="{}"><span class="range-unit-stepper"><button type="button" data-range-step="1" aria-label="Next time unit">&#9652;</button><button type="button" data-range-step="-1" aria-label="Previous time unit">&#9662;</button></span></span></label>'.format(
        html.escape(range_value, quote=True), html.escape(ANALYTICS_RANGE_UNIT_MAP[range_unit]), html.escape(range_unit, quote=True),
    )
    for label, field, empty_label in (("Tool", "tool_name", "Any tool"), ("Outcome", "outcome", "Any outcome"), ("Caller", "name", "Any caller"), ("Group", "group_id", "Any group")):
        selected = str(filters.get(field, ""))
        values = options[field]
        if field == "group_id":
            if selected and selected not in [value for value, _ in values]:
                values = [(selected, "Unavailable group (deleted)")] + values
            choices = "".join('<option value="{}"{}>{}</option>'.format(html.escape(value, quote=True), " selected" if value == selected else "", html.escape(label)) for value, label in values)
        elif field == "outcome":
            choices = "".join('<option value="{}"{}>{}</option>'.format(html.escape(value, quote=True), " selected" if value == selected else "", html.escape(label)) for value, label in values)
        else:
            if selected and selected not in values:
                values = [selected] + values
            choices = "".join('<option value="{}"{}>{}</option>'.format(html.escape(value, quote=True), " selected" if value == selected else "", html.escape(value)) for value in values)
        fields += '<label>{}<select name="{}"><option value="">{}</option>{}</select></label>'.format(
            label, field, empty_label, choices,
        )
    preserved_filters = "".join(
        '<input type="hidden" name="{}" value="{}">'.format(html.escape(field), html.escape(str(filters.get(field, "")), quote=True))
        for field in ("group_id", "context_id", "from", "to") if filters.get(field) and not (field == "from" and range_start is not None)
    )
    fields += preserved_filters
    form = '<form hx-get="/fragments/analytics" hx-target="#chat-panel" hx-swap="innerHTML"><header class="view-header"><div><h2>Tool analytics</h2><p>Tool volume, reliability, and latency from audit history.</p></div></header><div class="filters">{}</div></form>'.format(fields)
    if not data["available"]:
        return '<section class="view analytics" data-analytics="true" data-page-title="Tool analytics" data-page-description="Tool volume, reliability, and latency from audit history.">{}<p class="notice error">Audit data is unavailable. Enable auditing to begin collecting tool analytics.</p></section>'.format(form)
    p50, p95 = _percentile(data["durations"], .50), _percentile(data["durations"], .95)
    errors = sum(1 for row in data["rows"] if row["outcome"] == "error")
    error_rate = "{:.1f}%".format(errors * 100 / len(data["rows"])) if data["rows"] else "0.0%"
    metrics = '<div class="metrics"><article class="metric"><strong>Total calls</strong><span>{}</span></article><article class="metric"><strong>p50 / p95 latency</strong><span>{} / {} ms</span></article><article class="metric{}"><strong>Error rate</strong><span>{}</span></article></div>'.format(len(data["rows"]), p50 if p50 is not None else "—", p95 if p95 is not None else "—", " is-alert" if errors else "", error_rate)
    summary = '<div class="analytics-summary-row">{}{}</div>'.format(metrics, _analytics_donut_chart(data["by_tool"], "Calls by tool", show_legend=True) + _analytics_donut_chart(data["by_outcome"], "Call outcomes", show_legend=True))
    row_items = []
    for row in data["rows"]:
        group_name = row.get("group_name") or ("Deleted group (deleted)" if row["group_deleted"] else group_display_name(groups_directory, row["group_id"]))
        outcome_label = "Success" if row["outcome"] == "success" else format_error_category(row.get("error_category"))
        row_items.append('<tr><td>{}</td><td>{}</td><td>{}</td><td class="outcome outcome-{}">{}</td><td>{}</td><td>{} ms</td></tr>'.format(
            html.escape(format_timestamp(row["occurred_at"])), html.escape(row["tool_name"]), html.escape(str(row["name"] or "—")),
            html.escape(str(row["outcome"])), html.escape(outcome_label), html.escape(str(group_name)), row["duration_ms"],
        ))
    rows = "".join(row_items) or "<tr><td colspan=\"6\">No calls match this range.</td></tr>"
    displayed_by_time = {format_timestamp(bucket, ""): count for bucket, count in data["by_time"].items()}
    charts = '<!-- class="echarts-host-native" data-echarts="{}" --><div class="chart-gallery">{}{}</div>'.format(_echarts_data(data), _analytics_line_chart(displayed_by_time, "Calls over time"), _analytics_chart(data["by_caller"], "Calls by caller"))
    columns = '<colgroup><col style="width:22%"><col style="width:18%"><col style="width:18%"><col style="width:12%"><col style="width:20%"><col style="width:10%"></colgroup>'
    log_header = '<thead><tr><th>When</th><th>Tool</th><th>Caller</th><th>Outcome</th><th>Group</th><th>Duration</th></tr></thead>'
    return '<section class="view analytics" data-analytics="true" data-page-title="Tool analytics" data-page-description="Tool volume, reliability, and latency from audit history.">{}{}{}<section class="analytics-log"><header><h3>Event log</h3><span>{} matching calls</span></header><div class="event-log-table"><table class="event-log-header">{}{}</table><div class="event-log-scroll"><table>{}<tbody>{}</tbody></table></div></div></section></section>'.format(form, summary, charts, len(data["rows"]), columns, log_header, columns, rows)


def audit_storage_status(groups_directory: Optional[Path], environment: Optional[dict] = None) -> dict:
    """Read audit storage state without creating or modifying the database."""
    status = {"available": False, "retention": "unavailable", "size_bytes": 0, "row_count": 0, "reclaimable_bytes": 0, "metadata": None}
    if groups_directory is None:
        return status
    path = groups_directory / "observability.sqlite3"
    if not path.is_file():
        return status
    try:
        connection = open_read_only_database(path)
        try:
            status["row_count"] = connection.execute("SELECT COUNT(*) FROM tool_calls").fetchone()[0]
            page_size = connection.execute("PRAGMA page_size").fetchone()[0]
            free_pages = connection.execute("PRAGMA freelist_count").fetchone()[0]
            status["reclaimable_bytes"] = page_size * free_pages
        finally:
            connection.close()
        status["metadata"] = read_audit_metadata(groups_directory)
        status["retention"] = (status["metadata"] or {}).get("retention_setting") or "unknown (legacy audit database)"
        status["size_bytes"] = path.stat().st_size
        status["available"] = True
    except (OSError, sqlite3.Error, ValueError):
        status["error"] = "Audit storage is temporarily unavailable; retry shortly."
    return status


def _bytes(value: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if value < 1024 or unit == "GiB":
            return "{} {}".format(value if unit == "B" else round(value, 1), unit)
        value /= 1024
    return str(value)


def reclaim_audit_free_space(groups_directory: Optional[Path], pages: int = 100) -> dict:
    """Run one bounded incremental-vacuum step against audit storage only."""
    if groups_directory is None or not isinstance(pages, int) or not 1 <= pages <= 1000:
        return {"ok": False, "message": "Audit storage is unavailable."}
    path = groups_directory / "observability.sqlite3"
    if not path.is_file():
        return {"ok": False, "message": "No audit database exists yet."}
    try:
        connection = sqlite3.connect(str(path), timeout=0.1)
        try:
            connection.execute("PRAGMA busy_timeout=100")
            # This is deliberately bounded and does not delete retained history.
            connection.execute("PRAGMA incremental_vacuum(" + str(pages) + ")")
            connection.commit()
        finally:
            connection.close()
    except (OSError, sqlite3.Error):
        return {"ok": False, "message": "Audit storage is busy; retry later."}
    return {"ok": True, "message": "Reclaimed available audit free pages."}


def delete_audit_history(groups_directory: Optional[Path]) -> dict:
    """Immediately delete audit rows only; reclaiming freed pages remains separate."""
    if groups_directory is None:
        return {"ok": False, "message": "Audit storage is unavailable."}
    path = groups_directory / "observability.sqlite3"
    if not path.is_file():
        return {"ok": False, "message": "No audit database exists yet."}
    try:
        connection = sqlite3.connect(str(path), timeout=0.1)
        try:
            connection.execute("PRAGMA busy_timeout=100")
            deleted = connection.execute("DELETE FROM tool_calls").rowcount
            connection.commit()
        finally:
            connection.close()
    except (OSError, sqlite3.Error):
        return {"ok": False, "message": "Audit storage is busy; retry later."}
    return {"ok": True, "message": "Deleted {} audit-history rows. Reclaim free space separately if needed.".format(deleted)}


def render_storage(groups_directory: Optional[Path], csrf_token: Optional[str] = None) -> str:
    status = audit_storage_status(groups_directory)
    if not status["available"]:
        message = status.get("error") or "No audit database exists yet. Enable auditing and make a tool call to create it."
        return '<section class="view storage" data-storage="true" data-page-title="Storage" data-page-description="Audit history maintenance for this local observer."><header class="view-header"><div><h2>Storage</h2><p>Audit history maintenance for this local observer.</p></div></header><p class="notice error">{}</p></section>'.format(html.escape(message))
    metadata = status["metadata"] or {}
    values = (("Audit file", _bytes(status["size_bytes"])), ("Audit rows", str(status["row_count"])), ("Reclaimable", _bytes(status["reclaimable_bytes"])), ("Retention", str(status["retention"])), ("Activated", format_timestamp(metadata.get("audit_enabled_at"))), ("Last cleanup", format_timestamp(metadata.get("last_retention_cleanup_at"), "Never")))
    details = "".join('<dt>{}</dt><dd>{}</dd>'.format(html.escape(label), html.escape(value)) for label, value in values)
    control = '<p class="notice">Maintenance controls are available after the next update.</p>'
    if csrf_token is not None:
        token = html.escape(csrf_token, quote=True)
        control = '<button hx-post="/api/storage/reclaim" hx-target="#maintenance-status" hx-swap="innerHTML" hx-headers=\'{"X-CSRF-Token":"%s"}\'>Reclaim free space</button><button class="danger" hx-post="/api/storage/delete-history" hx-target="#maintenance-status" hx-swap="innerHTML" hx-confirm="Permanently delete all audit history? This cannot be undone." hx-headers=\'{"X-CSRF-Token":"%s","X-Crosstalk-Confirm":"DELETE AUDIT HISTORY"}\'>Delete audit history</button><div id="maintenance-status"></div>' % (token, token)
    return '<section class="view storage" data-storage="true" data-page-title="Storage" data-page-description="Audit history maintenance for this local observer."><header class="view-header"><div><h2>Storage</h2><p>Audit history maintenance for this local observer.</p></div></header><p class="notice">Audit storage is separate from group databases. Reclaiming free pages never deletes retained audit history.</p><dl class="storage-details">{}</dl>{}</section>'.format(details, control)


def _legacy_render_dashboard(groups_directory: Optional[Path], csrf_token: Optional[str] = None) -> str:
    groups = discover_groups(groups_directory) if groups_directory is not None else []
    selected = groups[0] if groups else None
    picker = "".join(
        '<button hx-get="/fragments/chat?group_id={id}" hx-target="#chat-panel" hx-swap="innerHTML" data-group-id="{id}" title="{id}">{name}</button>'.format(id=html.escape(group_id), name=html.escape(group_display_name(groups_directory, group_id)))
        for group_id in groups
    ) or '<p class="notice">No groups found. This page will update when a group database appears.</p>'
    return """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Crosstalk observer</title>
<link rel="stylesheet" href="/static/observer.css">
<script>try{var savedTheme=localStorage.getItem('crosstalk-theme');document.documentElement.dataset.theme=savedTheme||((window.matchMedia&&window.matchMedia('(prefers-color-scheme: light)').matches)?'light':'dark')}catch(e){}document.addEventListener('DOMContentLoaded',function(){var button=document.getElementById('theme-toggle');if(!button)return;function update(theme){document.documentElement.dataset.theme=theme;button.textContent=theme==='dark'?'Switch to light':'Switch to dark';button.setAttribute('aria-pressed',String(theme==='dark'));try{localStorage.setItem('crosstalk-theme',theme)}catch(e){}}update(document.documentElement.dataset.theme||'dark');button.addEventListener('click',function(){update(document.documentElement.dataset.theme==='dark'?'light':'dark')})})</script>
<script>(function(){var loading;function load(){if(window.echarts)return Promise.resolve(window.echarts);if(loading)return loading;loading=new Promise(function(ok,bad){var s=document.createElement('script');s.src='https://cdn.jsdelivr.net/npm/echarts@6.1.0/dist/echarts.min.js';s.integrity='sha384-C2iskrW/uPW46KzOjrvJIQo4YkV8lkD+QS0CrDN18IIPIpT/g2USu8bTP3nvmIAD';s.crossOrigin='anonymous';s.onload=function(){ok(window.echarts)};s.onerror=bad;document.head.appendChild(s)});return loading}function card(title,caption){return '<section class="echarts-card"><h4>'+title+'</h4><p>'+caption+'</p><div class="echart"></div></section>'}function render(host){if(host.dataset.rendered)return;var d;try{d=JSON.parse(host.dataset.echarts)}catch(e){return}load().then(function(e){host.dataset.rendered='1';host.innerHTML='<div class="echarts-gallery">'+card('Calls by tool','Volume by MCP tool')+card('Calls over time','Recent activity')+card('Call outcomes','Success and error distribution')+'</div>';var dark=document.documentElement.dataset.theme!=='light',text=dark?'#aebdce':'#65758b',grid=dark?'#33445d':'#dce5f0',blue=dark?'#60a5fa':'#3b82f6',nodes=host.querySelectorAll('.echart'),common={textStyle:{fontFamily:'system-ui'},tooltip:{trigger:'axis'},grid:{left:28,right:14,top:18,bottom:50},xAxis:{type:'category',axisLabel:{color:text,hideOverlap:true},axisLine:{lineStyle:{color:grid}},axisTick:{show:false}},yAxis:{type:'value',axisLabel:{color:text},splitLine:{lineStyle:{color:grid,type:'dashed'}}}};var a=e.init(nodes[0],null,{renderer:'svg'}),b=e.init(nodes[1],null,{renderer:'svg'}),c=e.init(nodes[2],null,{renderer:'svg'});a.setOption(Object.assign({},common,{xAxis:Object.assign({},common.xAxis,{data:d.tools.map(function(x){return x.label})}),series:[{type:'bar',data:d.tools.map(function(x){return x.value}),barMaxWidth:38,itemStyle:{color:blue,borderRadius:[5,5,0,0]}}]}));b.setOption(Object.assign({},common,{xAxis:Object.assign({},common.xAxis,{data:d.activity.map(function(x){return x.label})}),series:[{type:'line',smooth:true,data:d.activity.map(function(x){return x.value}),lineStyle:{color:blue,width:3},itemStyle:{color:blue},areaStyle:{color:dark?'rgba(96,165,250,.18)':'rgba(59,130,246,.15)'}}]}));c.setOption({color:[blue,'#f87171','#fbbf24','#a78bfa'],textStyle:{fontFamily:'system-ui'},tooltip:{trigger:'item'},legend:{bottom:0,textStyle:{color:text}},series:[{type:'pie',radius:['50%','74%'],label:{show:false},data:d.outcomes}]});new ResizeObserver(function(){a.resize();b.resize();c.resize()}).observe(host)}).catch(function(){host.insertAdjacentHTML('afterbegin','<p class="notice error">Interactive charts could not load; showing the built-in charts instead.</p>')})}function all(){document.querySelectorAll('.echarts-host').forEach(render)}document.addEventListener('DOMContentLoaded',function(){all();new MutationObserver(all).observe(document.getElementById('chat-panel'),{childList:true,subtree:true})})})();</script>
<script>(function(){var promise;function load(){if(window.echarts)return Promise.resolve(window.echarts);if(promise)return promise;promise=new Promise(function(resolve,reject){var script=document.createElement('script');script.src='https://cdn.jsdelivr.net/npm/echarts@6.1.0/dist/echarts.min.js';script.integrity='sha384-C2iskrW/uPW46KzOjrvJIQo4YkV8lkD+QS0CrDN18IIPIpT/g2USu8bTP3nvmIAD';script.crossOrigin='anonymous';script.onload=function(){resolve(window.echarts)};script.onerror=reject;document.head.appendChild(script)});return promise}function card(title,description){return '<section class="echarts-card"><h4>'+title+'</h4><p>'+description+'</p><div class="echart"></div></section>'}function render(host){if(host.dataset.rendered)return;var data;try{data=JSON.parse(host.dataset.echarts)}catch(e){return}load().then(function(echarts){host.dataset.rendered='true';host.innerHTML='<div class="echarts-gallery">'+card('Calls by tool','Volume by MCP tool')+card('Calls over time','Recent activity')+card('Call outcomes','Success and error distribution')+'</div>';var dark=document.documentElement.dataset.theme!=='light',text=dark?'#aebdce':'#65758b',grid=dark?'#33445d':'#dce5f0',blue=dark?'#60a5fa':'#3b82f6',nodes=host.querySelectorAll('.echart'),base={textStyle:{fontFamily:'system-ui'},grid:{left:28,right:14,top:18,bottom:46},xAxis:{type:'category',axisLine:{lineStyle:{color:grid}},axisTick:{show:false},axisLabel:{color:text}},yAxis:{type:'value',axisLabel:{color:text},splitLine:{lineStyle:{color:grid,type:'dashed'}}}};var tools=echarts.init(nodes[0],null,{renderer:'svg'}),activity=echarts.init(nodes[1],null,{renderer:'svg'}),outcomes=echarts.init(nodes[2],null,{renderer:'svg'});tools.setOption(Object.assign({},base,{tooltip:{trigger:'axis'},xAxis:Object.assign({},base.xAxis,{data:data.tools.map(function(item){return item.label}),axisLabel:{show:data.tools.length<=6,interval:0,color:text}}),series:[{type:'bar',data:data.tools.map(function(item){return item.value}),barMaxWidth:38,itemStyle:{color:blue,borderRadius:[5,5,0,0]}}]}));activity.setOption(Object.assign({},base,{tooltip:{trigger:'axis'},xAxis:Object.assign({},base.xAxis,{data:data.activity.map(function(item){return item.label}),axisLabel:{color:text,hideOverlap:true}}),series:[{type:'line',smooth:true,showSymbol:false,data:data.activity.map(function(item){return item.value}),lineStyle:{color:blue,width:2.5},itemStyle:{color:blue},areaStyle:{color:dark?'rgba(96,165,250,.24)':'rgba(59,130,246,.2)'}}]}));outcomes.setOption({color:[blue,'#f87171','#fbbf24','#a78bfa'],textStyle:{fontFamily:'system-ui'},tooltip:{trigger:'item',formatter:function(point){return point.name+': '+point.value+' calls ('+point.percent+'%)'}},legend:{bottom:0,textStyle:{color:text}},series:[{type:'pie',radius:['50%','74%'],label:{show:false},data:data.outcomes}]});new ResizeObserver(function(){tools.resize();activity.resize();outcomes.resize()}).observe(host)}).catch(function(){host.insertAdjacentHTML('afterbegin','<p class="notice error">Interactive charts could not load; showing the built-in charts instead.</p>')})}function renderAll(){document.querySelectorAll('.echarts-host-native').forEach(render)}document.addEventListener('DOMContentLoaded',function(){renderAll();new MutationObserver(renderAll).observe(document.getElementById('chat-panel'),{childList:true,subtree:true})})})();</script>
<script>(function(){function decorate(host,attempt){var nodes=host.querySelectorAll('.echart');if(!window.echarts||nodes.length!==3){if(attempt<20)setTimeout(function(){decorate(host,attempt+1)},50);return}if(host.dataset.outcomesDecorated)return;var chart=window.echarts.getInstanceByDom(nodes[2]);if(!chart){if(attempt<20)setTimeout(function(){decorate(host,attempt+1)},50);return}var data=JSON.parse(host.dataset.echarts),total=data.outcomes.reduce(function(sum,item){return sum+item.value},0),dark=document.documentElement.dataset.theme!=='light';chart.setOption({legend:{show:false},tooltip:{trigger:'item',formatter:function(point){return point.name}},graphic:[{type:'text',left:'center',top:'39%',style:{text:String(total)+'\\nCalls',textAlign:'center',fill:dark?'#e7edf7':'#1b2b42',font:'700 18px system-ui',lineHeight:23}}]});host.dataset.outcomesDecorated='true'}function decorateAll(){document.querySelectorAll('.echarts-host-native').forEach(function(host){decorate(host,0)})}document.addEventListener('DOMContentLoaded',function(){decorateAll();document.body.addEventListener('htmx:afterSettle',function(){setTimeout(decorateAll,0)})})})();</script>
<script>(function(){function area(host,attempt){var nodes=host.querySelectorAll('.echart');if(!window.echarts||nodes.length!==3){if(attempt<20)setTimeout(function(){area(host,attempt+1)},50);return}if(host.dataset.areaDecorated)return;var chart=window.echarts.getInstanceByDom(nodes[1]);if(!chart){if(attempt<20)setTimeout(function(){area(host,attempt+1)},50);return}chart.setOption({series:[{type:'line',showSymbol:false,areaStyle:{opacity:.48}}]});host.dataset.areaDecorated='true'}function areaAll(){document.querySelectorAll('.echarts-host-native').forEach(function(host){area(host,0)})}document.addEventListener('DOMContentLoaded',function(){areaAll();document.body.addEventListener('htmx:afterSettle',function(){setTimeout(areaAll,0)})})})();</script>
<script>(function(){function arrange(host,attempt){var cards=host.querySelectorAll('.echarts-card'),metrics=document.querySelector('.analytics > .metrics');if(cards.length!==3||!metrics){if(attempt<20)setTimeout(function(){arrange(host,attempt+1)},50);return}if(cards[2].querySelector('.kpi-stack'))return;metrics.classList.add('kpi-stack');cards[2].appendChild(metrics)}function arrangeAll(){document.querySelectorAll('.echarts-host-native').forEach(function(host){arrange(host,0)})}document.addEventListener('DOMContentLoaded',function(){arrangeAll();document.body.addEventListener('htmx:afterSettle',function(){setTimeout(arrangeAll,0)})})})();</script>
<style>:root[data-theme="light"] body{background:radial-gradient(circle at 72% -20%,#c8e0fb 0,transparent 35rem),var(--bg)!important}</style>
<script src="https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js" integrity="sha384-HGfztofotfshcF7+8n44JQL2oJmowVChPTg48S+jvZoztPfvwD79OC/LTtG6dMp+" crossorigin="anonymous"></script>
<script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.14.8/dist/cdn.min.js" integrity="sha384-X9kJyAubVxnP0hcA+AMMs21U445qsnqhnUF8EBlEpP3a42Kh/JwWjlv2ZcvGfphb" crossorigin="anonymous"></script>
<style>:root{color-scheme:dark;--bg:#090d18;--surface:#111827;--surface-raised:#182235;--surface-hover:#202d44;--border:#293752;--text:#edf3ff;--muted:#9eacc2;--accent:#72b7ff;--accent-strong:#3797ff;--danger:#ff9b9b;--shadow:0 18px 45px rgba(0,0,0,.22)}@media(prefers-color-scheme:light){:root:not([data-theme]){color-scheme:light;--bg:#eff5fc;--surface:#ffffff;--surface-raised:#f6f9fe;--surface-hover:#e9f1fb;--border:#cdd9e8;--text:#152238;--muted:#5b6d84;--accent:#176bc2;--accent-strong:#1474d4;--danger:#b62d3d;--shadow:0 18px 45px rgba(44,67,97,.14)}}:root[data-theme="light"]{color-scheme:light;--bg:#eff5fc;--surface:#ffffff;--surface-raised:#f6f9fe;--surface-hover:#e9f1fb;--border:#cdd9e8;--text:#152238;--muted:#5b6d84;--accent:#176bc2;--accent-strong:#1474d4;--danger:#b62d3d;--shadow:0 18px 45px rgba(44,67,97,.14)}*{box-sizing:border-box}body{min-height:100vh;margin:0;background:radial-gradient(circle at 72% -20%,#1c3b63 0,transparent 35rem),var(--bg);color:var(--text);font:15px/1.5 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}:root[data-theme="light"] body,@media(prefers-color-scheme:light){body{background:radial-gradient(circle at 72% -20%,#c8e0fb 0,transparent 35rem),var(--bg)}}main{max-width:1360px;margin:auto;padding:32px 24px 48px}main>header{display:flex;align-items:end;justify-content:space-between;gap:24px;padding:0 4px 28px}h1,h2,h3,p{margin-top:0}h1{margin-bottom:4px;font-size:clamp(1.65rem,4vw,2.35rem);line-height:1.1;letter-spacing:-.04em}main>header p{margin:0;color:var(--muted)}#activity-indicator{display:inline-block;color:var(--accent);font-weight:650}.theme-toggle{width:auto;margin:0;padding:9px 12px;color:var(--muted);white-space:nowrap}.layout{display:grid;grid-template-columns:250px minmax(0,1fr);gap:24px;align-items:start}nav{position:sticky;top:20px;padding:14px;background:rgba(17,24,39,.86);border:1px solid var(--border);border-radius:16px;box-shadow:var(--shadow);backdrop-filter:blur(14px)}:root[data-theme="light"] nav{background:rgba(255,255,255,.86)}nav h2{margin:22px 8px 8px;color:var(--muted);font-size:.72rem;letter-spacing:.12em;text-transform:uppercase}button{display:block;width:100%;margin:3px 0;padding:10px 12px;border:1px solid transparent;border-radius:9px;background:transparent;color:var(--text);font:inherit;font-weight:560;text-align:left;cursor:pointer;transition:background .16s,border-color .16s,transform .16s}button:hover{background:var(--surface-hover);border-color:#395273}button:focus-visible{outline:3px solid rgba(114,183,255,.55);outline-offset:2px}button:active{transform:translateY(1px)}.layout>#chat-panel{min-width:0;padding:26px;background:rgba(17,24,39,.87);border:1px solid var(--border);border-radius:18px;box-shadow:var(--shadow);backdrop-filter:blur(14px)}:root[data-theme="light"] .layout>#chat-panel{background:rgba(255,255,255,.87)}#chat-panel>section>header{padding-bottom:16px;border-bottom:1px solid var(--border)}#chat-panel h2{margin-bottom:4px;font-size:1.35rem;letter-spacing:-.02em}#chat-panel h3{margin-bottom:10px;font-size:.86rem;color:#c6d5ea}:root[data-theme="light"] #chat-panel h3{color:#344b69}.message{position:relative;margin:12px 0;padding:15px 16px 14px 18px;overflow:hidden;background:linear-gradient(120deg,rgba(35,55,83,.75),var(--surface-raised));border:1px solid #2b3c59;border-radius:12px;box-shadow:0 5px 15px rgba(0,0,0,.12)}:root[data-theme="light"] .message{background:linear-gradient(120deg,#edf5ff,var(--surface-raised));border-color:#cbd9eb;box-shadow:0 5px 15px rgba(44,67,97,.08)}.message:before{position:absolute;top:0;bottom:0;left:0;width:3px;background:var(--accent-strong);content:""}.message header,.message footer{display:flex;gap:12px;justify-content:space-between;color:var(--muted);font-size:.79rem}.message header strong{color:#dceaff}:root[data-theme="light"] .message header strong{color:#233956}.message p{margin:12px 0;white-space:pre-wrap;overflow-wrap:anywhere}.message footer{padding-top:10px;border-top:1px solid rgba(119,146,183,.18)}.notice{color:var(--muted)}.error{color:var(--danger)}.chat{display:flow-root}.chat>aside{float:right;width:220px;margin:0 0 18px 24px;padding:15px;background:var(--surface-raised);border:1px solid var(--border);border-radius:12px}.chat aside ul,.overview ul{padding:0;margin:0;list-style:none}.chat aside li{padding:8px 0;border-bottom:1px solid rgba(119,146,183,.16)}.chat aside li:last-child{border-bottom:0}.chat aside small,.overview small{display:block;color:var(--muted)}.metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(128px,1fr));gap:12px;margin-bottom:24px}.metric{min-height:96px;padding:15px;background:linear-gradient(145deg,var(--surface-raised),#141d2d);border:1px solid var(--border);border-radius:12px}:root[data-theme="light"] .metric{background:linear-gradient(145deg,#fff,var(--surface-raised))}.metric strong,.metric span{display:block}.metric strong{color:var(--muted);font-size:.75rem;font-weight:650;letter-spacing:.04em;text-transform:uppercase}.metric span{margin-top:8px;font-size:1.6rem;font-weight:680;letter-spacing:-.035em}.overview-columns{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:18px}.overview-columns>section{padding:18px;background:var(--surface-raised);border:1px solid var(--border);border-radius:12px}.overview li{padding:8px 0;border-bottom:1px solid rgba(119,146,183,.16)}.overview li:last-child{border-bottom:0}.filters{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-bottom:22px}.filters label{color:var(--muted);font-size:.78rem;font-weight:650;text-transform:capitalize}.filters input{display:block;width:100%;margin-top:5px;padding:9px 10px;background:#0d1422;border:1px solid #344562;border-radius:8px;color:var(--text);font:inherit}:root[data-theme="light"] .filters input,:root[data-theme="light"] .chart{background:#fff;border-color:#cbd9eb}.filters input:focus{border-color:var(--accent);outline:2px solid rgba(114,183,255,.2)}.filters button{align-self:end;background:var(--accent-strong);color:#061222;font-weight:750;text-align:center}.filters button:hover{background:#8bc5ff;border-color:#8bc5ff}.chart{width:100%;height:auto;margin-bottom:18px;padding:8px;background:#0d1422;border:1px solid var(--border);border-radius:10px}.chart .bar{fill:var(--accent-strong)}.chart text{fill:#dceaff;font-size:8px}:root[data-theme="light"] .chart text{fill:#233956}.chart line{stroke:#657895}.analytics{overflow:hidden}.analytics table{width:100%;margin-top:16px;border:1px solid var(--border);border-collapse:separate;border-spacing:0;border-radius:10px;overflow:hidden}.analytics th{background:#202c41;color:#c5d5ec;font-size:.72rem;letter-spacing:.05em;text-transform:uppercase}:root[data-theme="light"] .analytics th,:root[data-theme="light"] .storage-details dt{background:#e7eff9;color:#344b69}.analytics td,.analytics th{padding:10px 12px;text-align:left;border-bottom:1px solid var(--border)}.analytics tr:last-child td{border-bottom:0}.analytics tbody tr:nth-child(even){background:rgba(33,46,68,.42)}:root[data-theme="light"] .analytics tbody tr:nth-child(even){background:#f3f7fc}.analytics tbody tr:hover{background:var(--surface-hover)}.storage-details{display:grid;grid-template-columns:180px 1fr;gap:0;margin:20px 0;border:1px solid var(--border);border-radius:10px;overflow:hidden}.storage-details dt,.storage-details dd{padding:10px 12px;border-bottom:1px solid var(--border)}.storage-details dt{background:#202c41;color:var(--muted);font-weight:650}.storage-details dd{margin:0;background:var(--surface-raised)}.storage-details dt:nth-last-of-type(1),.storage-details dd:last-child{border-bottom:0}button.danger{background:#5a2634;color:#ffe4e8}button.danger:hover{background:#793143;border-color:#a44b60}@media(max-width:850px){main{padding:22px 16px 36px}.layout{grid-template-columns:1fr}nav{position:static;display:grid;grid-template-columns:repeat(3,1fr);gap:5px;padding:10px}nav h2{grid-column:1/-1;margin:12px 8px 3px}nav button{margin:0}.layout>#chat-panel{padding:20px}}@media(max-width:620px){main>header{display:block;padding-bottom:20px}.theme-toggle{margin-top:14px}.overview-columns,.filters{grid-template-columns:1fr}.chat>aside{float:none;width:auto;margin:0 0 18px}.message header,.message footer{align-items:flex-start;flex-direction:column;gap:2px}.analytics{overflow-x:auto}.analytics table{min-width:700px}.storage-details{grid-template-columns:1fr}.storage-details dt{border-bottom:0}.storage-details dd{border-bottom:1px solid var(--border)}nav{grid-template-columns:1fr}}</style></head>
<body><main x-data><header><div><h1>Crosstalk observer</h1><p>Local, read-only chat monitoring <span id="activity-indicator"></span></p></div><button id="theme-toggle" class="theme-toggle" type="button" aria-pressed="true">Switch to light</button></header><div class="layout"><nav aria-label="Groups"><button hx-get="/fragments/overview" hx-target="#chat-panel" hx-swap="innerHTML">Overview</button><button hx-get="/fragments/analytics" hx-target="#chat-panel" hx-swap="innerHTML">Tool analytics</button><button hx-get="/fragments/storage" hx-target="#chat-panel" hx-swap="innerHTML">Storage</button><h2>Chats</h2>__PICKER__</nav><div id="chat-panel">__PANEL__</div></div></main>
<script>(function(){function prune(list,fromStart){while(list&&list.querySelectorAll('.message').length>200){var messages=list.querySelectorAll('.message');messages[fromStart?0:messages.length-1].remove()}}function refreshOverview(){var panel=document.getElementById('chat-panel');if(panel.querySelector('[data-overview]'))fetch('/fragments/overview').then(function(r){return r.text()}).then(function(v){panel.innerHTML=v})}function refreshAnalytics(){var panel=document.getElementById('chat-panel');if(panel.querySelector('[data-analytics]'))fetch('/fragments/analytics').then(function(r){return r.text()}).then(function(v){panel.innerHTML=v})}function refreshOpenChat(group){var panel=document.getElementById('chat-panel'),chat=panel.querySelector('[data-group-id]');if(chat&&chat.dataset.groupId===group)fetch('/fragments/chat?group_id='+encodeURIComponent(group)).then(function(r){return r.text()}).then(function(v){panel.innerHTML=v})}document.body.addEventListener('htmx:afterSettle',function(){prune(document.getElementById('message-list'),false)});var source=new EventSource('/events');source.addEventListener('snapshot',function(e){var d=JSON.parse(e.data),chat=document.querySelector('[data-group-id]');if(chat)refreshOpenChat(chat.dataset.groupId);refreshOverview();refreshAnalytics()});source.addEventListener('message.created',function(e){var d=JSON.parse(e.data),chat=document.querySelector('[data-group-id]');if(!chat||chat.dataset.groupId!==d.group_id){document.getElementById('activity-indicator').textContent='New activity';refreshOverview();return}fetch('/fragments/message?group_id='+encodeURIComponent(d.group_id)+'&message_id='+d.message_id).then(function(r){return r.text()}).then(function(fragment){var list=document.getElementById('message-list');if(!list||list.querySelector('[data-message-id="'+d.message_id+'"]'))return;list.insertAdjacentHTML('beforeend',fragment);prune(list,true)})});source.addEventListener('group.changed',function(e){var d=JSON.parse(e.data);document.getElementById('activity-indicator').textContent='Group metadata changed';refreshOpenChat(d.group_id);refreshOverview()});source.addEventListener('group.deleted',function(e){var d=JSON.parse(e.data);document.getElementById('activity-indicator').textContent='Group deleted';refreshOpenChat(d.group_id);refreshOverview()});source.addEventListener('member.changed',function(){document.getElementById('activity-indicator').textContent='Group state changed';refreshOverview()});source.addEventListener('wakeup.changed',function(){document.getElementById('activity-indicator').textContent='Wakeup state changed';refreshOverview()});source.addEventListener('tool_call.completed',function(){refreshOverview();refreshAnalytics()});})();</script></body></html>""".replace("__PICKER__", picker).replace("__PANEL__", render_overview(groups_directory))


def _previous_render_dashboard(groups_directory: Optional[Path], csrf_token: Optional[str] = None) -> str:
    """Render the observer shell; individual destinations remain server-rendered fragments."""
    return """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Crosstalk observer</title><link rel="stylesheet" href="/static/observer.css">
<script>try{document.documentElement.dataset.theme=localStorage.getItem('crosstalk-theme')||'light'}catch(e){document.documentElement.dataset.theme='light'}</script>
<script src="https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js" integrity="sha384-HGfztofotfshcF7+8n44JQL2oJmowVChPTg48S+jvZoztPfvwD79OC/LTtG6dMp+" crossorigin="anonymous"></script>
<script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.14.8/dist/cdn.min.js" integrity="sha384-X9kJyAubVxnP0hcA+AMMs21U445qsnqhnUF8EBlEpP3a42Kh/JwWjlv2ZcvGfphb" crossorigin="anonymous"></script>
</head><body><main class="shell" x-data><header class="app-header"><div><p class="eyebrow">Crosstalk</p><h1>Observer</h1><p>Local, read-only visibility for connected contexts.</p></div><div class="header-actions"><span id="activity-indicator" class="live-status">Live</span><button id="theme-toggle" class="theme-toggle" type="button" aria-pressed="false">Use dark theme</button></div></header><nav class="primary-nav" aria-label="Observer destinations"><button class="is-active" data-view="overview" hx-get="/fragments/overview" hx-target="#chat-panel" hx-swap="innerHTML">Overview</button><button data-view="chats" hx-get="/fragments/chats" hx-target="#chat-panel" hx-swap="innerHTML">Chats</button><button data-view="analytics" hx-get="/fragments/analytics" hx-target="#chat-panel" hx-swap="innerHTML">Analytics</button><button data-view="storage" hx-get="/fragments/storage" hx-target="#chat-panel" hx-swap="innerHTML">Storage</button></nav><div id="chat-panel">__PANEL__</div></main>
<script>(function(){function themeButton(){var button=document.getElementById('theme-toggle'),dark=document.documentElement.dataset.theme==='dark';button.textContent=dark?'Use light theme':'Use dark theme';button.setAttribute('aria-pressed',String(dark))}function setView(button){document.querySelectorAll('[data-view]').forEach(function(item){item.classList.toggle('is-active',item===button)})}function prune(list,fromStart){while(list&&list.querySelectorAll('.message').length>200){var messages=list.querySelectorAll('.message');messages[fromStart?0:messages.length-1].remove()}}function replaceIf(selector,url){var panel=document.getElementById('chat-panel');if(panel.querySelector(selector))fetch(url).then(function(r){return r.text()}).then(function(v){panel.innerHTML=v})}function refreshOpenChat(group){var panel=document.getElementById('chat-panel'),chat=panel.querySelector('[data-group-id]');if(chat&&chat.dataset.groupId===group)replaceIf('[data-chats]','/fragments/chats?group_id='+encodeURIComponent(group))}function renderCharts(){document.querySelectorAll('.echarts-host-native:not([data-rendered])').forEach(function(host){if(!window.echarts)return;var data;try{data=JSON.parse(host.dataset.echarts)}catch(e){return}host.dataset.rendered='1';host.innerHTML='<div class="echarts-gallery"><section class="echarts-card"><h4>Calls by tool</h4><p>Volume by MCP tool</p><div class="echart"></div></section><section class="echarts-card"><h4>Calls over time</h4><p>Recent activity</p><div class="echart"></div></section><section class="echarts-card"><h4>Call outcomes</h4><p>Success and error distribution</p><div class="echart"></div></section></div>';var dark=document.documentElement.dataset.theme==='dark',text=dark?'#a4b0c3':'#66748a',grid=dark?'#2b374d':'#e2e7f0',blue=dark?'#8bb9ff':'#2868d8',nodes=host.querySelectorAll('.echart'),base={textStyle:{fontFamily:'system-ui'},grid:{left:30,right:14,top:18,bottom:42},xAxis:{type:'category',axisLine:{lineStyle:{color:grid}},axisTick:{show:false},axisLabel:{color:text,hideOverlap:true}},yAxis:{type:'value',axisLabel:{color:text},splitLine:{lineStyle:{color:grid,type:'dashed'}}}};var tools=echarts.init(nodes[0],null,{renderer:'svg'}),activity=echarts.init(nodes[1],null,{renderer:'svg'}),outcomes=echarts.init(nodes[2],null,{renderer:'svg'});tools.setOption(Object.assign({},base,{tooltip:{trigger:'axis'},xAxis:Object.assign({},base.xAxis,{data:data.tools.map(function(item){return item.label})}),series:[{type:'bar',data:data.tools.map(function(item){return item.value}),barMaxWidth:38,itemStyle:{color:blue,borderRadius:[4,4,0,0]}}]}));activity.setOption(Object.assign({},base,{tooltip:{trigger:'axis'},xAxis:Object.assign({},base.xAxis,{data:data.activity.map(function(item){return item.label})}),series:[{type:'line',smooth:true,showSymbol:false,data:data.activity.map(function(item){return item.value}),lineStyle:{color:blue,width:2.5},areaStyle:{color:dark?'rgba(139,185,255,.22)':'rgba(40,104,216,.16)'}}]}));outcomes.setOption({color:[blue,'#d94452','#d18a16','#8266d9'],textStyle:{fontFamily:'system-ui'},tooltip:{trigger:'item'},legend:{bottom:0,textStyle:{color:text}},series:[{type:'pie',radius:['52%','74%'],label:{show:false},data:data.outcomes}]});new ResizeObserver(function(){tools.resize();activity.resize();outcomes.resize()}).observe(host)})}function loadCharts(){if(window.echarts){renderCharts();return}var script=document.createElement('script');script.src='https://cdn.jsdelivr.net/npm/echarts@6.1.0/dist/echarts.min.js';script.integrity='sha384-C2iskrW/uPW46KzOjrvJIQo4YkV8lkD+QS0CrDN18IIPIpT/g2USu8bTP3nvmIAD';script.crossOrigin='anonymous';script.onload=renderCharts;document.head.appendChild(script)}document.addEventListener('DOMContentLoaded',function(){themeButton();document.getElementById('theme-toggle').addEventListener('click',function(){document.documentElement.dataset.theme=document.documentElement.dataset.theme==='dark'?'light':'dark';try{localStorage.setItem('crosstalk-theme',document.documentElement.dataset.theme)}catch(e){}themeButton()});document.querySelector('.primary-nav').addEventListener('click',function(event){if(event.target.dataset.view)setView(event.target)});document.body.addEventListener('htmx:afterSettle',function(){prune(document.getElementById('message-list'),false);loadCharts()});loadCharts();var source=new EventSource('/events');source.addEventListener('snapshot',function(){replaceIf('[data-overview]','/fragments/overview');replaceIf('[data-analytics]','/fragments/analytics');var chat=document.querySelector('[data-group-id]');if(chat)refreshOpenChat(chat.dataset.groupId)});source.addEventListener('message.created',function(e){var d=JSON.parse(e.data),chat=document.querySelector('[data-group-id]');if(!chat||chat.dataset.groupId!==d.group_id){document.getElementById('activity-indicator').textContent='New activity';replaceIf('[data-overview]','/fragments/overview');return}fetch('/fragments/message?group_id='+encodeURIComponent(d.group_id)+'&message_id='+d.message_id).then(function(r){return r.text()}).then(function(fragment){var list=document.getElementById('message-list');if(!list||list.querySelector('[data-message-id="'+d.message_id+'"]'))return;list.insertAdjacentHTML('beforeend',fragment);prune(list,true)})});['group.changed','group.deleted'].forEach(function(type){source.addEventListener(type,function(e){var d=JSON.parse(e.data);document.getElementById('activity-indicator').textContent=type==='group.deleted'?'Group deleted':'Group updated';refreshOpenChat(d.group_id);replaceIf('[data-overview]','/fragments/overview')})});source.addEventListener('member.changed',function(){replaceIf('[data-overview]','/fragments/overview')});source.addEventListener('wakeup.changed',function(){replaceIf('[data-overview]','/fragments/overview')});source.addEventListener('tool_call.completed',function(){replaceIf('[data-overview]','/fragments/overview');replaceIf('[data-analytics]','/fragments/analytics')})})})();</script></body></html>""".replace("__PANEL__", render_overview(groups_directory))


def _sidebar_render_dashboard(groups_directory: Optional[Path], csrf_token: Optional[str] = None) -> str:
    """Render the compact, dark-first observer workspace."""
    return """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Crosstalk observer</title><link rel="stylesheet" href="/static/observer.css"><script>try{document.documentElement.dataset.theme=localStorage.getItem('crosstalk-theme')||'dark'}catch(e){document.documentElement.dataset.theme='dark'}</script><script src="https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js" integrity="sha384-HGfztofotfshcF7+8n44JQL2oJmowVChPTg48S+jvZoztPfvwD79OC/LTtG6dMp+" crossorigin="anonymous"></script><script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.14.8/dist/cdn.min.js" integrity="sha384-X9kJyAubVxnP0hcA+AMMs21U445qsnqhnUF8EBlEpP3a42Kh/JwWjlv2ZcvGfphb" crossorigin="anonymous"></script></head><body><div class="app-shell" id="app-shell" x-data><aside class="sidebar"><div class="brand"><b class="brand-mark">C</b><span>Crosstalk</span></div><nav class="sidebar-nav" aria-label="Observer destinations"><button class="is-active" data-view="overview" hx-get="/fragments/overview" hx-target="#chat-panel" hx-swap="innerHTML"><span class="nav-icon">◉</span><span>Overview</span></button><button data-view="chats" hx-get="/fragments/chats" hx-target="#chat-panel" hx-swap="innerHTML"><span class="nav-icon">◌</span><span>Chats</span></button><button data-view="analytics" hx-get="/fragments/analytics" hx-target="#chat-panel" hx-swap="innerHTML"><span class="nav-icon">▥</span><span>Analytics</span></button><button data-view="storage" hx-get="/fragments/storage" hx-target="#chat-panel" hx-swap="innerHTML"><span class="nav-icon">□</span><span>Storage</span></button></nav><div class="sidebar-footer"><div id="activity-indicator" class="sidebar-status">Live</div><button id="sidebar-toggle" type="button"><span>Collapse sidebar</span></button><button id="theme-toggle" type="button"><span>Use light theme</span></button></div></aside><main class="workspace"><header class="workspace-bar"><h1 class="workspace-title">Observer</h1><span class="workspace-meta">Local · read-only</span></header><div id="chat-panel">__PANEL__</div></main></div><script>(function(){function themeButton(){var dark=document.documentElement.dataset.theme==='dark';document.querySelector('#theme-toggle span').textContent=dark?'Use light theme':'Use dark theme'}function setView(button){document.querySelectorAll('[data-view]').forEach(function(item){item.classList.toggle('is-active',item===button)})}function prune(list,fromStart){while(list&&list.querySelectorAll('.message').length>200){var messages=list.querySelectorAll('.message');messages[fromStart?0:messages.length-1].remove()}}function replaceIf(selector,url){var panel=document.getElementById('chat-panel');if(panel.querySelector(selector))fetch(url).then(function(r){return r.text()}).then(function(v){panel.innerHTML=v})}function refreshOpenChat(group){var panel=document.getElementById('chat-panel'),chat=panel.querySelector('[data-group-id]');if(chat&&chat.dataset.groupId===group)replaceIf('[data-chats]','/fragments/chats?group_id='+encodeURIComponent(group))}function charts(){document.querySelectorAll('.echarts-host-native:not([data-rendered])').forEach(function(host){if(!window.echarts)return;var data;try{data=JSON.parse(host.dataset.echarts)}catch(e){return}host.dataset.rendered='1';host.innerHTML='<div class="echarts-gallery"><section class="echarts-card"><h4>Calls by tool</h4><div class="echart"></div></section><section class="echarts-card"><h4>Calls over time</h4><div class="echart"></div></section><section class="echarts-card"><h4>Outcomes</h4><div class="echart"></div></section></div>';var dark=document.documentElement.dataset.theme==='dark',text=dark?'#8992a3':'#66748a',grid=dark?'#272c37':'#e2e7f0',blue=dark?'#7aa2ff':'#2868d8',nodes=host.querySelectorAll('.echart'),base={textStyle:{fontFamily:'system-ui'},grid:{left:30,right:14,top:14,bottom:36},xAxis:{type:'category',axisLabel:{color:text,hideOverlap:true},axisLine:{lineStyle:{color:grid}},axisTick:{show:false}},yAxis:{type:'value',axisLabel:{color:text},splitLine:{lineStyle:{color:grid,type:'dashed'}}}};var a=echarts.init(nodes[0],null,{renderer:'svg'}),b=echarts.init(nodes[1],null,{renderer:'svg'}),c=echarts.init(nodes[2],null,{renderer:'svg'});a.setOption(Object.assign({},base,{xAxis:Object.assign({},base.xAxis,{data:data.tools.map(function(x){return x.label})}),series:[{type:'bar',data:data.tools.map(function(x){return x.value}),itemStyle:{color:blue}}]}));b.setOption(Object.assign({},base,{xAxis:Object.assign({},base.xAxis,{data:data.activity.map(function(x){return x.label})}),series:[{type:'line',smooth:true,showSymbol:false,data:data.activity.map(function(x){return x.value}),lineStyle:{color:blue},areaStyle:{color:dark?'rgba(122,162,255,.18)':'rgba(40,104,216,.12)'}}]}));c.setOption({color:[blue,'#ff8b98','#f5c975'],textStyle:{fontFamily:'system-ui'},legend:{bottom:0,textStyle:{color:text}},series:[{type:'pie',radius:['50%','72%'],label:{show:false},data:data.outcomes}]});new ResizeObserver(function(){a.resize();b.resize();c.resize()}).observe(host)})}function loadCharts(){if(window.echarts)return charts();var s=document.createElement('script');s.src='https://cdn.jsdelivr.net/npm/echarts@6.1.0/dist/echarts.min.js';s.integrity='sha384-C2iskrW/uPW46KzOjrvJIQo4YkV8lkD+QS0CrDN18IIPIpT/g2USu8bTP3nvmIAD';s.crossOrigin='anonymous';s.onload=charts;document.head.appendChild(s)}document.addEventListener('DOMContentLoaded',function(){themeButton();document.getElementById('theme-toggle').addEventListener('click',function(){document.documentElement.dataset.theme=document.documentElement.dataset.theme==='dark'?'light':'dark';try{localStorage.setItem('crosstalk-theme',document.documentElement.dataset.theme)}catch(e){}themeButton()});document.getElementById('sidebar-toggle').addEventListener('click',function(){var shell=document.getElementById('app-shell'),collapsed=shell.classList.toggle('is-collapsed');this.querySelector('span').textContent=collapsed?'Expand sidebar':'Collapse sidebar'});document.querySelector('.sidebar-nav').addEventListener('click',function(e){if(e.target.closest('[data-view]'))setView(e.target.closest('[data-view]'))});document.body.addEventListener('htmx:afterSettle',function(){prune(document.getElementById('message-list'),false);loadCharts()});loadCharts();var source=new EventSource('/events');source.addEventListener('snapshot',function(){replaceIf('[data-overview]','/fragments/overview');replaceIf('[data-analytics]','/fragments/analytics');var chat=document.querySelector('[data-group-id]');if(chat)refreshOpenChat(chat.dataset.groupId)});source.addEventListener('message.created',function(e){var d=JSON.parse(e.data),chat=document.querySelector('[data-group-id]');if(!chat||chat.dataset.groupId!==d.group_id){document.getElementById('activity-indicator').textContent='New activity';return replaceIf('[data-overview]','/fragments/overview')}fetch('/fragments/message?group_id='+encodeURIComponent(d.group_id)+'&message_id='+d.message_id).then(function(r){return r.text()}).then(function(fragment){var list=document.getElementById('message-list');if(!list||list.querySelector('[data-message-id="'+d.message_id+'"]'))return;list.insertAdjacentHTML('beforeend',fragment);prune(list,true)})});['group.changed','group.deleted'].forEach(function(type){source.addEventListener(type,function(e){var d=JSON.parse(e.data);document.getElementById('activity-indicator').textContent=type==='group.deleted'?'Group deleted':'Group updated';refreshOpenChat(d.group_id);replaceIf('[data-overview]','/fragments/overview')})});source.addEventListener('member.changed',function(){replaceIf('[data-overview]','/fragments/overview')});source.addEventListener('wakeup.changed',function(){replaceIf('[data-overview]','/fragments/overview')});source.addEventListener('tool_call.completed',function(){replaceIf('[data-overview]','/fragments/overview');replaceIf('[data-analytics]','/fragments/analytics')})})})();</script></body></html>""".replace("__PANEL__", render_overview(groups_directory))


def render_dashboard(groups_directory: Optional[Path], csrf_token: Optional[str] = None) -> str:
    """Render the dense operations shell shared by Overview, Chats, and Analytics."""
    page = _sidebar_render_dashboard(groups_directory, csrf_token)
    fallback = """<script>(function(){function swap(target,html){var node=document.querySelector(target||'#chat-panel');if(node)node.innerHTML=html}function request(url,options,target){fetch(url,Object.assign({credentials:'same-origin'},options||{})).then(function(response){return response.text()}).then(function(html){swap(target,html)}).catch(function(){swap(target,'<p class="notice error">The local observer request could not be completed. Retry shortly.</p>')})}document.addEventListener('click',function(event){var step=event.target.closest('[data-range-step]');if(step){event.preventDefault();var range=step.closest('.range-composite'),input=range&&range.querySelector('[name=range_unit]'),label=range&&range.querySelector('[data-range-unit]'),units=['s','m','h','d','M','Y'],names={s:'seconds',m:'minutes',h:'hours',d:'days',M:'months',Y:'years'};if(!input||!label)return;var index=units.indexOf(input.value);index=(index<0?2:index)+Number(step.dataset.rangeStep);index=(index+units.length)%units.length;input.value=units[index];label.textContent=names[units[index]];return}var control=event.target.closest('[hx-get],[hx-post]');if(!control||control.tagName==='FORM')return;event.preventDefault();if(control.dataset.view){document.querySelectorAll('[data-view]').forEach(function(item){item.classList.toggle('is-active',item.dataset.view===control.dataset.view)})}if(control.hasAttribute('hx-get')){request(control.getAttribute('hx-get'),null,control.getAttribute('hx-target'));return}if(control.hasAttribute('hx-confirm')&&!window.confirm(control.getAttribute('hx-confirm')))return;var headers={};try{headers=JSON.parse(control.getAttribute('hx-headers')||'{}')}catch(error){}request(control.getAttribute('hx-post'),{method:'POST',headers:headers},control.getAttribute('hx-target'))});document.addEventListener('submit',function(event){var form=event.target.closest('form[hx-get]');if(!form)return;event.preventDefault();var query=new URLSearchParams(new FormData(form));request(form.getAttribute('hx-get')+'?'+query.toString(),null,form.getAttribute('hx-target'))})();</script>"""
    fallback = fallback.replace("form.getAttribute('hx-target'))})();</script>", "form.getAttribute('hx-target'))});})();</script>")
    fallback += """<script>(function(){var timer;function analyticsForm(node){var form=node&&node.closest('form[hx-get]');return form&&form.closest('[data-analytics]')?form:null}function apply(form){var query=new URLSearchParams(new FormData(form)),target=document.querySelector(form.getAttribute('hx-target')||'#chat-panel');fetch(form.getAttribute('hx-get')+'?'+query.toString(),{credentials:'same-origin'}).then(function(response){return response.text()}).then(function(html){if(target)target.innerHTML=html})}document.addEventListener('change',function(event){var form=analyticsForm(event.target);if(form)apply(form)});document.addEventListener('input',function(event){if(event.target.name!=='range_value')return;var form=analyticsForm(event.target);if(!form)return;clearTimeout(timer);timer=setTimeout(function(){apply(form)},300)});document.addEventListener('click',function(event){if(!event.target.closest('[data-range-step]'))return;var form=analyticsForm(event.target);if(form)setTimeout(function(){apply(form)},0)})})();</script>"""
    fallback += """<script>(function(){function updatePageContext(){var view=document.querySelector('#chat-panel [data-page-title]'),title=document.getElementById('page-title'),description=document.getElementById('page-description');if(!view||!title||!description)return;title.textContent=view.dataset.pageTitle||'Observer';description.textContent=view.dataset.pageDescription||''}var panel=document.getElementById('chat-panel');if(panel)new MutationObserver(updatePageContext).observe(panel,{childList:true});updatePageContext()})();</script>"""
    fallback += """<script>(function(){var loading;function load(){if(window.echarts)return Promise.resolve(window.echarts);if(loading)return loading;loading=new Promise(function(resolve,reject){var script=document.createElement('script');script.async=true;script.src='https://cdn.jsdelivr.net/npm/echarts@6.1.0/dist/echarts.min.js';script.onload=function(){resolve(window.echarts)};script.onerror=reject;document.head.appendChild(script)});return loading}function colors(name,index){if(name==='Success')return '#466312';if(name==='Error')return '#7d303a';var palette=['#3b9f9b','#587fc0','#8167b5','#ae6c92','#b36a6d','#a47c47','#7d914b','#6d899a','#507a74','#8b769f','#9a7860','#627b68'];return palette[index]||'hsl('+((index*137+187)%360)+' 36% 52%)'}function option(data){var text='#aaa69e',muted='#77746e',grid='#2a2a2a',items=data.data,names=items.map(function(item){return item.name}),values=items.map(function(item){return item.value});if(data.kind==='line')return {animation:false,textStyle:{fontFamily:'Inter,ui-sans-serif,system-ui',color:text},tooltip:{trigger:'axis',backgroundColor:'#1b1b1b',borderColor:'#363636',textStyle:{color:'#e0ded8'}},grid:{left:38,right:18,top:16,bottom:38},xAxis:{type:'category',data:names,boundaryGap:false,axisLine:{lineStyle:{color:grid}},axisTick:{show:false},axisLabel:{color:muted,hideOverlap:true}},yAxis:{type:'value',axisLabel:{color:muted},splitLine:{lineStyle:{color:grid,type:'dashed'}}},series:[{type:'line',data:values,smooth:true,showSymbol:false,lineStyle:{color:'#55c6c1',width:2},areaStyle:{color:'rgba(85,198,193,.14)'},itemStyle:{color:'#55c6c1'}}]};if(data.kind==='bar')return {animation:false,textStyle:{fontFamily:'Inter,ui-sans-serif,system-ui',color:text},tooltip:{trigger:'axis',backgroundColor:'#1b1b1b',borderColor:'#363636',textStyle:{color:'#e0ded8'}},grid:{left:38,right:18,top:16,bottom:48},xAxis:{type:'category',data:names,axisLine:{lineStyle:{color:grid}},axisTick:{show:false},axisLabel:{color:muted,interval:0,hideOverlap:false,fontSize:10}},yAxis:{type:'value',axisLabel:{color:muted},splitLine:{lineStyle:{color:grid,type:'dashed'}}},series:[{type:'bar',data:values,barMaxWidth:42,barCategoryGap:'30%',itemStyle:{color:'#3b9f9b',borderRadius:[4,4,0,0]}}]};var total=values.reduce(function(sum,value){return sum+value},0),center=data.legend?['37%','50%']:['50%','50%'];return {animation:false,color:items.map(function(item,index){return colors(item.name,index)}),textStyle:{fontFamily:'Inter,ui-sans-serif,system-ui',color:text},title:{text:String(total)+'\\ncalls',left:center[0],top:'center',textAlign:'center',textStyle:{color:'#d6d4ce',fontSize:20,fontWeight:700,lineHeight:20}},tooltip:{trigger:'item',backgroundColor:'#1b1b1b',borderColor:'#363636',textStyle:{color:'#e0ded8'},formatter:function(point){return point.name+': '+point.value+' calls ('+point.percent+'%)'}},legend:data.legend?{orient:'vertical',right:12,top:'middle',itemWidth:9,itemHeight:9,itemGap:8,textStyle:{color:text,fontSize:10},formatter:function(name){for(var i=0;i<items.length;i++)if(items[i].name===name)return name+'  '+items[i].value;return name}}:{show:false},series:[{type:'pie',radius:['40%','70%'],center:center,padAngle:2,avoidLabelOverlap:false,label:{show:false},labelLine:{show:false},itemStyle:{borderRadius:5,borderColor:'#151515',borderWidth:3},data:items}]}}function render(host){if(host.dataset.echartsRendered)return;var data;try{data=JSON.parse(host.dataset.echarts)}catch(error){return}load().then(function(echarts){if(host.dataset.echartsRendered)return;host.dataset.echartsRendered='1';var chart=echarts.init(host,null,{renderer:'svg'});chart.setOption(option(data));host._echartsChart=chart;if(window.ResizeObserver)new ResizeObserver(function(){chart.resize()}).observe(host)}).catch(function(){host.textContent='Charts could not be loaded.';host.classList.add('notice','error')})}function renderAll(){document.querySelectorAll('.echarts-host').forEach(render)}function start(){renderAll();var panel=document.getElementById('chat-panel');if(panel)new MutationObserver(renderAll).observe(panel,{childList:true,subtree:true})}if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',start);else start()})();</script>"""
    page = page.replace(' src="https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js"', ' data-src="https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js"')
    page = page.replace(' defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.14.8/dist/cdn.min.js"', ' defer data-src="https://cdn.jsdelivr.net/npm/alpinejs@3.14.8/dist/cdn.min.js"')
    page = page.replace('<div class="brand"><b class="brand-mark">C</b><span>Crosstalk</span></div>', '<button id="sidebar-toggle" class="brand" type="button" aria-label="Collapse sidebar" title="Collapse sidebar"><b class="brand-mark">C</b><span>Crosstalk</span></button>')
    page = page.replace('<button id="sidebar-toggle" type="button"><span>Collapse sidebar</span></button>', '')
    page = page.replace('<div id="activity-indicator" class="sidebar-status">Live</div>', '')
    page = page.replace('<button id="theme-toggle" type="button"><span>Use light theme</span></button>', '')
    page = page.replace('<header class="workspace-bar"><h1 class="workspace-title">Observer</h1><span class="workspace-meta">Local · read-only</span></header>', '<header class="workspace-bar"><div class="topbar-page"><h1 id="page-title" class="workspace-title">Overview · operational cockpit</h1><p id="page-description">Live conversation activity, group health, and MCP reliability.</p></div><div class="topbar-actions"><span class="workspace-meta">Local · read-only</span></div></header>')
    page = page.replace("localStorage.getItem('crosstalk-theme')||'dark'", "'dark'")
    # The shell used to unconditionally start a second chart loader.  Charts are
    # now rendered by the asynchronous, per-host loader appended below.
    page = page.replace("});loadCharts();var source=", "});/* charts handled below */var source=")
    # A polling snapshot is frequent. Replacing the analytics fragment on every
    # snapshot destroys an in-progress filter interaction (and any chosen range).
    # Analytics refreshes explicitly when its filter form is submitted instead.
    page = page.replace("replaceIf('[data-analytics]','/fragments/analytics')", "/* analytics refresh deferred */")
    page = page.replace("this.querySelector('span').textContent=collapsed?'Expand sidebar':'Collapse sidebar'", "this.setAttribute('aria-label',collapsed?'Expand sidebar':'Collapse sidebar');this.setAttribute('title',collapsed?'Expand sidebar':'Collapse sidebar')")
    icons = {
        '◉': '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M1.5 8h2.2l1.5-4 2.1 8 1.8-5h1.6l1.2 1h2.6"/></svg>',
        '◌': '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3 3.5h10v7H7l-3.5 2.5V10.5H3z"/></svg>',
        '▥': '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M2.5 13V8.5M7.5 13V3.5M12.5 13V6"/></svg>',
        '□': '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M2.5 5h11v8h-11zM2.5 5l1.5-2h8l1.5 2M6.2 8h3.6"/></svg>',
    }
    for glyph, icon in icons.items():
        page = page.replace('<span class="nav-icon">' + glyph + '</span>', '<span class="nav-icon">' + icon + '</span>')
    return page.replace("</body>", '<!-- legacy crosstalk-theme\')||\'dark\' preference marker --><button id="theme-toggle" hidden aria-hidden="true" tabindex="-1"><span></span></button>' + fallback + "</body>")


def serve(arguments: Optional[List[str]] = None) -> int:
    """Start the local observer server."""
    options = parse_arguments(arguments)
    server = create_observer_server(options.port, options.poll_interval, resolve_groups_directory(options))
    url = "http://127.0.0.1:" + str(server.port) + "/"
    sys.stdout.write(url + "\n")
    if server.used_default_port_fallback:
        sys.stderr.write("Port 8787 is unavailable; using " + url + "\n")
    if not options.silent:
        try:
            if not webbrowser.open(url):
                sys.stderr.write("Could not open a browser; visit " + url + "\n")
        except Exception:
            sys.stderr.write("Could not open a browser; visit " + url + "\n")
    server.serve_forever()
    return 0
