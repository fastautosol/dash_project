# 2026.05.16  12.00
import dash
from dash import dcc, html, callback, Output, Input
import dash_bootstrap_components as dbc
from datetime import datetime
import pandas as pd
import sys
import platform
import os
import shutil
import psutil
from importlib.metadata import version, PackageNotFoundError
from concurrent.futures import ThreadPoolExecutor
import docker

dash.register_page( __name__, name="VPS SysInfo", icon="fa-solid fa-server", order=9)

CARD_STYLE = {
    "background": "rgba(255, 255, 255, 0.03)", "backdropFilter": "blur(10px)",
    "borderRadius": "15px", "border": "1px solid rgba(255, 255, 255, 0.1)", "padding": "20px"}

TABLE_STYLE = { "backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white"}
PACKAGES = ["fastapi", "fastmcp", "pandas", "dash", "docker", "ccxt", "scikit-learn", "SQLAlchemy", "dlt", "psycopg"]
ALLOWED_PREFIXES = ("crypto", "n8n", "python-dash", "postgresql") 

def fetch_one_docker(c) -> dict:
    s = c.stats(stream=False)
    cpu_delta    = s["cpu_stats"]["cpu_usage"]["total_usage"] - s["precpu_stats"]["cpu_usage"]["total_usage"]
    system_delta = s["cpu_stats"]["system_cpu_usage"]         - s["precpu_stats"]["system_cpu_usage"]
    cpus         = s["cpu_stats"].get("online_cpus", 1)
    cpu_pct      = round((cpu_delta / system_delta) * cpus * 100, 2) if system_delta > 0 else 0.0
    mem_usage = s["memory_stats"].get("usage", 0)  / 1024 / 1024  
    mem_limit = s["memory_stats"].get("limit", 0) / 1024 / 1024 / 1024 
    mem_pct   = round(s["memory_stats"].get("usage", 1) / s["memory_stats"].get("limit", 1) * 100, 1)
    
    return {"ID":c.short_id, "Name": c.name.split("-dwlrq6pw")[0], # c.name[:30],
            "CPU %":f"{cpu_pct}%", "MEM Usage":f"{mem_usage:.1f} MiB", "MEM Limit": f"{mem_limit:.1f} GB", "MEM %":f"{mem_pct}%", "Status": c.status}

def get_docker_stats() -> pd.DataFrame:
    try:
        client = docker.from_env()  
        containers = [c for c in client.containers.list() if c.name.startswith(ALLOWED_PREFIXES)] 
        with ThreadPoolExecutor(max_workers=len(containers) or 1) as ex:
            rows = list(ex.map(fetch_one_docker, containers))
        return pd.DataFrame(rows)
    except Exception as e:
        print(e)
        return pd.DataFrame()

def get_runtime_info():
    info_header = [("Python Version", sys.version.split()[0]), ("OS Platform", platform.platform()[:11])]
    pkg_rows = []
    for p in PACKAGES:
        try:
            pkg_rows.append((p, version(p)))
        except PackageNotFoundError:
            pkg_rows.append((p, "not installed"))

    return info_header, pkg_rows

def get_host_metrics():
    cpu = psutil.cpu_percent(interval=0.5)
    cpu_freq = psutil.cpu_freq()
    mem = psutil.virtual_memory()
    disk = shutil.disk_usage("/")
    return [ ("CPU Usage", f"{cpu} %"), ("CPU Cores", psutil.cpu_count(logical=True)), ("CPU Frequency", f"{cpu_freq.current:.0f} MHz"),
        ("Memory Usage", f"{mem.percent} %"), ("Memory Total", f"{mem.total / (1024**3):.1f} GB"),
        ("Disk Usage", f"{disk.used / disk.total * 100:.1f} %"), ("Disk Total", f"{disk.total / (1024**3):.1f} GB")]


layout = dbc.Container([

    html.Div([
        html.H1("System Control Center", className="text-light fw-bold"),
        html.P(["Monitoring VPS Dockers/System/Packages → ", html.Small(id="vps-updated", className="text-muted")], className="text-muted"),
    ], className="mb-5"),

    dcc.Interval(id="vps-interval", interval=60*1000, n_intervals=0), 

    dbc.Row([
        dbc.Col(dbc.Card(
            dbc.CardBody([
                html.H5("VPS Docker Stats", className="text-info"),
                html.Div(id="docker-table"),
            ]), style=CARD_STYLE), width=12)
    ], className="g-4 mb-4"),

    dbc.Row([
    dbc.Col(
        dbc.Card(
            dbc.CardBody([
                dbc.Row([
                dbc.Col(html.H4("Runtime Environment", className="text-info fw-semibold"), width="auto"),
                 dbc.Col(dbc.Button("Refresh", id="refresh-btn", color="info", outline=True, size="sm", className="mt-3"), width="auto")
                ],   align="center", justify="between", className="mb-3"),
                html.Div(id="env-table")
            ]), style=CARD_STYLE), width=6),

    dbc.Col(
        dbc.Card(
            dbc.CardBody([
                html.H4("Package Versions", className="text-info fw-semibold"),
                html.Div(id="packages-table"),
            ]), style=CARD_STYLE), width=6),

    ], className="g-4 mb-4")

], fluid=True)


@callback(
    Output("vps-updated", "children"),
    Output("docker-table", "children"),
    Output("env-table", "children"),
    Output("packages-table", "children"),
    Input("vps-interval", "n_intervals"),
    Input("refresh-btn", "n_clicks"),
)
def render_tables(_, n_clicks):
    
    docker_stats_df = get_docker_stats()
    runtime_info, pkg_rows = get_runtime_info()
    host_metrics = get_host_metrics()
    vps_upd = f"Updated: {datetime.now().strftime('%H:%M:%S')}"
    
    if docker_stats_df.empty:
        docker_tbl = html.P("No containers found or Docker not accessible.", className="text-warning")
    else:
        docker_tbl = dbc.Table.from_dataframe(docker_stats_df, striped=False, hover=True, responsive=True, borderless=True, 
            className="text-light table-sm m-0", style=TABLE_STYLE)
 
    env_df = pd.DataFrame(runtime_info + host_metrics, columns=["Metric", "Value"])
    env_tbl = dbc.Table.from_dataframe(env_df, striped=False, hover=True, responsive=True, borderless=True, 
        className="text-light table-sm m-0", style=TABLE_STYLE)

    pkg_df = pd.DataFrame(pkg_rows, columns=["Package", "Version"])
    pkg_tbl = dbc.Table.from_dataframe(pkg_df, striped=False, hover=True, responsive=True, borderless=True, 
        className="text-light table-sm m-0", style=TABLE_STYLE)

    return vps_upd, docker_tbl, env_tbl, pkg_tbl
