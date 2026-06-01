# 2026.05.31  12.00
# UNIFIED DATABRICKS DIAGNOSTICS + LAZY TABLE LOADING + SDK WARMUP

import os
import logging
import threading
import json
from concurrent.futures import ThreadPoolExecutor
try:
    from concurrent.futures import TimeoutError as FuturesTimeoutError
except ImportError:
    FuturesTimeoutError = TimeoutError

import dash
from dash import dcc, html, Input, Output, callback, ALL, ctx
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import pandas as pd

# =================================================
# LOGGING & ENV
# =================================================
logging.basicConfig(level=logging.INFO)
DBX_TOKEN = os.getenv("DB_API_KEY")
DBX_HOST = 'https://dbc-9c577faf-b445.cloud.databricks.com/'
DBX_HTTP_PATH = '/sql/1.0/warehouses/cbfc343eb927c998'
WAREHOUSE_ID = 'cbfc343eb927c998' 
TARGET_WS_PATH = "/Users/csakiss@outlook.hu"
TARGET_CATALOG = "test_cat"
TARGET_JOB_ID = '718482410766048'

# Pre-clean host for SQL connector
HOST_CLEAN = (DBX_HOST or "").replace("https://", "").replace("http://", "").strip("/ ")

# =================================================
# SDK IMPORTS
# =================================================
try:
    from databricks.sdk import WorkspaceClient
    from databricks import sql as dbx_sql
    SDK_AVAILABLE = True
    SDK_ERROR = None
except Exception as _sdk_err:
    SDK_AVAILABLE = False
    SDK_ERROR = str(_sdk_err)

# =================================================
# DASH PAGE
# =================================================
dash.register_page(
    __name__,
    path="/databricks",
    name="Databricks New",
    order=5,
)

# =================================================
# STYLES
# =================================================
CARD_STYLE = {
    "background": "rgba(255,255,255,0.03)",
    "backdropFilter": "blur(10px)",
    "borderRadius": "15px",
    "border": "1px solid rgba(255,255,255,0.08)",
    "padding": "20px",
    "marginBottom": "20px",
}

SCROLL_STYLE = {
    "maxHeight": "400px",
    "overflowY": "auto",
    "borderRadius": "8px",
}

# =================================================
# LAYOUT
# =================================================
layout = dbc.Container(
    [
        html.Div([
            html.H2("Databricks Diagnostics New", className="text-info fw-bold"),
            html.P("Unified SDK metadata + Lazy Table Loading", className="text-muted"),
        ], className="mb-4"),

        dbc.Button(
            [html.I(className="fas fa-play me-2"), "Load All Info & Start Warehouse"],
            id="load-btn",
            color="info",
            className="mb-4 fw-bold",
        ),

        # Rows 1-4: Info cards — outer Loading only covers these
        dbc.Row([
            dbc.Col(
                html.Div( dcc.Loading( type="circle", color="#17a2b8", children=html.Div(
                    id="user-card",
                    children=html.P(
                        "Click 'Load All Info & Start Warehouse'",
                        className="text-muted"
                    )
                )), style=CARD_STYLE), md=6),
            dbc.Col(
                html.Div(dcc.Loading( type="circle", color="#17a2b8", children=html.Div(
                    id="jobs-card",
                    children=html.P(
                        "Click 'Load All Info & Start Warehouse'",
                        className="text-muted"
                    )
                )), style=CARD_STYLE), md=6),
        ]),
        dbc.Row([
            dbc.Col(
                html.Div(dcc.Loading(type="circle", color="#17a2b8", children=html.Div(
                    id="job-history-card",
                    children=html.P(
                        "Click 'Load All Info & Start Warehouse'",
                        className="text-muted"
                    )
                )), style=CARD_STYLE), md=6),
            dbc.Col(
                 html.Div(dcc.Loading(type="circle", color="#17a2b8", children=html.Div(
                    id="clusters-card",
                    children=html.P(
                        "Click 'Load All Info & Start Warehouse'",
                        className="text-muted"
                    )
                )), style=CARD_STYLE), md=6),
        ]),
        dbc.Row([
            dbc.Col(
                html.Div(dcc.Loading(type="circle", color="#17a2b8", children=html.Div(
                    id="warehouses-card",
                    children=html.P(
                        "Click 'Load All Info & Start Warehouse'",
                        className="text-muted"
                    )
                )), style=CARD_STYLE), md=6),
            dbc.Col(
                html.Div(dcc.Loading(type="circle", color="#17a2b8", children=html.Div(
                    id="workspace-card",
                    children=html.P(
                        "Click 'Load All Info & Start Warehouse'",
                        className="text-muted"
                    )
                )), style=CARD_STYLE), md=6),
        ]),
        dbc.Row([
            dbc.Col(
                html.Div(dcc.Loading(type="circle", color="#17a2b8", children=html.Div(
                    id="unity-card",
                    children=html.P(
                        "Click 'Load All Info & Start Warehouse'",
                        className="text-muted"
                    )
                )), style=CARD_STYLE), md=12),
        ]),
                
        
        # Row 5: Table data — outside info Loading so clicking Load Table, never blanks the info cards above
        dbc.Row([
            dbc.Col(
            html.Div(
            [
                html.H4("Selected Table Data",  className="text-info mb-3"),
                dcc.Loading(id="loading-table-data", type="circle", color="#17a2b8", 
                    children=html.Div( id="selected-table-output",
                        children=html.P("Click a 'Load Table' button above to display table contents.", className="text-muted")
                    )
                ),
            ],style=CARD_STYLE), md=12),
        ]),
        
    ],
    fluid=True,
)

# =================================================
# HELPERS
# =================================================
def safe_call(func, timeout=30, fallback=None):
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        future = executor.submit(func)
        return future.result(timeout=timeout)
    except FuturesTimeoutError:
        logging.warning("safe_call: timeout after %s s", timeout)
        return fallback
    except Exception as exc:
        logging.error("safe_call error: %s", exc)
        return fallback
    finally:
        executor.shutdown(wait=False)

def warmup_warehouse(w_client):
    """Uses SDK to start the warehouse in the background."""
    try:
        logging.info(f"Starting background warehouse warmup via SDK for ID: {WAREHOUSE_ID}...")
        # .result() blocks the background thread until the warehouse is RUNNING
        w_client.warehouses.start(WAREHOUSE_ID).result()
        logging.info("Warehouse start command completed successfully!")
    except Exception as e:
        # If it's already running, the API might return an INVALID_STATE error.
        # We catch it so the background thread doesn't crash silently.
        logging.warning(f"Warehouse start API response (might already be running): {e}")

def build_table(df):
    if df is None or df.empty:
        return html.Div("No data available", className="text-warning")
    return dbc.Table.from_dataframe(
        df, striped=False, bordered=False, hover=True, responsive=True,
        className="text-light m-0",
        style={
            "backgroundColor": "transparent",
            "--bs-table-bg": "transparent",
            "--bs-table-accent-bg": "transparent",
            "color": "white",
        },
    )

def build_unity_tables_card(tables, catalog):
    """Builds the UC tables list manually to inject 'Load Table' buttons."""
    if not tables:
        return html.P("No tables found", className="text-muted")
    
    header = html.Thead(html.Tr([
        html.Th("Table"), html.Th("Schema"), html.Th("Type"), html.Th("Action", style={"width": "120px"})
    ]))
    
    rows = []
    for t in tables:
        t_name = getattr(t, 'name', '?')
        s_name = getattr(t, 'schema_name', '?')
        t_type = str(getattr(t, 'table_type', '?')).replace("TableType.", "")
        
        if s_name.lower() in ('information_schema', 'default'):
            continue
            
        # Pattern matching ID for dynamic callback
        btn_id = {"type": "load-table-btn", "index": f"{catalog}.{s_name}.{t_name}"}
        btn = dbc.Button("Load Table", id=btn_id, size="sm", color="info", className="py-0 px-2")
        
        rows.append(html.Tr([
            html.Td(t_name),
            html.Td(s_name),
            html.Td(t_type),
            html.Td(btn)
        ]))
        
    if not rows:
        return html.P("No user tables found (only system schemas).", className="text-muted")

    return dbc.Table(
        [header, html.Tbody(rows)],
        striped=False, bordered=False, hover=True, responsive=True,
        className="text-light m-0",
        style={"backgroundColor": "transparent", "--bs-table-bg": "transparent", "color": "white"}
    )

def error_card(title, message):
    return [
        html.H4(title, className="text-warning mb-3"),
        html.Div(str(message), className="text-muted"),
    ]

# =================================================
# CALLBACK 1: METADATA & WARMUP
# =================================================
@callback(
    Output("user-card", "children"),
    Output("jobs-card", "children"),
    Output("job-history-card", "children"),
    Output("clusters-card", "children"),
    Output("warehouses-card", "children"),
    Output("workspace-card", "children"),
    Output("unity-card", "children"),
    Input("load-btn", "n_clicks"),
    prevent_initial_call=True,
)
def load_databricks_info(n_clicks):
    if not n_clicks:
        raise PreventUpdate

    if not SDK_AVAILABLE:
        err = error_card("SDK Import Error", SDK_ERROR)
        return err, err, err, err, err, err, err

    try:
        w = WorkspaceClient(host=f"https://{HOST_CLEAN}", token=DBX_TOKEN)
    except Exception as exc:
        err = error_card("Connection Error", exc)
        return err, err, err, err, err, err, err

    # 1. Trigger Warehouse Warmup IMMEDIATELY in background thread
    threading.Thread(target=warmup_warehouse, args=(w,), daemon=True).start()

    # --- Metadata Fetching ---
    current_user = safe_call(lambda: w.current_user.me(), timeout=30)
    user_card = html.Div([
        html.H4("Current User", className="text-info mb-3"),
        html.P(f"User Name: {current_user.user_name}", className="text-light"),
        html.P(f"Display Name: {current_user.display_name}", className="text-light"),
        html.P(f"Active: {current_user.active}", className="text-light"),
    ]) if current_user else error_card("Current User", "Timeout or unavailable")

    jobs = safe_call(lambda: list(w.jobs.list()), timeout=30, fallback=[])
    jobs_card = html.Div([
        html.H4("Jobs", className="text-info mb-3"),
        build_table(pd.DataFrame([{"Job ID": j.job_id, "Job Name": j.settings.name if j.settings else "Unknown"} for j in jobs[:10]]))
    ]) if jobs else error_card("Jobs", "No jobs found or timeout")

    job_runs = safe_call(lambda: list(w.jobs.list_runs(job_id=int(TARGET_JOB_ID), limit=1)), timeout=30, fallback=[])
    if job_runs:
        runs_df = pd.DataFrame([{
            "Run ID": r.run_id, 
            "Result": str(r.state.result_state) if (r.state and r.state.result_state) else "N/A",
            "Life Cycle": str(r.state.life_cycle_state) if (r.state and r.state.life_cycle_state) else "Unknown",            
            "Start Time": str(pd.to_datetime(r.start_time, unit='ms')) if r.start_time else "N/A",
            "Duration (s)": f"{(r.end_time - r.start_time) / 1000:.1f}" if (r.start_time and r.end_time) else "Running",
        } for r in job_runs])
        job_history_card = html.Div([
            html.H4(f"Job History: {TARGET_JOB_ID}", className="text-info mb-3"),
            html.P(f"Showing {len(runs_df)} run(s)", className="text-muted small mb-2"),
            html.Div(build_table(runs_df), style=SCROLL_STYLE)
        ])
    else:
        job_history_card = error_card("Job History", f"No runs found for job {TARGET_JOB_ID}")

    clusters = safe_call(lambda: list(w.clusters.list()), timeout=20, fallback=[])
    clusters_card = html.Div([
        html.H4("Clusters", className="text-info mb-3"),
        build_table(pd.DataFrame([{"Cluster": c.cluster_name, "State": str(c.state), "Runtime": c.spark_version} for c in clusters]))
    ]) if clusters else error_card("Clusters", "No clusters found or timeout")

    warehouses = safe_call(lambda: list(w.warehouses.list()), timeout=20, fallback=[])
    warehouses_card = html.Div([
        html.H4("SQL Warehouses", className="text-info mb-3"),
        build_table(pd.DataFrame([{"Warehouse": wh.name, "State": str(wh.state), "Size": str(wh.cluster_size)} for wh in warehouses]))
    ]) if warehouses else error_card("Warehouses", "No warehouses found or timeout")

    workspace_items = safe_call(lambda: list(w.workspace.list(path=TARGET_WS_PATH)), timeout=20, fallback=[])
    notebooks = [i for i in workspace_items if "NOTEBOOK" in str(i.object_type).upper()] if workspace_items else []
    if notebooks:
        ws_df = pd.DataFrame([
            {"Name": item.path.split("/")[-1], "Path": item.path, "Language": getattr(item, 'language', 'N/A')}
            for item in notebooks[:50]
        ])
        workspace_card = html.Div([
            html.H4(f"Notebooks ({TARGET_WS_PATH})", className="text-info mb-3"),
            html.Div(build_table(ws_df), style=SCROLL_STYLE),
        ])
    else:
        workspace_card = error_card("Workspace Notebooks", f"No notebooks in {TARGET_WS_PATH}")

    # --- Unity Catalog ---
    schemas = safe_call(lambda: list(w.schemas.list(catalog_name=TARGET_CATALOG)), timeout=20, fallback=[])
    all_tables, all_volumes = [], []
    if schemas:
        for s in schemas:
            sn = s.name
            t = safe_call(lambda _sn=sn: list(w.tables.list(catalog_name=TARGET_CATALOG, schema_name=_sn)), timeout=10, fallback=[])
            all_tables.extend(t)
            v = safe_call(lambda _sn=sn: list(w.volumes.list(catalog_name=TARGET_CATALOG, schema_name=_sn)), timeout=10, fallback=[])
            all_volumes.extend(v)

    schema_df = pd.DataFrame([{"Schema": s.name, "Catalog": s.catalog_name} for s in schemas[:15]]) if schemas else pd.DataFrame()
    vol_df = pd.DataFrame([
        {"Volume": getattr(v, 'name', '?'), "Schema": getattr(v, 'schema_name', '?'), "Type": str(getattr(v, 'volume_type', '?'))}
        for v in all_volumes[:20]
    ]) if all_volumes else pd.DataFrame()

    print("=" * 80)
    print("TABLE COUNT:", len(all_tables))
    for t in all_tables:
        print(getattr(t, "schema_name", "?"), getattr(t, "name", "?"))
    print("=" * 80)
    
    unity_card = html.Div([
        html.H4(f"Unity Catalog: {TARGET_CATALOG}", className="text-info mb-3"),
        html.H5("Schemas", className="text-light mt-2 mb-1"),
        build_table(schema_df) if not schema_df.empty else html.P("No schemas", className="text-muted"),
        html.H5("Tables (Click to Load Data)", className="text-light mt-3 mb-1"),
        build_unity_tables_card(all_tables, TARGET_CATALOG),
        html.H5("Volumes / Files", className="text-light mt-3 mb-1"),
        build_table(vol_df) if not vol_df.empty else html.P("No volumes", className="text-muted"),
    ])

    return (
        user_card, jobs_card, job_history_card, clusters_card,
        warehouses_card, workspace_card, unity_card
    )

# =================================================
# CALLBACK 2: DYNAMIC TABLE LOADING
# =================================================
@callback(
    Output("selected-table-output", "children"),
    Input({"type": "load-table-btn", "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def load_any_table(n_clicks):
    if not ctx.triggered:
        raise PreventUpdate

    # When "Load All Info" renders the unity-card, Dash fires this pattern-matching
    # callback automatically for all newly created buttons with n_clicks=None.
    # Bail out unless the triggered value is an actual click (value >= 1).
    if not ctx.triggered[0].get('value'):
        raise PreventUpdate

    # ctx.triggered_id is already parsed by Dash — no manual JSON splitting needed.
    # (Manual split('.')[0] broke because the index value itself contains dots,
    #  e.g. "test_cat.test_db.my_table", causing json.loads to see truncated JSON.)
    triggered_dict = ctx.triggered_id
    if not triggered_dict or 'index' not in triggered_dict:
        raise PreventUpdate

    index_val = triggered_dict['index']

    def fetch_data():
        conn = dbx_sql.connect(server_hostname=HOST_CLEAN, http_path=DBX_HTTP_PATH, access_token=DBX_TOKEN)
        cursor = conn.cursor()
        
        catalog, schema, table = index_val.split('.')
        query = f"SELECT * FROM `{catalog}`.`{schema}`.`{table}` LIMIT 100"
        title = f"Table Data: {catalog}.{schema}.{table}" 
        cursor.execute(query)
        result = cursor.fetchall_arrow().to_pandas()
    
        cursor.close()
        conn.close()
    
        return result, title

    result_tuple = safe_call(fetch_data, timeout=60, fallback=None)
    
    if result_tuple and result_tuple[0] is not None and not result_tuple[0].empty:
        df, title = result_tuple
        return html.Div([html.P(title, className="text-info fw-bold mb-2"), html.Div(build_table(df), style=SCROLL_STYLE)])
    else:
        return dbc.Alert(
            "Could not fetch data. Check warehouse state, permissions, or warehouse status.",
            color="warning",
        )
