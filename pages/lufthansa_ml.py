# 2026.06.01  11.00
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# ------------------------------------------------------------------
# 1.  Feature / target preparation
# ------------------------------------------------------------------
def prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Return a clean copy with delay columns and calendar features."""
    d = df.copy()
    d.columns = d.columns.str.strip()  # <--- STRIP WHITESPACE FROM COLUMNS
    d = d.replace({"null": np.nan})
    
    def _col(candidates):
        for c in candidates:
            if c in d.columns:
                return c
        return None

    dep_sch  = _col(["dep_sch_ts", "departure_scheduled_ts"])
    dep_act  = _col(["dep_act_ts", "departure_actual_ts"])
    arr_sch  = _col(["arr_sch_ts", "arrival_scheduled_ts"])
    arr_act  = _col(["arr_act_ts", "arrival_actual_ts"])

    for col in [dep_sch, dep_act, arr_sch, arr_act]:
        if col:
            d[col] = pd.to_datetime(d[col].astype(str), errors="coerce")

    if dep_sch and dep_act:
        d["dep_delay"] = (d[dep_act] - d[dep_sch]).dt.total_seconds() / 60
    if arr_sch and arr_act:
        d["arr_delay"] = (d[arr_act] - d[arr_sch]).dt.total_seconds() / 60
    if dep_sch:
        d["dep_hour"] = d[dep_sch].dt.hour
        d["dep_dow"]  = d[dep_sch].dt.dayofweek
        d["dep_sched"] = d[dep_sch]

    return d

# ------------------------------------------------------------------
# 2.  Train
# ------------------------------------------------------------------
FEATURES = ["dep_delay", "dep_hour", "dep_dow"]
TARGET   = "arr_delay"

def train(df: pd.DataFrame, learning_rate: float = 0.06, max_iter: int = 300, max_depth: int | None = None, random_state: int = 42) -> tuple:
    d = df.dropna(subset=[TARGET]).copy()
    X = d[FEATURES]
    y = d[TARGET].astype(float)

    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.20, random_state=random_state)

    model = HistGradientBoostingRegressor(
        learning_rate=learning_rate, max_iter=max_iter, max_depth=max_depth, random_state=random_state
    ).fit(X_tr, y_tr)

    y_pred = model.predict(X_te)
    metrics = {
        "rmse": float(np.sqrt(mean_squared_error(y_te, y_pred))),
        "mae":  float(mean_absolute_error(y_te, y_pred)),
        "r2":   float(r2_score(y_te, y_pred)),
        "n_train": len(X_tr),
        "n_test":  len(X_te),
    }
    return model, metrics

# ------------------------------------------------------------------
# 3.  Predict on latest N rows
# ------------------------------------------------------------------
def predict_latest(model, df: pd.DataFrame, n: int = 50) -> pd.DataFrame:
    latest = df.sort_values("dep_sched", ascending=False).head(n).copy()
    X = latest[FEATURES]
    latest["pred_arr_delay"] = model.predict(X)
    latest["error_min"] = latest["pred_arr_delay"] - latest["arr_delay"]
    out_cols = ["route_key", "dep_sched", "dep_delay", "arr_delay", "pred_arr_delay", "error_min"]

    return latest[[c for c in out_cols if c in latest.columns]].reset_index(drop=True)
