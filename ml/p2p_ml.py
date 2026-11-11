"""
p2p_ml: molding-machine anomaly detection, IDENTICAL code on Databricks and Fabric.
==================================================================================
One file on purpose: upload it next to the notebook (Databricks workspace folder) or to
Lakehouse Files (Fabric) and `import p2p_ml`. The platforms differ only in how they load data,
track experiments, register and serve models -- which is exactly what we compare.

Models (unsupervised; labels are used ONLY for offline evaluation against ground truth):
  baseline   robust z-score against the work order's setpoints (a model must beat this)
  iforest    scikit-learn Pipeline(StandardScaler, IsolationForest)
  autoenc    Keras dense autoencoder. The SCORING model computes the reconstruction error
             inside the network (standard layers only) and outputs ONE scalar per row, because
             Fabric's real-time endpoints accept Keras but need a scalar output schema and no
             custom code. Same model on both platforms.

Features (per machine-minute while RUNNING, relative to the active work order's mold setpoints;
built by FEATURE_SQL on the platform's SQL engine, finished in pandas by finish_features):
  melt_dev_c, melt_range_c, mold_dev_c, inj_dev_pct, cav_ratio, cycle_dev_pct, oil_temp_c
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

FEATURES = ["melt_dev_c", "melt_range_c", "mold_dev_c", "inj_dev_pct", "cav_ratio", "cycle_dev_pct", "oil_temp_c"]
SIGNALS = ["melt_temp_c", "mold_temp_c", "inj_pressure_peak_bar", "cavity_pressure_peak_bar", "cycle_time_s",
           "hyd_oil_temp_c", "machine_state"]
MOLDING_SCENARIOS = ["heater_band_failure", "barrel_overheat", "pressure_overshoot", "mold_cooling_loss"]
THRESHOLD_QUANTILE = 0.995      # raw per-minute threshold (also the IsolationForest contamination for endpoint labels)
ALERT_QUANTILE = 0.999          # operational alerts: 15-min smoothed score above this quantile of smoothed training scores
SEED = 20261027


# ---------------------------------------------------------------------------
# Features
# ---------------------------------------------------------------------------
# Heavy reshaping in SQL (Spark SQL on both platforms; DuckDB for local tests): pivot the molding
# signals per machine-minute, attach the active work order's mold setpoints with a range join, keep only
# fully-running minutes. Only the result (~2M rows x 15 cols for 90 days) goes to pandas: pivoting the raw
# 21M-row aggregate in pandas ran out of memory.
FEATURE_SQL = """
WITH t AS (
  SELECT tag_id, asset_id, signal FROM {tag_dim}
  WHERE asset_kind = 'molding_machine'
    AND signal IN ('melt_temp_c','mold_temp_c','inj_pressure_peak_bar','cavity_pressure_peak_bar',
                   'cycle_time_s','hyd_oil_temp_c','machine_state')),
w AS (
  SELECT t.asset_id, a.bucket_ts,
    MAX(CASE WHEN t.signal = 'melt_temp_c' THEN a.mean END) AS melt_temp_c,
    MAX(CASE WHEN t.signal = 'melt_temp_c' THEN a.max - a.min END) AS melt_range_c,
    MAX(CASE WHEN t.signal = 'mold_temp_c' THEN a.mean END) AS mold_temp_c,
    MAX(CASE WHEN t.signal = 'inj_pressure_peak_bar' THEN a.mean END) AS inj_pressure_peak_bar,
    MAX(CASE WHEN t.signal = 'cavity_pressure_peak_bar' THEN a.mean END) AS cavity_pressure_peak_bar,
    MAX(CASE WHEN t.signal = 'cycle_time_s' THEN a.mean END) AS cycle_time_s,
    MAX(CASE WHEN t.signal = 'hyd_oil_temp_c' THEN a.mean END) AS hyd_oil_temp_c,
    MAX(CASE WHEN t.signal = 'machine_state' THEN a.mean END) AS machine_state
  FROM {agg} a JOIN t ON a.tag_id = t.tag_id
  GROUP BY t.asset_id, a.bucket_ts)
SELECT w.*, o.work_order_id, m.melt_temp_setpoint_c, m.mold_temp_setpoint_c,
       m.injection_pressure_setpoint_bar, m.nominal_cycle_s
FROM w
JOIN {work_orders} o ON o.resource_id = w.asset_id AND w.bucket_ts >= o.start_ts AND w.bucket_ts < o.end_ts
JOIN {molds} m ON m.mold_id = o.mold_id
WHERE w.machine_state >= 0.999
"""


def feature_sql(agg: str, tag_dim: str, work_orders: str, molds: str) -> str:
    return FEATURE_SQL.format(agg=agg, tag_dim=tag_dim, work_orders=work_orders, molds=molds)


def finish_features(wide: pd.DataFrame) -> pd.DataFrame:
    """pandas step on the SQL result: carry per-shot signals across minutes without a shot, derive features."""
    f = wide.copy()
    f["bucket_ts"] = pd.to_datetime(f["bucket_ts"], utc=True)
    f = f.sort_values(["asset_id", "bucket_ts"])
    for c in ("inj_pressure_peak_bar", "cavity_pressure_peak_bar", "cycle_time_s"):   # shots every 8-40 s
        f[c] = f.groupby("work_order_id")[c].ffill(limit=2)
    f["melt_dev_c"] = f["melt_temp_c"] - f["melt_temp_setpoint_c"]
    f["mold_dev_c"] = f["mold_temp_c"] - f["mold_temp_setpoint_c"]
    f["inj_dev_pct"] = 100 * (f["inj_pressure_peak_bar"] / f["injection_pressure_setpoint_bar"] - 1)
    f["cav_ratio"] = f["cavity_pressure_peak_bar"] / f["injection_pressure_setpoint_bar"]
    f["cycle_dev_pct"] = 100 * (f["cycle_time_s"] / f["nominal_cycle_s"] - 1)
    f["oil_temp_c"] = f["hyd_oil_temp_c"]
    f = f.dropna(subset=FEATURES)
    for c in FEATURES:
        f[c] = f[c].astype("float32")
    return f[["asset_id", "bucket_ts", "work_order_id"] + FEATURES].reset_index(drop=True)


def split(features: pd.DataFrame, train_days: int = 45):
    t0 = features["bucket_ts"].min()
    cut = t0 + pd.Timedelta(days=train_days)
    return features[features["bucket_ts"] < cut], features[features["bucket_ts"] >= cut], cut


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class Baseline:
    """Robust z-score per feature (median / MAD from training); score = max |z|."""

    def fit(self, X: pd.DataFrame):
        self.med = X[FEATURES].median()
        self.mad = (X[FEATURES] - self.med).abs().median().replace(0, 1e-6) * 1.4826
        return self

    def score(self, X: pd.DataFrame) -> np.ndarray:
        return ((X[FEATURES] - self.med) / self.mad).abs().max(axis=1).to_numpy()

    def to_json(self) -> str:
        return json.dumps({"median": self.med.to_dict(), "mad": self.mad.to_dict()})


def fit_iforest(X: pd.DataFrame, params: dict | None = None):
    from sklearn.ensemble import IsolationForest
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    pipe = Pipeline([("scale", StandardScaler()),
                     ("iforest", IsolationForest(n_estimators=int((params or {}).get("n_estimators", 200)),
                                                 max_samples=float((params or {}).get("max_samples", 1.0)) if (params or {}).get("max_samples") else "auto",
                                                 max_features=float((params or {}).get("max_features", 1.0)),
                                                 contamination=1 - THRESHOLD_QUANTILE, random_state=SEED, n_jobs=-1))])
    return pipe.fit(X[FEATURES])


def score_iforest(pipe, X: pd.DataFrame) -> np.ndarray:
    return -pipe.score_samples(X[FEATURES])          # higher = more anomalous


def fit_autoencoder(X: pd.DataFrame, epochs: int = 15, params: dict | None = None):
    """Returns the SCORING model: features -> one scalar (mean squared reconstruction error)."""
    import keras
    from keras import layers
    keras.utils.set_random_seed(SEED)
    n = len(FEATURES)
    x = X[FEATURES].to_numpy("float32")
    inp = keras.Input(shape=(n,), name="features")
    norm = layers.Normalization(name="norm")
    norm.adapt(x)
    z = norm(inp)
    p = params or {}
    hidden, neck = int(p.get("hidden", 16)), int(p.get("bottleneck", 4))
    epochs = int(p.get("epochs", epochs))
    h = layers.Dense(hidden, activation="relu")(z)
    h = layers.Dense(neck, activation="relu", name="bottleneck")(h)
    h = layers.Dense(hidden, activation="relu")(h)
    rec = layers.Dense(n, name="reconstruction")(h)
    trainer = keras.Model(inp, rec)
    trainer.compile(optimizer=keras.optimizers.Adam(float(p.get("lr", 1e-3))), loss="mse")
    trainer.fit(x, np.asarray(norm(x)), epochs=epochs, batch_size=1024, verbose=0, validation_split=0.1, shuffle=True)
    diff = layers.Subtract()([z, rec])
    sq = layers.Multiply()([diff, diff])
    score = layers.Dense(1, use_bias=False, trainable=False, name="anomaly_score",
                         kernel_initializer=keras.initializers.Constant(1.0 / n))(sq)
    return keras.Model(inp, score, name="molding_autoencoder_scorer")


def score_autoencoder(model, X: pd.DataFrame) -> np.ndarray:
    return model.predict(X[FEATURES].to_numpy("float32"), batch_size=8192, verbose=0).ravel()


def train_all(features: pd.DataFrame, train_days: int = 45, epochs: int = 15) -> dict:
    """Fit all three models on the training period; thresholds = training-score quantile."""
    train, test, cut = split(features, train_days)
    b = Baseline().fit(train)
    ifo = fit_iforest(train)
    ae = fit_autoencoder(train, epochs)
    thr = {"baseline": float(np.quantile(b.score(train), THRESHOLD_QUANTILE)),
           "iforest": float(np.quantile(score_iforest(ifo, train), THRESHOLD_QUANTILE)),
           "autoenc": float(np.quantile(score_autoencoder(ae, train), THRESHOLD_QUANTILE))}
    return {"baseline": b, "iforest": ifo, "autoenc": ae, "thresholds": thr, "train_cut": cut,
            "n_train": len(train), "n_test": len(test)}


def score_all(models: dict, features: pd.DataFrame) -> pd.DataFrame:
    out = []
    for name, fn in (("baseline", lambda X: models["baseline"].score(X)),
                     ("iforest", lambda X: score_iforest(models["iforest"], X)),
                     ("autoenc", lambda X: score_autoencoder(models["autoenc"], X))):
        s = fn(features)
        out.append(pd.DataFrame({"asset_id": features["asset_id"].to_numpy(), "bucket_ts": features["bucket_ts"].to_numpy(),
                                 "model": name, "score": s, "flag": s > models["thresholds"][name]}))
    return pd.concat(out, ignore_index=True)


def alerts(scores: pd.DataFrame, train_cut, window: str = "15min", quantile: float = ALERT_QUANTILE) -> pd.DataFrame:
    """Operational alerting, identical for every model: smooth each machine's score over `window`
    (a precursor must PERSIST), threshold at `quantile` of the smoothed TRAINING scores.
    Raw per-minute flags at the 99.5th percentile produced 3-5 false alarms per machine-day."""
    s = scores.copy()
    s["bucket_ts"] = pd.to_datetime(s["bucket_ts"], utc=True)
    s = s.sort_values(["model", "asset_id", "bucket_ts"])
    s["score_smooth"] = (s.set_index("bucket_ts").groupby(["model", "asset_id"])["score"]
                         .rolling(window).mean().to_numpy())
    train = s[s["bucket_ts"] < pd.Timestamp(train_cut)]
    thr = train.groupby("model")["score_smooth"].quantile(quantile)
    s["alert_threshold"] = s["model"].map(thr)
    s["flag"] = s["score_smooth"] > s["alert_threshold"]
    return s.reset_index(drop=True)


# ---------------------------------------------------------------------------
# MLflow (same calls on both platforms; registry names differ)
# ---------------------------------------------------------------------------
def log_and_register(models: dict, features: pd.DataFrame, names: dict, experiment: str) -> dict:
    """names = {"iforest": <registry name>, "autoenc": <registry name>}. Returns registered versions."""
    import mlflow
    from mlflow.models import infer_signature
    mlflow.set_experiment(experiment)
    sample = features[FEATURES].head(200).astype("float64")
    versions = {}
    with mlflow.start_run(run_name="molding_anomaly") as run:
        mlflow.log_params({"features": ",".join(FEATURES), "threshold_quantile": THRESHOLD_QUANTILE,
                           "train_cut": str(models["train_cut"]), "n_train": models["n_train"]})
        mlflow.log_metrics({f"threshold_{k}": v for k, v in models["thresholds"].items()})
        mlflow.log_dict(json.loads(models["baseline"].to_json()), "baseline/params.json")
        # column-based in AND out (named output column): the shape Fabric's preview endpoints document
        sig_if = infer_signature(sample, pd.DataFrame({"prediction": models["iforest"].predict(sample)}))
        # cloudpickle pinned: MLflow 3.x defaults to skops (extra package); older platform runtimes default to
        # cloudpickle. Pinning makes the saved model identical on both platforms whatever the runtime version.
        mlflow.sklearn.log_model(models["iforest"], "iforest", signature=sig_if, input_example=sample.head(5),
                                 serialization_format=mlflow.sklearn.SERIALIZATION_FORMAT_CLOUDPICKLE)
        # Keras flavour REQUIRES a tensor signature (float32 array [n, 7] -> [n, 1]). Fabric's preview real-time
        # endpoints are documented to need scalar/column schemas: whether they serve this model is a test on Fabric.
        x = sample.to_numpy("float32")
        sig_ae = infer_signature(x, score_autoencoder(models["autoenc"], sample).reshape(-1, 1).astype("float32"))
        try:
            import mlflow.keras as mk                          # Keras 3 flavour (MLflow >= 2.12)
        except ImportError:
            mk = None
        if mk is not None:
            mk.log_model(models["autoenc"], "autoenc", signature=sig_ae)
        else:                                                  # older runtimes: TensorFlow flavour
            mlflow.tensorflow.log_model(models["autoenc"], "autoenc", signature=sig_ae)
        for key in ("iforest", "autoenc"):
            mv = mlflow.register_model(f"runs:/{run.info.run_id}/{key}", names[key])
            versions[key] = mv.version
    return versions


# ---------------------------------------------------------------------------
# Offline evaluation against ground truth (run on your laptop, never in the platforms)
# ---------------------------------------------------------------------------
def evaluate(scores: pd.DataFrame, anomaly_log: pd.DataFrame, after: pd.Timestamp | None = None,
             gap_min: int = 5) -> pd.DataFrame:
    """Event-level metrics per model.
    detected     any flag on the machine between precursor start and the deviation being opened
    lead_min     deviation opened minus first flag (how early the model warned)
    false_alarms alerts (runs of flagged minutes, gaps <= gap_min) not overlapping any precursor window
    """
    s = scores.copy()
    s["bucket_ts"] = pd.to_datetime(s["bucket_ts"], utc=True)
    ev = anomaly_log[anomaly_log["scenario"].isin(MOLDING_SCENARIOS)].copy()
    ev["start_ts"] = pd.to_datetime(ev["start_ts"], utc=True)
    ev["opened_ts"] = pd.to_datetime(ev["end_ts"], utc=True) + pd.Timedelta(hours=2)
    if after is not None:
        ev = ev[ev["start_ts"] >= after]
        s = s[s["bucket_ts"] >= after]
    rows = []
    days = (s["bucket_ts"].max() - s["bucket_ts"].min()).total_seconds() / 86400
    machines = s["asset_id"].nunique()
    for model, g in s.groupby("model"):
        f = g[g["flag"]].sort_values(["asset_id", "bucket_ts"])
        det, leads = 0, []
        for _, e in ev.iterrows():
            hit = f[(f["asset_id"] == e["asset_id"]) & (f["bucket_ts"] >= e["start_ts"]) & (f["bucket_ts"] < e["opened_ts"])]
            if len(hit):
                det += 1
                leads.append((e["opened_ts"] - hit["bucket_ts"].min()).total_seconds() / 60)
        # alerts = contiguous flagged runs per machine
        f = f.assign(new=(f.groupby("asset_id")["bucket_ts"].diff() > pd.Timedelta(minutes=gap_min)) |
                     f["asset_id"].ne(f["asset_id"].shift()))
        f["alert"] = f["new"].cumsum()
        alerts = f.groupby("alert").agg(asset_id=("asset_id", "first"), t0=("bucket_ts", "min"), t1=("bucket_ts", "max"))
        false = 0
        for _, a in alerts.iterrows():
            e = ev[ev["asset_id"] == a["asset_id"]]
            if not ((e["start_ts"] <= a["t1"]) & (e["opened_ts"] > a["t0"])).any():
                false += 1
        rows.append({"model": model, "events": len(ev), "detected": det,
                     "recall": round(det / len(ev), 3) if len(ev) else None,
                     "median_lead_min": round(float(np.median(leads)), 0) if leads else None,
                     "alerts": len(alerts), "false_alarms": false,
                     "alert_precision": round(1 - false / len(alerts), 3) if len(alerts) else None,
                     "false_alarms_per_machine_day": round(false / max(machines * days, 1e-9), 3)})
    return pd.DataFrame(rows).sort_values("model").reset_index(drop=True)

# ===========================================================================
# v2: MLOps structure (features table -> experiment -> champion -> batch / streaming inference)
# ===========================================================================
ALERT_WINDOW = "15min"
LABEL_HORIZON_H = 12            # an alert counts as an early warning if a deviation is opened within 12 h
CANDIDATES = ("baseline", "iforest", "autoenc")
CHAMPION_ALIAS = "champion"
MAX_FALSE_ALARMS_PER_MACHINE_DAY = 0.1

try:                                                   # mlflow is present on both platforms
    from mlflow.pyfunc import PythonModel as _PyfuncBase
except Exception:                                      # pragma: no cover
    _PyfuncBase = object


class AnomalyScorer(_PyfuncBase):
    """ONE serving interface for every candidate: DataFrame of FEATURES in, `anomaly_score` out.
    Lets the batch and streaming inference jobs load whichever model is champion, by alias,
    without knowing its type. The module file travels with the model (code_paths)."""

    def load_context(self, context):
        meta = json.load(open(context.artifacts["meta"]))
        self.kind, self.features = meta["kind"], meta["features"]
        if self.kind == "iforest":
            import joblib
            self.model = joblib.load(context.artifacts["model"])
        elif self.kind == "autoenc":
            import keras
            self.model = keras.saving.load_model(context.artifacts["model"])
        else:
            p = json.load(open(context.artifacts["model"]))
            self.med, self.mad = pd.Series(p["median"]), pd.Series(p["mad"])

    def predict(self, context, model_input, params=None):
        X = model_input[self.features].astype("float32")
        if self.kind == "iforest":
            s = -self.model.score_samples(X)
        elif self.kind == "autoenc":
            s = self.model.predict(X.to_numpy("float32"), batch_size=8192, verbose=0).ravel()
        else:
            s = ((X - self.med) / self.mad).abs().max(axis=1).to_numpy()
        return pd.DataFrame({"anomaly_score": np.asarray(s, dtype="float64")})


def fit_candidate(kind: str, train: pd.DataFrame, epochs: int = 6, params: dict | None = None):
    if kind == "baseline":
        return Baseline().fit(train)
    if kind == "iforest":
        return fit_iforest(train, params)
    return fit_autoencoder(train, epochs, params)


def score_candidate(kind: str, model, X: pd.DataFrame) -> np.ndarray:
    return {"baseline": lambda: model.score(X), "iforest": lambda: score_iforest(model, X),
            "autoenc": lambda: score_autoencoder(model, X)}[kind]()


def smooth_flags(scores: pd.DataFrame, threshold: float, window: str = ALERT_WINDOW) -> pd.DataFrame:
    """scores: asset_id, bucket_ts, score. Per machine, mean over `window` (a precursor must persist),
    flag above `threshold`. Same rule in training, batch and streaming."""
    s = scores.copy()
    s["bucket_ts"] = pd.to_datetime(s["bucket_ts"], utc=True)
    s = s.sort_values(["asset_id", "bucket_ts"])
    s["score_smooth"] = s.set_index("bucket_ts").groupby("asset_id")["score"].rolling(window).mean().to_numpy()
    s["flag"] = s["score_smooth"] > threshold
    return s.reset_index(drop=True)


def alert_threshold(kind: str, model, train: pd.DataFrame, window: str = ALERT_WINDOW) -> float:
    tr = pd.DataFrame({"asset_id": train["asset_id"].to_numpy(), "bucket_ts": train["bucket_ts"].to_numpy(),
                       "score": score_candidate(kind, model, train)})
    return float(smooth_flags(tr, np.inf, window)["score_smooth"].quantile(ALERT_QUANTILE))


def _alert_runs(flagged: pd.DataFrame, gap_min: int = 5) -> pd.DataFrame:
    f = flagged[flagged["flag"]].sort_values(["asset_id", "bucket_ts"])
    if f.empty:
        return pd.DataFrame(columns=["asset_id", "t0", "t1"])
    new = (f.groupby("asset_id")["bucket_ts"].diff() > pd.Timedelta(minutes=gap_min)) | f["asset_id"].ne(f["asset_id"].shift())
    f = f.assign(alert=new.cumsum())
    return f.groupby("alert").agg(asset_id=("asset_id", "first"), t0=("bucket_ts", "min"), t1=("bucket_ts", "max")).reset_index(drop=True)


def deviation_events(deviations: pd.DataFrame, start, end) -> pd.DataFrame:
    """On-platform BUSINESS labels: molding deviations recorded in the quality system (txn_deviations)."""
    d = deviations[(deviations["category"] == "molding") & (deviations["asset_type"] == "molding_machine")].copy()
    d["opened_ts"] = pd.to_datetime(d["opened_ts"], utc=True)
    d = d[(d["opened_ts"] >= pd.Timestamp(start)) & (d["opened_ts"] < pd.Timestamp(end))]
    return d[["deviation_id", "asset_id", "opened_ts", "type_key"]].reset_index(drop=True)


def evaluate_vs_deviations(flagged: pd.DataFrame, deviations: pd.DataFrame, start, end,
                           horizon_h: int = LABEL_HORIZON_H) -> dict:
    """recall: deviations preceded by an alert on that machine within horizon_h; lead: minutes of warning;
    false alarms: alerts not followed by a deviation on that machine within horizon_h."""
    ev = deviation_events(deviations, start, end)
    fl = flagged[(flagged["bucket_ts"] >= pd.Timestamp(start)) & (flagged["bucket_ts"] < pd.Timestamp(end))]
    runs = _alert_runs(fl)
    h = pd.Timedelta(hours=horizon_h)
    det, leads = 0, []
    for _, e in ev.iterrows():
        r = runs[(runs["asset_id"] == e["asset_id"]) & (runs["t1"] >= e["opened_ts"] - h) & (runs["t0"] < e["opened_ts"])]
        if len(r):
            det += 1
            leads.append((e["opened_ts"] - max(r["t0"].min(), e["opened_ts"] - h)).total_seconds() / 60)
    false = 0
    for _, a in runs.iterrows():
        e = ev[(ev["asset_id"] == a["asset_id"]) & (ev["opened_ts"] > a["t0"]) & (ev["opened_ts"] <= a["t1"] + h)]
        false += e.empty
    days = max((pd.Timestamp(end) - pd.Timestamp(start)).total_seconds() / 86400, 1e-9)
    machines = max(fl["asset_id"].nunique(), 1)
    return {"events": len(ev), "detected": det, "recall": round(det / len(ev), 3) if len(ev) else 0.0,
            "median_lead_min": round(float(np.median(leads)), 1) if leads else 0.0,
            "alerts": len(runs), "false_alarms": int(false),
            "false_alarms_per_machine_day": round(false / (machines * days), 4)}


def log_candidate(kind: str, model, train: pd.DataFrame, valid_metrics: dict, threshold: float,
                  train_cut, experiment: str, params: dict | None = None, tuning_mode: str = "single") -> str:
    """One MLflow run per candidate: params, validation metrics, and the AnomalyScorer pyfunc
    (artifacts: fitted model + meta). Returns the model URI."""
    import tempfile, joblib, mlflow
    from mlflow.models import infer_signature
    mlflow.set_experiment(experiment)
    tmp = tempfile.mkdtemp()
    meta = {"kind": kind, "features": FEATURES, "alert_threshold": threshold, "alert_window": ALERT_WINDOW}
    json.dump(meta, open(f"{tmp}/meta.json", "w"))
    if kind == "iforest":
        path = f"{tmp}/model.joblib"; joblib.dump(model, path)
    elif kind == "autoenc":
        path = f"{tmp}/model.keras"; model.save(path)
    else:
        path = f"{tmp}/model.json"; open(path, "w").write(model.to_json())
    X = train[FEATURES].head(200).astype("float32")
    sig = infer_signature(X, pd.DataFrame({"anomaly_score": score_candidate(kind, model, X)}))
    with mlflow.start_run(run_name=f"molding_{kind}") as run:
        mlflow.set_tag("candidate", kind)
        mlflow.set_tag("tuning_mode", tuning_mode)
        if params:
            mlflow.log_params({f"hp_{k}": v for k, v in params.items()})
        mlflow.log_params({"kind": kind, "features": ",".join(FEATURES), "alert_window": ALERT_WINDOW,
                           "alert_quantile": ALERT_QUANTILE, "train_cut": str(train_cut), "n_train": len(train)})
        mlflow.log_metric("alert_threshold", threshold)
        mlflow.log_metrics({f"val_{k}": float(v) for k, v in valid_metrics.items()})
        kwargs = dict(python_model=AnomalyScorer(), artifacts={"model": path, "meta": f"{tmp}/meta.json"},
                      signature=sig, input_example=X.head(3))
        try:
            mlflow.pyfunc.log_model("scorer", code_paths=[__file__], **kwargs)
        except TypeError:                                          # older MLflow
            mlflow.pyfunc.log_model("scorer", code_path=[__file__], **kwargs)
        return f"runs:/{run.info.run_id}/scorer"


def pick_champion(results: list[dict]) -> dict:
    """Champion rule (stated up front, same on both platforms): among candidates with at most
    MAX_FALSE_ALARMS_PER_MACHINE_DAY, highest recall, then longest median lead time."""
    ok = [r for r in results if r["metrics"]["false_alarms_per_machine_day"] <= MAX_FALSE_ALARMS_PER_MACHINE_DAY] or results
    return max(ok, key=lambda r: (r["metrics"]["recall"], r["metrics"]["median_lead_min"]))


def register_champion(model_uri: str, name: str) -> str:
    """Register and point the 'champion' alias at it; falls back to a version tag where aliases
    are not supported."""
    import mlflow
    from mlflow import MlflowClient
    mv = mlflow.register_model(model_uri, name)
    c = MlflowClient()
    try:
        c.set_registered_model_alias(name, CHAMPION_ALIAS, mv.version)
    except Exception as exc:                                       # record the gap as a finding
        print(f"[champion] alias not supported here ({type(exc).__name__}); using a version tag instead")
        c.set_model_version_tag(name, mv.version, "role", CHAMPION_ALIAS)
    return str(mv.version)


def load_champion(name: str):
    """(pyfunc model, alert threshold, version) for the current champion: alias first, tag fallback."""
    import mlflow
    from mlflow import MlflowClient
    c = MlflowClient()
    try:
        mv = c.get_model_version_by_alias(name, CHAMPION_ALIAS)
    except Exception:
        vs = [v for v in c.search_model_versions(f"name='{name}'") if (v.tags or {}).get("role") == CHAMPION_ALIAS]
        mv = max(vs, key=lambda v: int(v.version))
    model = mlflow.pyfunc.load_model(f"models:/{name}/{mv.version}")
    thr = float(c.get_run(mv.run_id).data.metrics["alert_threshold"])
    return model, thr, str(mv.version)


def score_features(model, features: pd.DataFrame) -> pd.DataFrame:
    s = model.predict(features[FEATURES])
    return pd.DataFrame({"asset_id": features["asset_id"].to_numpy(), "bucket_ts": features["bucket_ts"].to_numpy(),
                         "score": s["anomaly_score"].to_numpy()})


# ---- streaming: per-minute machine aggregates -> features -> score (shared by both platforms) ----
def minute_aggregates_to_wide(agg: pd.DataFrame) -> pd.DataFrame:
    """agg: asset_id, minute, signal, mean_v, min_v, max_v (from the platform's streaming aggregation).
    Returns the same wide shape FEATURE_SQL produces (minus setpoints)."""
    w = agg.pivot_table(index=["asset_id", "minute"], columns="signal", values="mean_v", aggfunc="first")
    rng = agg[agg["signal"] == "melt_temp_c"].set_index(["asset_id", "minute"])
    w["melt_range_c"] = (rng["max_v"] - rng["min_v"]).reindex(w.index)
    w = w.reset_index().rename(columns={"minute": "bucket_ts"})
    for c in SIGNALS:
        if c not in w:
            w[c] = np.nan
    return w


def attach_setpoints(wide: pd.DataFrame, work_orders: pd.DataFrame, molds: pd.DataFrame) -> pd.DataFrame:
    wo = work_orders[work_orders["resource_id"].astype(str).str.startswith("IMM")].merge(
        molds[["mold_id", "melt_temp_setpoint_c", "mold_temp_setpoint_c", "injection_pressure_setpoint_bar",
               "nominal_cycle_s"]], on="mold_id").rename(columns={"resource_id": "asset_id"})
    wo["start_ts"] = pd.to_datetime(wo["start_ts"], utc=True)
    wo["end_ts"] = pd.to_datetime(wo["end_ts"], utc=True)
    w = wide.copy()
    w["bucket_ts"] = pd.to_datetime(w["bucket_ts"], utc=True)
    f = pd.merge_asof(w.sort_values("bucket_ts"), wo.sort_values("start_ts")[
        ["asset_id", "start_ts", "end_ts", "work_order_id", "melt_temp_setpoint_c", "mold_temp_setpoint_c",
         "injection_pressure_setpoint_bar", "nominal_cycle_s"]],
        left_on="bucket_ts", right_on="start_ts", by="asset_id", direction="backward")
    f = f[(f["bucket_ts"] < f["end_ts"]) & (f["machine_state"] >= 0.999)]
    return f.drop(columns=["start_ts", "end_ts"])


def stream_microbatch(agg: pd.DataFrame, recent_scores: pd.DataFrame, model, threshold: float,
                      work_orders: pd.DataFrame, molds: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """One micro-batch of COMPLETED minutes -> (new scores, new alert runs, stats).
    recent_scores: the last ~20 minutes already scored (for the 15-minute smoothing)."""
    stats = {"machines_in_batch": int(agg["asset_id"].nunique()) if len(agg) else 0}
    if agg.empty:
        return pd.DataFrame(), pd.DataFrame(), stats
    feats = finish_features(attach_setpoints(minute_aggregates_to_wide(agg), work_orders, molds))
    stats["machines_scored"] = int(feats["asset_id"].nunique()) if len(feats) else 0   # drops if tags go unknown
    if feats.empty:
        return pd.DataFrame(), pd.DataFrame(), stats
    new = score_features(model, feats)
    hist = pd.concat([recent_scores[["asset_id", "bucket_ts", "score"]], new], ignore_index=True) if len(recent_scores) else new
    flagged = smooth_flags(hist, threshold)
    new_flagged = flagged.merge(new[["asset_id", "bucket_ts"]].assign(bucket_ts=lambda d: pd.to_datetime(d["bucket_ts"], utc=True)),
                                on=["asset_id", "bucket_ts"])
    return new_flagged, _alert_runs(new_flagged), stats


# ===========================================================================
# Hyperparameter tuning: one objective and search space, three execution modes
# ===========================================================================
TUNABLE = ("iforest", "autoenc")                     # the baseline has no hyperparameters
SEARCH_SPACE = {                                     # framework-neutral; converted per tuning library below
    "iforest": {"n_estimators": ("int", 100, 400), "max_samples": ("float", 0.05, 1.0), "max_features": ("float", 0.5, 1.0)},
    "autoenc": {"hidden": ("int", 8, 32), "bottleneck": ("int", 2, 6), "lr": ("log", 1e-4, 1e-2), "epochs": ("int", 3, 10)},
}
TUNE_DAYS = 15                                       # last 15 of the 45 training days validate the trials


def tuning_split(train: pd.DataFrame, tune_days: int = TUNE_DAYS, sample: float = 0.25):
    """Train days 1-30 (fit) / days 31-45 (score trials). The final 45 days stay untouched for the champion.
    `sample` subsamples the fit rows to keep trials cheap (and data shipped to workers small)."""
    cut = train["bucket_ts"].max() - pd.Timedelta(days=tune_days)
    fit, val = train[train["bucket_ts"] < cut], train[train["bucket_ts"] >= cut]
    if sample < 1:
        fit = fit.sample(frac=sample, random_state=SEED).sort_values(["asset_id", "bucket_ts"])
    return fit, val, cut


def tuning_objective(kind: str, params: dict, fit: pd.DataFrame, val: pd.DataFrame, deviations: pd.DataFrame) -> dict:
    """One trial: fit on `fit`, threshold on `fit`, alerts and business-label metrics on `val`.
    objective = 100*recall + lead hours - 50*false alarms per machine-day (higher is better)."""
    m = fit_candidate(kind, fit, params=params)
    thr = alert_threshold(kind, m, fit)
    s = pd.DataFrame({"asset_id": val["asset_id"].to_numpy(), "bucket_ts": val["bucket_ts"].to_numpy(),
                      "score": score_candidate(kind, m, val)})
    met = evaluate_vs_deviations(smooth_flags(s, thr), deviations, val["bucket_ts"].min(), val["bucket_ts"].max())
    met["objective"] = round(100 * met["recall"] + met["median_lead_min"] / 60 - 50 * met["false_alarms_per_machine_day"], 3)
    return met


def optuna_space(trial, kind: str) -> dict:
    out = {}
    for k, (t, lo, hi) in SEARCH_SPACE[kind].items():
        out[k] = (trial.suggest_int(k, lo, hi) if t == "int" else
                  trial.suggest_float(k, lo, hi, log=(t == "log")))
    return out


def ray_space(kind: str) -> dict:
    from ray import tune
    return {k: (tune.randint(lo, hi + 1) if t == "int" else tune.loguniform(lo, hi) if t == "log" else tune.uniform(lo, hi))
            for k, (t, lo, hi) in SEARCH_SPACE[kind].items()}


def flaml_space(kind: str) -> dict:
    from flaml import tune
    return {k: (tune.randint(lo, hi + 1) if t == "int" else tune.loguniform(lo, hi) if t == "log" else tune.uniform(lo, hi))
            for k, (t, lo, hi) in SEARCH_SPACE[kind].items()}


def tune_optuna(kind: str, fit, val, deviations, n_trials: int = 8) -> list[dict]:
    """Sequential trials on the driver (Optuna TPE). Returns [{params, metrics}] in trial order."""
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    trials = []

    def obj(trial):
        p = optuna_space(trial, kind)
        met = tuning_objective(kind, p, fit, val, deviations)
        trials.append({"params": p, "metrics": met})
        return met["objective"]
    optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=SEED)).optimize(obj, n_trials=n_trials)
    return trials


def log_trials(kind: str, trials: list[dict], experiment: str, mode: str, wall_seconds: float) -> dict:
    """Driver-side logging (identical for every mode): a parent run per tuned model, one child run per trial.
    Returns the best trial."""
    import mlflow
    mlflow.set_experiment(experiment)
    best = max(trials, key=lambda t: t["metrics"]["objective"])
    with mlflow.start_run(run_name=f"tune_{kind}_{mode}"):
        mlflow.set_tags({"stage": "tuning", "candidate": kind, "tuning_mode": mode})
        mlflow.log_metrics({"n_trials": len(trials), "tuning_wall_seconds": round(wall_seconds, 1),
                            "best_objective": best["metrics"]["objective"]})
        mlflow.log_params({f"best_{k}": v for k, v in best["params"].items()})
        for i, t in enumerate(trials):
            with mlflow.start_run(run_name=f"trial_{i:02d}", nested=True):
                mlflow.log_params(t["params"])
                mlflow.log_metrics({k: float(v) for k, v in t["metrics"].items()})
    return best
