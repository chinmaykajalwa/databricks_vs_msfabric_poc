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


def fit_iforest(X: pd.DataFrame):
    from sklearn.ensemble import IsolationForest
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    pipe = Pipeline([("scale", StandardScaler()),
                     ("iforest", IsolationForest(n_estimators=200, contamination=1 - THRESHOLD_QUANTILE,
                                                 random_state=SEED, n_jobs=-1))])
    return pipe.fit(X[FEATURES])


def score_iforest(pipe, X: pd.DataFrame) -> np.ndarray:
    return -pipe.score_samples(X[FEATURES])          # higher = more anomalous


def fit_autoencoder(X: pd.DataFrame, epochs: int = 15):
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
    h = layers.Dense(16, activation="relu")(z)
    h = layers.Dense(4, activation="relu", name="bottleneck")(h)
    h = layers.Dense(16, activation="relu")(h)
    rec = layers.Dense(n, name="reconstruction")(h)
    trainer = keras.Model(inp, rec)
    trainer.compile(optimizer=keras.optimizers.Adam(1e-3), loss="mse")
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
