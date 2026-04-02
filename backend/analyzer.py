"""Multi-dataset analytics engine powered by Pandas."""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

DATA_PATH = Path(__file__).parent.parent / "data" / "filesystem.json"

# ── Multi-dataset workspace ──────────────────────────────────────────

_datasets: dict[str, pd.DataFrame] = {}
_active_dataset: str = ""
_links: pd.DataFrame = pd.DataFrame()
_dynamic_links: list[dict] = []  # AI-created links
_dataset_name: str = ""
_raw_points: list[dict] | None = None
_snapshots: list[dict] = []

# Legacy aliases
_df_points = None
_df_links = None


def load_data():
    """Load filesystem.json as the default dataset."""
    global _df_links
    if not DATA_PATH.exists():
        return {"points": 0, "links": 0, "root": "none"}

    with open(DATA_PATH) as f:
        data = json.load(f)

    df = pd.DataFrame(data["points"])
    _df_links = pd.DataFrame(data["links"])

    df["extension"] = df["label"].apply(
        lambda x: Path(x).suffix.lower() if "." in x else ""
    )
    df["project"] = df["id"].apply(
        lambda x: x.split("/")[0] if "/" in x and x != "." else x
    )

    register_dataset("filesystem", df, _df_links)
    set_active("filesystem")

    return {
        "points": len(df),
        "links": len(_df_links),
        "root": data["root"],
    }


def register_dataset(name: str, df: pd.DataFrame, links: pd.DataFrame | None = None):
    """Register a dataset in the workspace."""
    global _datasets, _links, _raw_points, _dataset_name, _df_points, _df_links

    if "index" not in df.columns:
        df = df.copy()
        df["index"] = range(len(df))

    _datasets[name] = df
    _dataset_name = name
    _df_points = df
    _raw_points = df.to_dict(orient="records")

    if links is not None and len(links) > 0:
        _links = links
        _df_links = links
    else:
        _links = pd.DataFrame()
        _df_links = pd.DataFrame()


def set_active(name: str):
    """Set the active dataset."""
    global _active_dataset, _df_points, _raw_points, _dataset_name
    if name in _datasets:
        _active_dataset = name
        _dataset_name = name
        _df_points = _datasets[name]
        _raw_points = _df_points.to_dict(orient="records")


def get_df(name: str | None = None) -> pd.DataFrame:
    """Get a dataset by name, or the active one."""
    if name and name in _datasets:
        return _datasets[name]
    if _active_dataset and _active_dataset in _datasets:
        return _datasets[_active_dataset]
    if _df_points is not None:
        return _df_points
    # Return empty DataFrame if nothing loaded
    return pd.DataFrame()


def list_datasets() -> dict:
    """List all loaded datasets."""
    return {
        "datasets": {
            name: {"rows": len(df), "columns": list(df.columns)}
            for name, df in _datasets.items()
        },
        "active": _active_dataset,
    }


# ── Cross-dataset connections ────────────────────────────────────────


def find_connections(dataset_a: str, dataset_b: str) -> dict:
    """Find potential foreign key relationships between two datasets."""
    if dataset_a not in _datasets or dataset_b not in _datasets:
        return {"error": f"Dataset not found. Available: {list(_datasets.keys())}"}

    df_a = _datasets[dataset_a]
    df_b = _datasets[dataset_b]
    connections = []

    for col_a in df_a.columns:
        vals_a = set(df_a[col_a].dropna().astype(str).unique())
        if len(vals_a) < 2 or len(vals_a) > len(df_a) * 0.95:
            continue  # skip constants and near-unique columns

        for col_b in df_b.columns:
            vals_b = set(df_b[col_b].dropna().astype(str).unique())
            if len(vals_b) < 2:
                continue

            overlap = vals_a & vals_b
            if len(overlap) >= 2:
                score = len(overlap) / min(len(vals_a), len(vals_b))
                if score > 0.1:
                    connections.append({
                        "column_a": f"{dataset_a}.{col_a}",
                        "column_b": f"{dataset_b}.{col_b}",
                        "overlap_count": len(overlap),
                        "overlap_pct": round(score * 100, 1),
                        "sample_values": list(overlap)[:5],
                    })

    connections.sort(key=lambda c: c["overlap_count"], reverse=True)
    return {"connections": connections[:15]}


def join_datasets(dataset_a: str, col_a: str, dataset_b: str, col_b: str,
                  how: str = "inner", name: str | None = None) -> dict:
    """Join two datasets and register the result."""
    if dataset_a not in _datasets or dataset_b not in _datasets:
        return {"error": f"Dataset not found. Available: {list(_datasets.keys())}"}

    df_a = _datasets[dataset_a]
    df_b = _datasets[dataset_b]

    try:
        result = df_a.merge(df_b, left_on=col_a, right_on=col_b, how=how, suffixes=("_a", "_b"))
        result_name = name or f"{dataset_a}+{dataset_b}"
        register_dataset(result_name, result)
        set_active(result_name)
        return {
            "name": result_name,
            "rows": len(result),
            "columns": list(result.columns),
            "message": f"Joined {dataset_a}.{col_a} <-> {dataset_b}.{col_b}: {len(result)} rows",
        }
    except Exception as e:
        return {"error": str(e)}


# ── Dynamic link creation ────────────────────────────────────────────


def create_links(code: str, label: str = "custom") -> dict:
    """Create links between nodes using Pandas code.

    The code must set 'links' to a DataFrame with 'source' and 'target' columns
    containing values from the active dataset's index column.
    """
    global _dynamic_links

    df = get_df().copy()
    namespace = {"df": df, "pd": pd, "np": np}

    try:
        exec(compile(code, "<create_links>", "exec"), namespace)

        if "links" not in namespace:
            return {"error": "Code must set a 'links' variable with 'source' and 'target' columns"}

        new_links = namespace["links"]
        if not isinstance(new_links, pd.DataFrame):
            return {"error": "'links' must be a DataFrame"}

        if "source" not in new_links.columns or "target" not in new_links.columns:
            return {"error": "'links' DataFrame must have 'source' and 'target' columns"}

        records = new_links[["source", "target"]].head(10000).to_dict(orient="records")
        for r in records:
            r["_label"] = label

        _dynamic_links.extend(records)

        return {
            "created": len(records),
            "total_dynamic_links": len(_dynamic_links),
            "label": label,
            "sample": records[:5],
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def get_dynamic_links() -> list[dict]:
    """Get all AI-created dynamic links."""
    return _dynamic_links


def clear_dynamic_links() -> dict:
    """Clear all dynamic links."""
    global _dynamic_links
    count = len(_dynamic_links)
    _dynamic_links = []
    return {"cleared": count}


# ── Computed columns ─────────────────────────────────────────────────


def add_column(name: str, code: str) -> dict:
    """Add a computed column to the active dataset.

    The code has access to 'df' and must set 'result' to a Series or array.
    """
    df = get_df()
    namespace = {"df": df, "pd": pd, "np": np}

    try:
        exec(compile(code, "<add_column>", "exec"), namespace)

        if "result" not in namespace:
            return {"error": "Code must set 'result' to a Series or array"}

        col_data = namespace["result"]
        df[name] = col_data

        sample = df[name].dropna().head(5).tolist()
        return {
            "column": name,
            "dtype": str(df[name].dtype),
            "unique_values": int(df[name].nunique()),
            "sample": [str(v) for v in sample],
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ── Snapshots ────────────────────────────────────────────────────────


def save_snapshot(label: str, graph_commands: list[dict] | None = None,
                  description: str = "") -> dict:
    """Save a snapshot of the current investigation state."""
    snap = {
        "id": len(_snapshots),
        "label": label,
        "description": description,
        "timestamp": datetime.now().isoformat(),
        "dataset": _active_dataset,
        "graph_commands": graph_commands or [],
        "columns": list(get_df().columns),
    }
    _snapshots.append(snap)
    return {"saved": snap["id"], "label": label, "total": len(_snapshots)}


def get_snapshots() -> dict:
    """Get all investigation snapshots."""
    return {
        "snapshots": [
            {"id": s["id"], "label": s["label"], "timestamp": s["timestamp"],
             "description": s["description"]}
            for s in _snapshots
        ],
        "count": len(_snapshots),
    }


def get_snapshot(snap_id: int) -> dict:
    """Get a specific snapshot to replay."""
    if snap_id < 0 or snap_id >= len(_snapshots):
        return {"error": f"Snapshot {snap_id} not found"}
    return _snapshots[snap_id]


# ── Existing tools (generic versions) ────────────────────────────────


def get_node_indices(ids: list[str]) -> list[int]:
    df = get_df()
    id_col = df.columns[0]
    matched = df[df[id_col].astype(str).isin([str(i) for i in ids])]
    return matched["index"].tolist() if "index" in matched.columns else []


def get_node_indices_by_filter(filter_expr: str) -> list[int]:
    df = get_df()
    try:
        matched = df.query(filter_expr)
        return matched["index"].tolist() if "index" in matched.columns else []
    except Exception:
        return []


def query_dataframe(operation: str, column: str | None = None,
                    filter_expr: str | None = None, group_by: str | None = None,
                    sort_by: str | None = None, ascending: bool = False,
                    limit: int = 20, dataset: str | None = None) -> dict:
    """Flexible query on any dataset."""
    df = get_df(dataset)

    try:
        if filter_expr:
            df = df.query(filter_expr)

        if operation == "filter":
            cols = [c for c in df.columns if c != "index"][:8]
            result = df.head(limit)[cols]
            return {"count": len(df), "rows": result.to_dict(orient="records")}

        elif operation == "group_count":
            if not group_by or group_by not in df.columns:
                return {"error": f"Invalid group_by column: {group_by}. Available: {list(df.columns)}"}
            result = df.groupby(group_by).size().sort_values(ascending=ascending).head(limit)
            return {"groups": result.to_dict()}

        elif operation == "group_sum":
            if not group_by or not column:
                return {"error": "group_by and column required"}
            result = df.groupby(group_by)[column].sum().sort_values(ascending=ascending).head(limit)
            return {"groups": _safe_serialize(result.to_dict())}

        elif operation == "value_counts":
            col = column or df.columns[0]
            result = df[col].value_counts().head(limit)
            return {"counts": result.to_dict()}

        elif operation == "describe":
            col = column or df.select_dtypes(include="number").columns[0]
            desc = df[col].describe().to_dict()
            return {"stats": {k: round(v, 2) if isinstance(v, float) else v for k, v in desc.items()}}

        else:
            return {"error": f"Unknown operation: {operation}"}
    except Exception as e:
        return {"error": str(e)}


def run_pandas_code(code: str) -> dict:
    """Execute arbitrary Pandas code. Access: df (active), datasets (dict of all), pd, np."""
    df = get_df().copy()

    namespace = {
        "df": df,
        "datasets": {k: v.copy() for k, v in _datasets.items()},
        "pd": pd,
        "np": np,
        "_human_size": _human_size,
    }

    try:
        exec(compile(code, "<analysis>", "exec"), namespace)

        if "result" in namespace:
            r = namespace["result"]
            if isinstance(r, pd.DataFrame):
                r = r.copy()
                r.index = r.index.astype(str)
                r.columns = r.columns.astype(str)
                return {"data": r.head(50).to_dict(orient="records"), "rows": len(r)}
            elif isinstance(r, pd.Series):
                s = r.head(50)
                s.index = s.index.astype(str)
                return {"data": s.to_dict(), "length": len(r)}
            elif isinstance(r, (dict, list, int, float, str, bool)):
                return {"data": _safe_serialize(r)}
            else:
                return {"data": str(r)}
        return {"data": "Code executed (no 'result' variable set)"}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ── Plugin system ────────────────────────────────────────────────────

_plugins: dict[str, dict] = {}
PLUGINS_PATH = Path(__file__).parent / "plugins.json"


def _load_plugins():
    global _plugins
    if PLUGINS_PATH.exists():
        with open(PLUGINS_PATH) as f:
            _plugins = json.load(f)


def _save_plugins():
    with open(PLUGINS_PATH, "w") as f:
        json.dump(_plugins, f, indent=2)


def save_plugin(name: str, description: str, code: str) -> dict:
    _plugins[name] = {"description": description, "code": code}
    _save_plugins()
    return {"saved": name, "total_plugins": len(_plugins)}


def run_plugin(name: str) -> dict:
    if name not in _plugins:
        return {"error": f"Plugin '{name}' not found. Available: {list(_plugins.keys())}"}
    return run_pandas_code(_plugins[name]["code"])


def list_plugins() -> dict:
    return {"plugins": {k: v["description"] for k, v in _plugins.items()}, "count": len(_plugins)}


_load_plugins()


# ── Helpers ──────────────────────────────────────────────────────────


def _safe_serialize(obj):
    if isinstance(obj, dict):
        return {str(k): _safe_serialize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_safe_serialize(v) for v in obj]
    elif isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    else:
        return str(obj)


def _human_size(b: float) -> str:
    if b == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB"]
    i = 0
    while b >= 1024 and i < len(units) - 1:
        b /= 1024
        i += 1
    return f"{b:.1f} {units[i]}" if i > 0 else f"{int(b)} B"
