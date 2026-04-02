"""FastAPI backend for Cosmo Filesystem Analytics."""

import io
import json
import os
import requests
import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import analyzer

# ── Config ───────────────────────────────────────────────────────────

OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL_ANALYST = "anthropic/claude-sonnet-4.6"
MODEL_COMPANION = "google/gemini-3.1-pro-preview"

# ── App ──────────────────────────────────────────────────────────────

app = FastAPI(title="Cosmo Analytics")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5199", "http://localhost:5173", "http://127.0.0.1:5199"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Tool definitions for OpenRouter ──────────────────────────────────

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_dataframe",
            "description": "Flexible query on the active dataset. Can filter, group, count, sum, or describe columns. Column names are provided in the system prompt.",
            "parameters": {
                "type": "object",
                "properties": {
                    "operation": {
                        "type": "string",
                        "enum": ["filter", "group_count", "group_sum", "value_counts", "describe"],
                        "description": "Type of operation",
                    },
                    "column": {"type": "string", "description": "Column to operate on (for group_sum, value_counts, describe)"},
                    "filter_expr": {"type": "string", "description": "Pandas query expression, e.g. \"fileSize > 1000000 and category == 'code'\""},
                    "group_by": {"type": "string", "description": "Column to group by"},
                    "sort_by": {"type": "string", "description": "Column to sort by"},
                    "ascending": {"type": "boolean", "default": False},
                    "limit": {"type": "integer", "default": 20},
                },
                "required": ["operation"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "control_graph",
            "description": "Control the graph visualization. You can zoom, recolor, resize, select, and navigate. Use this AFTER analyzing data to visually show insights. You SHOULD use this on every response to make the graph reflect your analysis. Actions: 'zoom_to' (zoom camera to specific nodes), 'recolor' (change node colors by a column/filter), 'resize' (change node sizes), 'select' (highlight nodes), 'reset' (restore defaults), 'filter_view' (show only matching nodes, dim others).",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["zoom_to", "recolor", "resize", "select", "reset", "filter_view"],
                        "description": "Graph action to perform",
                    },
                    "node_ids": {"type": "array", "items": {"type": "string"}, "description": "Node ID paths (for zoom_to, select)"},
                    "filter_expr": {"type": "string", "description": "Pandas query to match nodes (for zoom_to, select, filter_view, recolor, resize)"},
                    "color_by": {"type": "string", "description": "Column name to color by (for recolor). Use 'category', 'extension', 'project', 'depth', or 'color' (default)."},
                    "color_map": {"type": "object", "description": "Map of column values to hex colors (for recolor). E.g. {'code':'#ff0000','data':'#00ff00'}"},
                    "size_by": {"type": "string", "description": "Column name to size by (for resize). Use 'fileSize', 'depth', 'size' (default)."},
                    "label": {"type": "string", "description": "Short label describing what the view is showing (shown on the graph overlay)"},
                },
                "required": ["action"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_pandas_code",
            "description": "Execute arbitrary Pandas code against the filesystem DataFrame. The code has access to 'df' (the DataFrame with columns: id, label, color, category, size, fileSize, depth, isDir, index, extension, project) and 'pd' (pandas). Set a 'result' variable to return data. Use this for any analysis that doesn't fit the other tools.",
            "parameters": {
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "Python/Pandas code to execute. Must set 'result' variable. Example: result = df[df['category']=='code'].groupby('extension')['fileSize'].sum().sort_values(ascending=False).head(10)",
                    },
                },
                "required": ["code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "save_plugin",
            "description": "Save a reusable analysis as a named plugin. Use this when you create a useful analysis that the user might want to run again later.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Short plugin name (e.g. 'dead_code_finder')"},
                    "description": {"type": "string", "description": "What this plugin analyzes"},
                    "code": {"type": "string", "description": "The Pandas code to save"},
                },
                "required": ["name", "description", "code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_plugin",
            "description": "Run a previously saved analysis plugin by name.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Plugin name to run"},
                },
                "required": ["name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_plugins",
            "description": "List all saved analysis plugins.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "find_connections",
            "description": "Find potential foreign key relationships between two loaded datasets. Detects shared column values that could link the datasets together.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset_a": {"type": "string", "description": "Name of the first dataset"},
                    "dataset_b": {"type": "string", "description": "Name of the second dataset"},
                },
                "required": ["dataset_a", "dataset_b"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "join_datasets",
            "description": "Join two datasets on matching columns and create a new combined dataset.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset_a": {"type": "string"},
                    "col_a": {"type": "string", "description": "Column in dataset_a to join on"},
                    "dataset_b": {"type": "string"},
                    "col_b": {"type": "string", "description": "Column in dataset_b to join on"},
                    "how": {"type": "string", "enum": ["inner", "left", "right", "outer"], "default": "inner"},
                    "name": {"type": "string", "description": "Name for the resulting dataset"},
                },
                "required": ["dataset_a", "col_a", "dataset_b", "col_b"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_datasets",
            "description": "List all loaded datasets in the workspace with their row counts and columns.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_links",
            "description": "Create graph edges between rows using Pandas code. Code must set 'links' to a DataFrame with 'source' and 'target' columns. Use this to visualize relationships like 'connect people in the same country' or 'link products to their categories'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "code": {"type": "string", "description": "Pandas code that sets 'links' DataFrame with 'source' and 'target' columns. Has access to 'df' (active dataset), 'pd', 'np'."},
                    "label": {"type": "string", "description": "Short label for these links (e.g. 'same_country')"},
                },
                "required": ["code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_column",
            "description": "Add a computed column to the active dataset. Code must set 'result' to a Series or array. Use this to create derived features like risk scores, bins, categories.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Name for the new column"},
                    "code": {"type": "string", "description": "Pandas code that sets 'result'. Has access to 'df', 'pd', 'np'. Example: result = pd.cut(df['age'], bins=[0,30,50,100], labels=['young','mid','senior'])"},
                },
                "required": ["name", "code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "save_snapshot",
            "description": "Save the current investigation state as a named snapshot. Use this after finding an important insight so the user can replay the investigation later.",
            "parameters": {
                "type": "object",
                "properties": {
                    "label": {"type": "string", "description": "Short snapshot label"},
                    "description": {"type": "string", "description": "What this snapshot shows"},
                },
                "required": ["label"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_chart",
            "description": "Create a chart that renders inline in the chat. Supports: bar, line, scatter, pie, doughnut, radar, polarArea, bubble. Use this to visualize analysis results — distributions, comparisons, trends, correlations. ALWAYS prefer charts over raw tables when possible.",
            "parameters": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["bar", "line", "scatter", "pie", "doughnut", "radar", "polarArea", "bubble", "horizontalBar"],
                        "description": "Chart type",
                    },
                    "title": {"type": "string", "description": "Chart title"},
                    "labels": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "X-axis labels (for bar, line, radar, pie, etc.)",
                    },
                    "datasets": {
                        "type": "array",
                        "description": "Array of dataset objects. Each has: label (string), data (number[]), and optional backgroundColor (string or string[]).",
                        "items": {
                            "type": "object",
                            "properties": {
                                "label": {"type": "string"},
                                "data": {"type": "array", "items": {"type": "number"}},
                                "backgroundColor": {
                                    "description": "Color(s) — single string or array of strings",
                                },
                            },
                            "required": ["label", "data"],
                        },
                    },
                    "x_label": {"type": "string", "description": "X-axis label"},
                    "y_label": {"type": "string", "description": "Y-axis label"},
                },
                "required": ["type", "datasets"],
            },
        },
    },
]

SYSTEM_PROMPT = """You are the AI analyst for Cosmo, a GPU-accelerated data visualization and analytics platform. You help users explore any dataset through natural language.

You have access to a multi-dataset workspace. Users can upload multiple CSV/JSON files. You can:
- Analyze any dataset with Pandas (run_pandas_code is your most powerful tool)
- Find connections between datasets (find_connections, join_datasets)
- Create graph edges dynamically (create_links) to visualize relationships
- Add computed columns (add_column) for derived features
- Save investigation snapshots (save_snapshot)
- Control the graph visualization (control_graph) to zoom, select, and navigate

Key principles:
1. Use tools to get real data — never guess
2. ALWAYS use create_chart to visualize results — bar charts for comparisons, pie for proportions, scatter for correlations, line for trends
3. Use control_graph to update the graph visualization — zoom, select, navigate
4. Create dynamic links (create_links) to reveal hidden relationships
5. Add computed columns (add_column) when you need derived features
6. Save snapshots at key insights
7. For complex analysis, use run_pandas_code (access: df, datasets dict, pd, np)
8. Prefer charts over tables whenever possible — they're more impactful
9. EXPLAIN your methodology: before showing results, briefly state what analysis you're running and why (e.g. "I'll group by X and aggregate Y to find...")
10. When you create a computed column or dynamic links, explain the logic and why it matters
- category: 'directory', 'code', 'web', 'data', 'docs', 'media', 'shell', 'other'
- fileSize: size in bytes (0 for directories)
- depth: nesting level from root
- extension: file extension (e.g. '.js', '.py')
- project: top-level directory name

When answering:
1. Use your tools to get actual data - never guess at file counts or sizes
2. ALWAYS use control_graph to update the visualization after analyzing — zoom into relevant areas, recolor by your findings, resize by metrics. Don't just highlight dots — transform the view to tell a story.
3. Be concise and specific with numbers
4. Format responses with markdown for readability
5. For complex analysis, use run_pandas_code to write custom Pandas code
6. If you create a useful analysis, save it as a plugin with save_plugin so it can be reused
7. When comparing projects, use get_directory_stats on each one

The run_pandas_code tool is your most powerful tool. You can write any Pandas code. The DataFrame 'df' has all filesystem nodes. Set 'result' to return data. Examples:
- result = df.groupby('project').agg(files=('id','count'), size=('fileSize','sum')).sort_values('size', ascending=False).head(20)
- result = df[df['extension']=='.py'].groupby('project')['fileSize'].sum().sort_values(ascending=False)
- result = df[~df['isDir']].groupby('project')['category'].value_counts().unstack(fill_value=0)"""

# ── Endpoints ────────────────────────────────────────────────────────


COMPANION_THEMES = {
    "devils_advocate": "Challenge every conclusion. Find counterexamples. Poke holes in the analysis. Ask 'but what about...' questions.",
    "anomaly_hunter": "Focus exclusively on outliers, anomalies, and unexpected patterns. Find the weird stuff others miss.",
    "optimizer": "Focus on actionable improvements: cost savings, cleanup opportunities, efficiency gains. Be ruthlessly practical.",
    "connector": "Look for hidden relationships between data points. Use create_links and find_connections. Build the graph.",
    "storyteller": "Weave the data into a narrative. What's the story this data tells? Who are the characters?",
    "default": "Dig deeper, challenge, or extend the analysis. Find something they missed.",
}

COMPANION_SYSTEM = """You are a Companion Researcher working alongside a primary Data Analyst. You both have the same powerful analytics tools.

Your investigation theme: {theme}

Core approach:
- Read the analyst's findings and respond according to your theme
- Run your OWN analyses — don't just comment, USE THE TOOLS
- Create dynamic links (create_links) to reveal relationships
- Add computed columns (add_column) when you need new features
- Use control_graph to navigate and zoom into what you find
- Be concise but insightful"""


class ChatRequest(BaseModel):
    message: str
    history: list[dict] = []
    role: str = "analyst"
    companion_theme: str = "default"


class GraphCommand(BaseModel):
    action: str = "select"
    indices: list[int] = []
    color_by: str | None = None
    color_map: dict | None = None
    size_by: str | None = None
    label: str | None = None


class ToolCall(BaseModel):
    name: str
    args_summary: str = ""
    result_summary: str = ""


class ChartSpec(BaseModel):
    type: str
    title: str = ""
    labels: list[str] = []
    datasets: list[dict] = []
    x_label: str = ""
    y_label: str = ""


class ChatResponse(BaseModel):
    response: str
    graph_commands: list[GraphCommand] = []
    tool_calls: list[ToolCall] = []
    charts: list[ChartSpec] = []
    role: str = "analyst"


@app.on_event("startup")
def startup():
    print("Cosmo backend ready — no dataset loaded. Upload a file to begin.")


def _run_llm_with_tools(messages: list[dict], model: str) -> tuple[str, list[dict], list[dict], list[dict]]:
    """Run an LLM with tool-use loop. Returns (response_text, graph_commands, tool_log, charts)."""
    graph_commands: list[dict] = []
    tool_log: list[dict] = []
    charts: list[dict] = []

    for _ in range(15):
        payload = {
            "model": model,
            "messages": messages,
            "tools": TOOLS,
            "temperature": 0.3,
            "max_tokens": 4096,
        }
        headers = {
            "Authorization": f"Bearer {OPENROUTER_KEY}",
            "Content-Type": "application/json",
        }

        resp = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        choice = data["choices"][0]
        msg = choice["message"]

        tool_calls = msg.get("tool_calls")
        if tool_calls:
            messages.append(msg)
            for tc in tool_calls:
                fn_name = tc["function"]["name"]
                fn_args = json.loads(tc["function"]["arguments"]) if tc["function"]["arguments"] else {}
                print(f"  [{model.split('/')[-1]}] Tool: {fn_name}({fn_args})")
                result = _execute_tool(fn_name, fn_args)
                if fn_name == "control_graph":
                    graph_commands.append(result)
                elif fn_name == "create_chart":
                    charts.append(result)

                # Log the tool call
                args_summary = ""
                if fn_name == "run_pandas_code":
                    code = fn_args.get("code", "")
                    args_summary = code[:120] + ("..." if len(code) > 120 else "")
                elif fn_name == "query_dataframe":
                    args_summary = f"{fn_args.get('operation','')} | {fn_args.get('filter_expr','') or fn_args.get('group_by','')}"
                elif fn_name == "get_directory_stats":
                    args_summary = fn_args.get("path", "")
                elif fn_name == "control_graph":
                    args_summary = f"{fn_args.get('action','')} | {fn_args.get('label','')}"
                else:
                    args_summary = json.dumps(fn_args, default=str)[:100]

                result_summary = ""
                if isinstance(result, dict):
                    if "error" in result:
                        result_summary = f"Error: {result['error'][:80]}"
                    elif "data" in result:
                        d = result["data"]
                        if isinstance(d, list):
                            result_summary = f"{len(d)} rows"
                        elif isinstance(d, dict):
                            result_summary = f"{len(d)} keys"
                        else:
                            result_summary = str(d)[:80]
                    elif "count" in result:
                        result_summary = f"{result['count']} results"

                tool_log.append({
                    "name": fn_name,
                    "args_summary": args_summary,
                    "result_summary": result_summary,
                })

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": json.dumps(result, default=str),
                })
            continue

        return msg.get("content", ""), graph_commands, tool_log, charts

    return "Analysis timed out.", [], tool_log, charts


def _build_system_prompt(base: str) -> str:
    """Inject current dataset schema into the system prompt."""
    df = analyzer.get_df()
    col_info = []
    for c in df.columns:
        nunique = int(df[c].nunique())
        col_info.append(f"- {c} ({df[c].dtype}, {nunique} unique)")
    schema_block = f"\n\nCurrent dataset: '{analyzer._dataset_name}' with {len(df)} rows.\nColumns:\n" + "\n".join(col_info)
    return base + schema_block


@app.post("/api/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    """Chat with either the analyst or companion."""
    is_companion = req.role == "companion"
    model = MODEL_COMPANION if is_companion else MODEL_ANALYST
    if is_companion:
        theme_text = COMPANION_THEMES.get(req.companion_theme, COMPANION_THEMES["default"])
        base = COMPANION_SYSTEM.format(theme=theme_text)
    else:
        base = SYSTEM_PROMPT
    system = _build_system_prompt(base)

    # Add dataset list to system prompt
    ds_list = analyzer.list_datasets()
    if len(ds_list["datasets"]) > 1:
        system += f"\n\nLoaded datasets: {', '.join(ds_list['datasets'].keys())}"

    messages = [{"role": "system", "content": system}]
    for msg in req.history[-12:]:
        messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": req.message})

    try:
        content, graph_cmds, tool_log, chart_specs = _run_llm_with_tools(messages, model)
        commands = [GraphCommand(**c) for c in graph_cmds]
        calls = [ToolCall(**t) for t in tool_log]
        chart_objs = [ChartSpec(**c) for c in chart_specs]
        return ChatResponse(response=content, graph_commands=commands, tool_calls=calls, charts=chart_objs, role=req.role)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"LLM error: {e}")


def _execute_tool(name: str, args: dict) -> dict:
    """Dispatch tool call to analyzer functions."""
    if name == "query_dataframe":
        return analyzer.query_dataframe(**args)
    elif name == "control_graph":
        indices = []
        if "node_ids" in args and args.get("node_ids"):
            indices = analyzer.get_node_indices(args["node_ids"])
        elif "filter_expr" in args and args.get("filter_expr"):
            indices = analyzer.get_node_indices_by_filter(args["filter_expr"])
        return {
            "action": args.get("action", "select"),
            "indices": indices,
            "count": len(indices),
            "color_by": args.get("color_by"),
            "color_map": args.get("color_map"),
            "size_by": args.get("size_by"),
            "label": args.get("label"),
        }
    elif name == "run_pandas_code":
        return analyzer.run_pandas_code(args.get("code", ""))
    elif name == "save_plugin":
        return analyzer.save_plugin(args["name"], args["description"], args["code"])
    elif name == "run_plugin":
        return analyzer.run_plugin(args["name"])
    elif name == "list_plugins":
        return analyzer.list_plugins()
    elif name == "find_connections":
        return analyzer.find_connections(args["dataset_a"], args["dataset_b"])
    elif name == "join_datasets":
        return analyzer.join_datasets(**args)
    elif name == "list_datasets":
        return analyzer.list_datasets()
    elif name == "create_links":
        return analyzer.create_links(args.get("code", ""), args.get("label", "custom"))
    elif name == "add_column":
        return analyzer.add_column(args["name"], args["code"])
    elif name == "save_snapshot":
        return analyzer.save_snapshot(args.get("label", "snapshot"), description=args.get("description", ""))
    elif name == "create_chart":
        return {
            "type": args.get("type", "bar"),
            "title": args.get("title", ""),
            "labels": args.get("labels", []),
            "datasets": args.get("datasets", []),
            "x_label": args.get("x_label", ""),
            "y_label": args.get("y_label", ""),
        }
    else:
        return {"error": f"Unknown tool: {name}"}


@app.get("/api/stats")
async def stats():
    """Get basic stats for the current dataset."""
    df = analyzer.get_df()
    result = {
        "total_rows": len(df),
        "columns": list(df.columns),
        "dataset_name": analyzer._dataset_name,
    }
    if "isDir" in df.columns:
        files = df[~df["isDir"]]
        result["total_files"] = len(files)
        result["total_dirs"] = len(df[df["isDir"]])
        if "fileSize" in df.columns:
            result["total_size"] = analyzer._human_size(files["fileSize"].sum())
        if "category" in df.columns:
            result["categories"] = files["category"].value_counts().to_dict()
    return result


@app.post("/api/upload")
async def upload_dataset(file: UploadFile = File(...), append: bool = False):
    """Upload a CSV or JSON dataset. Use append=true to add to workspace without clearing."""
    content = await file.read()
    filename = file.filename or "upload"

    try:
        if not append:
            analyzer._datasets.clear()
            analyzer._dynamic_links.clear()

        if filename.endswith(".json"):
            raw = json.loads(content)
            if isinstance(raw, list):
                df = pd.DataFrame(raw)
            elif isinstance(raw, dict) and "points" in raw:
                # Cosmograph-style: {points: [...], links: [...]}
                df = pd.DataFrame(raw["points"])
                analyzer._df_links = pd.DataFrame(raw.get("links", []))
            else:
                df = pd.DataFrame([raw])
        else:
            try:
                df = pd.read_csv(io.BytesIO(content))
            except UnicodeDecodeError:
                df = pd.read_csv(io.BytesIO(content), encoding="latin-1")

        # Add index column if missing
        if "index" not in df.columns:
            df["index"] = range(len(df))

        # Register in workspace
        name = filename.rsplit(".", 1)[0]  # strip extension
        links_df = analyzer._df_links if hasattr(analyzer, '_df_links') and analyzer._df_links is not None else pd.DataFrame()
        analyzer.register_dataset(name, df, links_df)
        analyzer.set_active(name)

        # Auto-detect graph structure
        graph_config = _auto_detect_graph(df)

        print(f"Uploaded: {filename} — {len(df)} rows, {len(df.columns)} cols")

        return {
            "success": True,
            "filename": filename,
            "rows": len(df),
            "columns": list(df.columns),
            "dtypes": {c: str(df[c].dtype) for c in df.columns},
            "sample": df.head(3).to_dict(orient="records"),
            "graph_config": graph_config,
        }
    except Exception as e:
        print(f"Upload error: {e}")
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {e}")


def _auto_detect_graph(df: pd.DataFrame) -> dict:
    """Auto-detect which columns to use for graph visualization."""
    cols = set(df.columns)
    config = {}

    # ID column — must have mostly unique values
    for candidate in ["id", "ID", "Id", "node_id", "person_id", "track_id", "user_id", "item_id"]:
        if candidate in cols:
            config["pointIdBy"] = candidate
            break
    if "pointIdBy" not in config:
        # Find first column with high cardinality (>80% unique)
        for c in df.columns:
            if df[c].nunique() > len(df) * 0.8:
                config["pointIdBy"] = c
                break
    if "pointIdBy" not in config:
        # Fall back to index
        config["pointIdBy"] = "index"

    # Label — prefer human-readable name columns
    for candidate in ["label", "name", "Name", "title", "Title", "track_name", "artist", "artist_name", "description"]:
        if candidate in cols and candidate != config.get("pointIdBy"):
            config["pointLabelBy"] = candidate
            break
    # Fall back to first string column
    if "pointLabelBy" not in config:
        for c in df.select_dtypes(include="object").columns:
            if c != config.get("pointIdBy") and df[c].nunique() > 1:
                config["pointLabelBy"] = c
                break
    if "pointLabelBy" not in config:
        config["pointLabelBy"] = config["pointIdBy"]

    # Color
    for candidate in ["color", "category", "type", "group", "cluster", "class"]:
        if candidate in cols:
            config["pointColorBy"] = candidate
            break

    # Size
    for candidate in ["size", "value", "weight", "amount", "count", "fileSize", "score"]:
        if candidate in cols:
            config["pointSizeBy"] = candidate
            break

    # Links: check if separate link data exists, or if df has source+target
    for src_candidate in ["source", "Source", "from", "From", "src"]:
        for tgt_candidate in ["target", "Target", "to", "To", "dst"]:
            if src_candidate in cols and tgt_candidate in cols:
                config["linkSourceBy"] = src_candidate
                config["linkTargetBy"] = tgt_candidate
                break

    return config


@app.get("/api/graph-data")
async def graph_data():
    """Get all datasets combined as graph-ready points + links."""
    try:
        datasets = analyzer._datasets
        if not datasets:
            return {"points": [], "links": []}

        # Combine all datasets with a _source column
        frames = []
        for name, df in datasets.items():
            chunk = df.copy()
            chunk["_source"] = name
            # Limit columns: keep first 7 + index + _source
            keep = list(df.columns)[:7]
            if "index" in df.columns and "index" not in keep:
                keep.append("index")
            keep.append("_source")
            frames.append(chunk[keep])

        combined = pd.concat(frames, ignore_index=True)
        combined["index"] = range(len(combined))

        # Cap at 50K rows
        if len(combined) > 50000:
            combined = pd.concat([combined.iloc[:100], combined.sample(n=49900, random_state=42)])
            combined = combined.reset_index(drop=True)
            combined["index"] = range(len(combined))

        # Replace NaN/inf
        combined = combined.fillna("")
        for c in combined.select_dtypes(include="float").columns:
            combined[c] = combined[c].replace([float("inf"), float("-inf")], 0)

        points = combined.to_dict(orient="records")

        links = []
        if analyzer._df_links is not None and not analyzer._df_links.empty:
            links = analyzer._df_links.to_dict(orient="records")

        # When multiple datasets, use _source for coloring
        graph_config = _auto_detect_graph(combined)
        if len(datasets) > 1:
            graph_config["pointColorBy"] = "_source"

        return {"points": points, "links": links, "graph_config": graph_config}
    except Exception as e:
        print(f"graph-data error: {e}")
        import traceback; traceback.print_exc()
        return {"points": [], "links": []}


@app.get("/api/schema")
async def schema():
    """Get the current dataset schema for LLM system prompt."""
    df = analyzer.get_df()
    col_info = []
    for c in df.columns:
        dtype = str(df[c].dtype)
        nunique = int(df[c].nunique())
        sample = df[c].dropna().head(3).tolist()
        col_info.append(f"- {c} ({dtype}, {nunique} unique): e.g. {sample}")
    return {
        "dataset": analyzer._dataset_name,
        "rows": len(df),
        "columns": "\n".join(col_info),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, h11_max_incomplete_event_size=500_000_000)
