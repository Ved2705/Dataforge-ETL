"""
api/ai.py — AI-Powered Features
- Auto column mapping (detect meaning of each column)
- Natural language queries on datasets
- PII detection
- Smart suggestions
Priority: Groq (free, fast) -> Gemini -> Claude -> Pollinations (free fallback)
"""

from __future__ import annotations
import os, json, re
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user
from backend.models import db, Dataset

ai_bp = Blueprint("ai", __name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY", "")
GROQ_API_KEY      = os.getenv("GROQ_API_KEY", "")


def _find_dataset_by_ds_id(ds_id, user_id):
    ds = Dataset.query.filter_by(user_id=user_id).filter(
        Dataset.storage_path.ilike(f"%{ds_id}%")
    ).first()
    return ds


def _get_profile_data(ds):
    pj = ds.profile_json or {}
    if "profile" in pj and isinstance(pj["profile"], dict):
        return pj["profile"]
    return pj


# ═════════════════════════════════════════════════════
# AI PROVIDERS
# ═════════════════════════════════════════════════════

def call_groq(prompt: str, system: str = "", max_tokens: int = 4096) -> str:
    """Call Groq API — free, fast, 14400 req/day, never expires."""
    import urllib.request
    if not GROQ_API_KEY:
        raise ValueError("GROQ_API_KEY not set")

    body = json.dumps({
        "model": "llama-3.1-8b-instant",
        "messages": [
            {
                "role": "system",
                "content": system or "You are a data engineering expert. Always respond with valid JSON only, no markdown, no explanation."
            },
            {"role": "user", "content": prompt}
        ],
        "max_tokens": max_tokens,
        "temperature": 0.1
    }).encode()

    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {GROQ_API_KEY}"
        }
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.load(resp)

    text = data.get("choices", [{}])[0].get("message", {}).get("content")
    if not text:
        raise ValueError(f"Groq returned empty response: {data}")
    return text


def call_gemini(prompt: str, system: str = "", max_tokens: int = 2048) -> str:
    """Call Google Gemini API."""
    import urllib.request
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY not set")

    full_prompt = f"{system}\n\n{prompt}" if system else prompt

    body = json.dumps({
        "contents": [{"parts": [{"text": full_prompt}]}],
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": 0.1,
        }
    }).encode()

    req = urllib.request.Request(
        f"https://generativelanguage.googleapis.com/v1/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.load(resp)

    candidates = data.get("candidates", [])
    if not candidates:
        raise ValueError(f"Gemini no candidates: {data}")
    parts = candidates[0].get("content", {}).get("parts", [])
    if not parts:
        raise ValueError(f"Gemini empty parts: {data}")
    text = parts[0].get("text")
    if not text:
        raise ValueError(f"Gemini null text: {data}")
    return text


def call_claude(prompt: str, system: str = "", max_tokens: int = 1024) -> str:
    """Call Anthropic Claude API."""
    import urllib.request
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY not set")

    body = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": max_tokens,
        "system": system or "You are a data engineering expert. Always respond with valid JSON only, no markdown.",
        "messages": [{"role": "user", "content": prompt}]
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.load(resp)
    return data["content"][0]["text"]


def call_pollinations(prompt: str, system: str = "", max_tokens: int = 1024) -> str:
    """Call Pollinations AI — free, no key required."""
    import urllib.request
    body = json.dumps({
        "messages": [
            {"role": "system", "content": system or "You are a data engineering expert. Always respond with valid JSON only, no markdown."},
            {"role": "user", "content": prompt}
        ],
        "model": "openai",
        "jsonMode": True
    }).encode()

    req = urllib.request.Request(
        "https://text.pollinations.ai/",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        text = resp.read().decode("utf-8")
    if not text:
        raise ValueError("Pollinations returned empty response")
    return text


def call_ai(prompt: str, system: str = "", max_tokens: int = 2048) -> str:
    """
    Call best available AI provider.
    Priority: Groq -> Gemini -> Claude -> Pollinations
    """
    errors = []

    if GROQ_API_KEY:
        try:
            return call_groq(prompt, system, max_tokens)
        except Exception as e:
            errors.append(f"Groq: {e}")

    if GEMINI_API_KEY:
        try:
            return call_gemini(prompt, system, max_tokens)
        except Exception as e:
            errors.append(f"Gemini: {e}")

    if ANTHROPIC_API_KEY:
        try:
            return call_claude(prompt, system, max_tokens)
        except Exception as e:
            errors.append(f"Claude: {e}")

    try:
        return call_pollinations(prompt, system, max_tokens)
    except Exception as e:
        errors.append(f"Pollinations: {e}")

    raise ValueError(f"All AI providers failed — {'; '.join(errors)}")


def get_active_engine() -> str:
    if GROQ_API_KEY:      return "groq"
    if GEMINI_API_KEY:    return "gemini"
    if ANTHROPIC_API_KEY: return "claude"
    return "local"


def safe_json(text: str) -> dict:
    """Safely extract JSON from AI response."""
    if not text:
        raise ValueError("AI returned empty response")
    text = text.strip()
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*",     "", text)
    text = text.strip().rstrip("`").strip()
    return json.loads(text)


# ═════════════════════════════════════════════════════
# SMART LOCAL NL-TO-PANDAS ENGINE (fallback)
# ═════════════════════════════════════════════════════

def _smart_nl_to_pandas(question: str, df) -> dict | None:
    import pandas as pd
    q        = question.lower().strip().rstrip("?.,!")
    col_map  = {c.lower(): c for c in df.columns}
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

    def find_col(text):
        text = text.lower()
        for lc, real in sorted(col_map.items(), key=lambda x: -len(x[0])):
            if lc in text or lc.replace("_", " ") in text:
                return real
        return None

    def find_two_cols(text):
        found = []
        t = text.lower()
        for lc, real in sorted(col_map.items(), key=lambda x: -len(x[0])):
            if lc in t or lc.replace("_", " ") in t:
                found.append(real)
                t = t.replace(lc, "").replace(lc.replace("_", " "), "")
        return found[:2] if len(found) >= 2 else (found + [None, None])[:2]

    def find_number(text):
        nums = re.findall(r'\d+\.?\d*', text)
        return float(nums[0]) if nums else None

    def find_group_col(text):
        by_match = re.search(r'(?:by|per|for each|group\s*by)\s+(\w[\w\s]*)', text)
        return find_col(by_match.group(1)) if by_match else None

    if any(k in q for k in ["describe","summary","statistics","stats","overview"]):
        col = find_col(q)
        if col and col in num_cols:
            return {"pandas_code": f"df['{col}'].describe()", "explanation": f"Summary statistics for {col}", "result_type": "table"}
        return {"pandas_code": "df.describe()", "explanation": "Summary statistics for all numeric columns", "result_type": "table"}

    if any(k in q for k in ["mean","average","avg"]):
        col = find_col(q); group_col = find_group_col(q)
        if col and group_col:
            return {"pandas_code": f"df.groupby('{group_col}')['{col}'].mean()", "explanation": f"Average {col} by {group_col}", "result_type": "table"}
        if col:
            return {"pandas_code": f"df['{col}'].mean()", "explanation": f"Mean of {col}", "result_type": "value"}

    if "median" in q:
        col = find_col(q)
        if col:
            return {"pandas_code": f"df['{col}'].median()", "explanation": f"Median of {col}", "result_type": "value"}

    if any(k in q for k in ["sum","total"]):
        col = find_col(q); group_col = find_group_col(q)
        if col and group_col:
            return {"pandas_code": f"df.groupby('{group_col}')['{col}'].sum()", "explanation": f"Total {col} by {group_col}", "result_type": "table"}
        if col:
            return {"pandas_code": f"df['{col}'].sum()", "explanation": f"Sum of {col}", "result_type": "value"}

    if any(k in q for k in ["min","minimum","lowest","smallest"]):
        col = find_col(q)
        if col:
            return {"pandas_code": f"df['{col}'].min()", "explanation": f"Minimum of {col}", "result_type": "value"}

    if any(k in q for k in ["max","maximum","highest","largest","biggest"]):
        col = find_col(q)
        if col:
            return {"pandas_code": f"df['{col}'].max()", "explanation": f"Maximum of {col}", "result_type": "value"}

    if any(k in q for k in ["count","how many","number of"]):
        col = find_col(q); group_col = find_group_col(q)
        if group_col:
            return {"pandas_code": f"df['{group_col}'].value_counts()", "explanation": f"Count by {group_col}", "result_type": "table"}
        if col:
            return {"pandas_code": f"df['{col}'].value_counts()", "explanation": f"Value counts for {col}", "result_type": "table"}
        return {"pandas_code": "len(df)", "explanation": "Total row count", "result_type": "value"}

    if any(k in q for k in ["unique","distinct"]):
        col = find_col(q)
        if col:
            return {"pandas_code": f"df['{col}'].unique()", "explanation": f"Unique values of {col}", "result_type": "list"}

    if any(k in q for k in ["top","first","head"]):
        n = find_number(q) or 5; col = find_col(q)
        if col and col in num_cols:
            return {"pandas_code": f"df.nlargest({int(n)}, '{col}')", "explanation": f"Top {int(n)} by {col}", "result_type": "table"}
        return {"pandas_code": f"df.head({int(n)})", "explanation": f"First {int(n)} rows", "result_type": "table"}

    if any(k in q for k in ["bottom","last","tail"]):
        n = find_number(q) or 5; col = find_col(q)
        if col and col in num_cols:
            return {"pandas_code": f"df.nsmallest({int(n)}, '{col}')", "explanation": f"Bottom {int(n)} by {col}", "result_type": "table"}
        return {"pandas_code": f"df.tail({int(n)})", "explanation": f"Last {int(n)} rows", "result_type": "table"}

    if "sort" in q:
        col = find_col(q); asc = "descending" not in q and "desc" not in q
        if col:
            return {"pandas_code": f"df.sort_values('{col}', ascending={asc})", "explanation": f"Sort by {col}", "result_type": "table"}

    if any(k in q for k in ["filter","where","above","below","greater","less","more than","over","under"]):
        col = find_col(q); n = find_number(q)
        if col and n is not None:
            if any(k in q for k in ["above","greater","more than","over","at least"]): sym = ">"
            elif any(k in q for k in ["below","less","under"]): sym = "<"
            else: sym = "=="
            return {"pandas_code": f"df[df['{col}'] {sym} {n}]", "explanation": f"Rows where {col} {sym} {n}", "result_type": "table"}

    if "correlation" in q or "corr" in q:
        c1, c2 = find_two_cols(q)
        if c1 and c2:
            return {"pandas_code": f"df['{c1}'].corr(df['{c2}'])", "explanation": f"Correlation between {c1} and {c2}", "result_type": "value"}
        return {"pandas_code": "df.corr(numeric_only=True)", "explanation": "Correlation matrix", "result_type": "table"}

    if any(k in q for k in ["missing","null","nan","empty"]):
        col = find_col(q)
        if col:
            return {"pandas_code": f"df['{col}'].isna().sum()", "explanation": f"Missing values in {col}", "result_type": "value"}
        return {"pandas_code": "df.isna().sum()", "explanation": "Missing values per column", "result_type": "table"}

    if any(k in q for k in ["shape","size","dimensions","how big"]):
        return {"pandas_code": "df.shape", "explanation": "Dataset dimensions", "result_type": "value"}

    if any(k in q for k in ["columns","fields","column names"]):
        return {"pandas_code": "df.columns.tolist()", "explanation": "All column names", "result_type": "list"}

    if any(k in q for k in ["group","grouped","per","by each","for each","rows by"]):
        col = find_col(q)
        if col:
            if col in num_cols:
                return {"pandas_code": f"df['{col}'].value_counts()", "explanation": f"Distribution of {col}", "result_type": "table"}
            return {"pandas_code": f"df.groupby('{col}').size().reset_index(name='count').sort_values('count', ascending=False)", "explanation": f"Row count grouped by {col}", "result_type": "table"}

    if any(k in q for k in ["show","display","list","print","get"]):
        col = find_col(q)
        if col:
            return {"pandas_code": f"df['{col}'].value_counts()", "explanation": f"Values in {col}", "result_type": "table"}

    return None


# ═════════════════════════════════════════════════════
# COLUMN ANALYSIS
# ═════════════════════════════════════════════════════

@ai_bp.post("/analyze-columns")
@login_required
def analyze_columns():
    body    = request.get_json(force=True)
    ds_id   = body.get("ds_id")
    ds      = _find_dataset_by_ds_id(ds_id, current_user.id)
    if not ds:
        return jsonify({"error": "Dataset not found"}), 404
    profile = _get_profile_data(ds)

    col_summaries = []
    for col, info in profile.items():
        if col == "__meta__": continue
        summary = {
            "name":        col,
            "dtype":       info.get("dtype"),
            "kind":        info.get("kind"),
            "sample":      info.get("sample", [])[:3],
            "unique":      info.get("unique"),
            "missing_pct": info.get("missing_pct"),
        }
        if info.get("kind") == "numeric":
            summary["min"]  = info.get("min")
            summary["max"]  = info.get("max")
            summary["mean"] = info.get("mean")
        col_summaries.append(summary)

    prompt = f"""Analyze these dataset columns and return a JSON object.

Columns:
{json.dumps(col_summaries, indent=2)}

Return ONLY a JSON object with this exact structure, no markdown, no explanation:
{{
  "columns": {{
    "<column_name>": {{
      "semantic_type": "<id/name/email/phone/address/date/salary/age/gender/department/location/score/category/description/url/currency/percentage/count/boolean/nationality/unknown>",
      "label": "<human friendly label>",
      "is_pii": <true or false>,
      "pii_type": "<name/email/phone/ssn/address/null>",
      "quality_issues": ["<issue1>"],
      "suggested_transforms": ["<transform1>"],
      "business_meaning": "<one sentence>"
    }}
  }},
  "dataset_summary": "<2 sentence description>",
  "recommended_pipeline": ["<step1>", "<step2>"],
  "join_candidates": ["<column>"],
  "pii_risk": "<low/medium/high>"
}}"""

    engine = get_active_engine()

    try:
        result = call_ai(prompt, max_tokens=4096)
        data   = safe_json(result)
        return jsonify({"success": True, "analysis": data, "engine": engine})
    except ValueError:
        return jsonify({
            "success": True,
            "analysis": _mock_column_analysis(profile),
            "mock":     True,
            "engine":   "local",
            "note":     "Set GROQ_API_KEY in .env for AI-powered analysis"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _mock_column_analysis(profile: dict) -> dict:
    type_map = {
        "id":          ["id", "uid", "uuid", "key"],
        "name":        ["name", "firstname", "lastname", "fullname"],
        "email":       ["email", "mail"],
        "phone":       ["phone", "mobile", "tel"],
        "date":        ["date", "time", "created", "updated", "hired", "birth"],
        "salary":      ["salary", "wage", "pay", "compensation", "income"],
        "age":         ["age", "years"],
        "department":  ["dept", "department", "division", "team"],
        "location":    ["city", "state", "country", "location", "region", "office"],
        "score":       ["score", "rating", "performance", "grade"],
        "gender":      ["gender", "sex"],
        "nationality": ["nationality", "nation", "citizenship"],
    }
    columns = {}
    for col, info in profile.items():
        if col == "__meta__": continue
        col_lower = col.lower()
        semantic  = "unknown"
        for stype, keywords in type_map.items():
            if any(k in col_lower for k in keywords):
                semantic = stype
                break
        is_pii = semantic in ("name", "email", "phone", "address")
        columns[col] = {
            "semantic_type":        semantic,
            "label":                col.replace("_", " ").title(),
            "is_pii":               is_pii,
            "pii_type":             semantic if is_pii else None,
            "quality_issues":       (["High missing rate"] if info.get("missing_pct", 0) > 20 else []),
            "suggested_transforms": _suggest_transforms(info),
            "business_meaning":     f"Column containing {semantic} data"
        }
    pii_count = sum(1 for c in columns.values() if c["is_pii"])
    return {
        "columns":              columns,
        "dataset_summary":      f"Dataset with {len(columns)} columns containing various business data.",
        "recommended_pipeline": ["snake_case_columns", "drop_duplicates", "fill_missing", "remove_outliers"],
        "join_candidates":      [c for c, v in columns.items() if v["semantic_type"] == "id"],
        "pii_risk":             "high" if pii_count > 2 else "medium" if pii_count > 0 else "low"
    }


def _suggest_transforms(info: dict) -> list:
    suggestions = []
    if info.get("missing_pct", 0) > 0:
        suggestions.append("fill_missing")
    if info.get("kind") == "numeric":
        suggestions.append("normalize")
        if info.get("skew", 0) and abs(info["skew"]) > 1:
            suggestions.append("remove_outliers")
    if info.get("kind") == "categorical":
        suggestions.append("standardize_categories")
    return suggestions


# ═════════════════════════════════════════════════════
# NATURAL LANGUAGE QUERY
# ═════════════════════════════════════════════════════

@ai_bp.post("/query")
@login_required
def nl_query():
    body     = request.get_json(force=True)
    ds_id    = body.get("ds_id")
    question = body.get("question", "")
    ds       = _find_dataset_by_ds_id(ds_id, current_user.id)
    if not ds:
        return jsonify({"error": "Dataset not found"}), 404
    if not ds.storage_path or not os.path.exists(ds.storage_path):
        return jsonify({"error": "Dataset file not found"}), 404

    import pandas as pd
    ext = ds.name.rsplit(".", 1)[-1].lower()
    try:
        if ext == "csv":             df = pd.read_csv(ds.storage_path, on_bad_lines="skip", low_memory=False)
        elif ext == "json":          df = pd.read_json(ds.storage_path)
        elif ext in ("xlsx", "xls"): df = pd.read_excel(ds.storage_path)
        else:                        df = pd.read_csv(ds.storage_path, on_bad_lines="skip", low_memory=False)
    except Exception as e:
        return jsonify({"error": f"Could not load dataset: {e}"}), 500

    schema_info = {col: str(df[col].dtype) for col in df.columns}
    sample_data = df.head(3).fillna("").to_dict(orient="records")
    engine      = get_active_engine()

    ai_prompt = f"""You are a pandas expert. Convert this natural language question to a pandas expression.

Dataset columns and types: {json.dumps(schema_info)}
Sample data (3 rows): {json.dumps(sample_data)}
Question: "{question}"

Return ONLY this JSON object, no markdown:
{{
  "pandas_code": "<single pandas expression using variable df>",
  "explanation": "<plain English explanation>",
  "result_type": "<table/value/list>"
}}

Rules:
- Variable must be named df
- Single expression only
- Must be safe to eval()
- Use actual column names from the dataset"""

    ai_used = False
    code = explanation = ""

    if GROQ_API_KEY or GEMINI_API_KEY or ANTHROPIC_API_KEY:
        try:
            result      = call_ai(ai_prompt, max_tokens=500)
            ai_data     = safe_json(result)
            code        = ai_data.get("pandas_code", "")
            explanation = ai_data.get("explanation", "")
            ai_used     = True
        except Exception:
            ai_used = False

    if not ai_used or not code:
        local_result = _smart_nl_to_pandas(question, df)
        if local_result:
            code        = local_result["pandas_code"]
            explanation = local_result["explanation"]
            engine      = "local"
        else:
            return jsonify({
                "success":     True,
                "question":    question,
                "code":        "df.describe()",
                "explanation": "Could not parse query — showing summary statistics.",
                "result":      df.describe().fillna("").to_dict(),
                "result_type": "table",
                "engine":      "local",
            })

    try:
        import numpy as np
        query_result = eval(code, {"__builtins__": {}}, {"df": df, "pd": pd, "np": np})

        if hasattr(query_result, "to_frame"):
            result_data = query_result.reset_index().fillna("").to_dict(orient="records")
            result_type = "table"
        elif hasattr(query_result, "to_dict"):
            result_data = query_result.reset_index().fillna("").to_dict(orient="records") \
                          if hasattr(query_result, "reset_index") else query_result.fillna("").to_dict()
            result_type = "table"
        elif hasattr(query_result, "tolist"):
            result_data = query_result.tolist()
            result_type = "list"
        elif isinstance(query_result, tuple):
            result_data = str(query_result)
            result_type = "value"
        else:
            val = query_result
            if hasattr(val, "item"): val = val.item()
            if isinstance(val, float): val = round(val, 4)
            result_data = str(val)
            result_type = "value"

        return jsonify({
            "success":     True,
            "question":    question,
            "code":        code,
            "explanation": explanation,
            "result":      result_data,
            "result_type": result_type,
            "engine":      engine,
        })

    except Exception as e:
        local_result = _smart_nl_to_pandas(question, df)
        if local_result:
            try:
                import numpy as np
                query_result = eval(local_result["pandas_code"],
                                    {"__builtins__": {}},
                                    {"df": df, "pd": pd, "np": np})
                if hasattr(query_result, "reset_index"):
                    result_data = query_result.reset_index().fillna("").to_dict(orient="records")
                elif hasattr(query_result, "tolist"):
                    result_data = query_result.tolist()
                else:
                    val = query_result
                    if hasattr(val, "item"): val = val.item()
                    result_data = str(round(val, 4) if isinstance(val, float) else val)
                return jsonify({
                    "success":     True,
                    "question":    question,
                    "code":        local_result["pandas_code"],
                    "explanation": local_result["explanation"],
                    "result":      result_data,
                    "result_type": "table",
                    "engine":      "local",
                })
            except Exception:
                pass
        return jsonify({"error": f"Query execution failed: {str(e)}"}), 500


# ═════════════════════════════════════════════════════
# SMART SUGGESTIONS
# ═════════════════════════════════════════════════════

@ai_bp.post("/suggest")
@login_required
def get_suggestions():
    body  = request.get_json(force=True)
    ds_id = body.get("ds_id")
    ds    = _find_dataset_by_ds_id(ds_id, current_user.id)
    if not ds:
        return jsonify({"error": "Dataset not found"}), 404
    profile = _get_profile_data(ds)

    issues, suggestions = [], []
    for col, info in profile.items():
        if col == "__meta__": continue
        if info.get("missing_pct", 0) > 30:
            issues.append(f"'{col}' has {info['missing_pct']}% missing values")
            suggestions.append({"type": "fill_missing", "column": col, "priority": "high"})
        if info.get("kind") == "numeric" and info.get("skew") and abs(info["skew"]) > 2:
            issues.append(f"'{col}' is heavily skewed (skew={info['skew']})")
            suggestions.append({"type": "remove_outliers", "column": col, "priority": "medium"})
        if info.get("duplicate_count", 0) > 0:
            issues.append(f"Dataset has duplicate rows")
            suggestions.append({"type": "drop_duplicates", "priority": "high"})

    return jsonify({
        "issues":      issues,
        "suggestions": suggestions,
        "auto_steps":  list({s["type"] for s in suggestions if s.get("priority") == "high"}),
    })


# ═════════════════════════════════════════════════════
# PII SCAN
# ═════════════════════════════════════════════════════

@ai_bp.post("/pii-scan")
@login_required
def pii_scan():
    body  = request.get_json(force=True)
    ds_id = body.get("ds_id")
    ds    = _find_dataset_by_ds_id(ds_id, current_user.id)
    if not ds:
        return jsonify({"error": "Dataset not found"}), 404

    import pandas as pd, re as regex
    try:
        df = pd.read_csv(ds.storage_path, on_bad_lines="skip", low_memory=False)
    except Exception:
        return jsonify({"error": "Could not load dataset"}), 500

    pii_patterns = {
        "email":       r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        "phone":       r"\+?[\d\s\-\(\)]{10,15}",
        "ssn":         r"\d{3}-\d{2}-\d{4}",
        "credit_card": r"\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}",
    }

    pii_found = []
    for col in df.columns:
        col_lower = col.lower()
        sample    = df[col].dropna().astype(str).head(50)
        if any(k in col_lower for k in ["name","email","phone","ssn","mobile","address","birth"]):
            pii_found.append({
                "column":         col,
                "pii_type":       "name_based",
                "confidence":     "high",
                "sample_count":   len(sample),
                "recommendation": "mask or remove before sharing"
            })
            continue
        for ptype, pattern in pii_patterns.items():
            matches = sample.str.match(pattern, na=False).sum()
            if matches > len(sample) * 0.3:
                pii_found.append({
                    "column":         col,
                    "pii_type":       ptype,
                    "confidence":     "high" if matches > len(sample) * 0.7 else "medium",
                    "sample_count":   int(matches),
                    "recommendation": "mask, hash, or remove before sharing"
                })
                break

    return jsonify({
        "ds_id":           ds_id,
        "pii_found":       pii_found,
        "pii_count":       len(pii_found),
        "risk_level":      "high" if len(pii_found) > 2 else "medium" if pii_found else "low",
        "columns_scanned": len(df.columns),
    })