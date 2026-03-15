"""
api/templates.py — Pre-built Pipeline Templates
Templates for common data engineering scenarios.
"""

from __future__ import annotations
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user

templates_bp = Blueprint("templates", __name__)

TEMPLATES = {
    "hr_data": {
        "id":          "hr_data",
        "name":        "HR Data Standardizer",
        "description": "Clean and standardize employee/HR datasets. Handles salary normalization, department standardization, and PII flagging.",
        "icon":        "👥",
        "category":    "Human Resources",
        "tags":        ["hr", "employees", "salary", "people"],
        "use_cases":   ["Employee records", "Payroll data", "Headcount reports"],
        "steps": [
            {"type": "snake_case_columns",  "config": {}},
            {"type": "drop_duplicates",     "config": {}},
            {"type": "drop_missing",        "config": {"threshold_pct": 70}},
            {"type": "fill_missing",        "config": {"strategy": "mode"}},
            {"type": "cast_types",          "config": {}},
            {"type": "normalize",           "config": {"columns": ["salary", "years_exp", "age"]}},
            {"type": "remove_outliers",     "config": {"method": "iqr", "threshold": 2.0}},
        ],
        "expected_columns": ["name", "salary", "department", "hire_date", "location"],
        "quality_checks":   ["salary > 0", "no null names", "valid hire dates"],
        "estimated_time":   "~15s",
    },
    "sales_data": {
        "id":          "sales_data",
        "name":        "Sales Report Cleaner",
        "description": "Process sales transactions, remove invalid orders, standardize product names and currencies.",
        "icon":        "💰",
        "category":    "Sales & Finance",
        "tags":        ["sales", "revenue", "transactions", "orders"],
        "use_cases":   ["Sales pipeline data", "Transaction logs", "Revenue reports"],
        "steps": [
            {"type": "snake_case_columns",  "config": {}},
            {"type": "drop_duplicates",     "config": {}},
            {"type": "fill_missing",        "config": {"strategy": "constant", "value": 0}},
            {"type": "filter_rows",         "config": {"column": "amount", "operator": "gt", "value": 0}},
            {"type": "cast_types",          "config": {}},
            {"type": "remove_outliers",     "config": {"method": "iqr", "threshold": 2.5}},
            {"type": "derive_column",       "config": {"name": "revenue_category",
                "expression": "amount.apply(lambda x: 'high' if x > 10000 else 'medium' if x > 1000 else 'low')"}},
        ],
        "expected_columns": ["order_id", "amount", "product", "date", "customer"],
        "quality_checks":   ["amount > 0", "no null order IDs", "valid dates"],
        "estimated_time":   "~12s",
    },
    "financial_audit": {
        "id":          "financial_audit",
        "name":        "Financial Data Auditor",
        "description": "Strict financial data cleaning with full audit trail, outlier detection, and regulatory compliance checks.",
        "icon":        "📊",
        "category":    "Sales & Finance",
        "tags":        ["finance", "audit", "compliance", "accounting"],
        "use_cases":   ["GL transactions", "Budget reports", "Expense data"],
        "steps": [
            {"type": "snake_case_columns",  "config": {}},
            {"type": "drop_duplicates",     "config": {}},
            {"type": "cast_types",          "config": {}},
            {"type": "filter_rows",         "config": {"column": "amount", "operator": "neq", "value": 0}},
            {"type": "remove_outliers",     "config": {"method": "zscore", "threshold": 3.0}},
            {"type": "normalize",           "config": {}},
        ],
        "expected_columns": ["transaction_id", "amount", "account", "date", "category"],
        "quality_checks":   ["no null transactions", "amount != 0", "valid accounts"],
        "estimated_time":   "~10s",
    },
    "customer_data": {
        "id":          "customer_data",
        "name":        "Customer Data Cleaner",
        "description": "Standardize customer records, detect and mask PII, deduplicate contacts, validate emails and phones.",
        "icon":        "🧑‍💼",
        "category":    "CRM & Marketing",
        "tags":        ["customers", "crm", "contacts", "pii"],
        "use_cases":   ["CRM exports", "Email lists", "Customer profiles"],
        "steps": [
            {"type": "snake_case_columns",  "config": {}},
            {"type": "drop_duplicates",     "config": {}},
            {"type": "fill_missing",        "config": {"strategy": "constant", "value": "Unknown"}},
            {"type": "cast_types",          "config": {}},
            {"type": "drop_missing",        "config": {"threshold_pct": 60}},
        ],
        "expected_columns": ["customer_id", "name", "email", "phone", "country"],
        "quality_checks":   ["valid emails", "no null IDs", "valid countries"],
        "estimated_time":   "~8s",
    },
    "product_catalog": {
        "id":          "product_catalog",
        "name":        "Product Catalog Normalizer",
        "description": "Clean product data, standardize categories, fill missing descriptions, normalize prices.",
        "icon":        "📦",
        "category":    "E-Commerce",
        "tags":        ["products", "catalog", "inventory", "ecommerce"],
        "use_cases":   ["Product feeds", "Inventory exports", "Catalog data"],
        "steps": [
            {"type": "snake_case_columns",  "config": {}},
            {"type": "drop_duplicates",     "config": {}},
            {"type": "fill_missing",        "config": {"strategy": "mode"}},
            {"type": "filter_rows",         "config": {"column": "price", "operator": "gt", "value": 0}},
            {"type": "normalize",           "config": {"columns": ["price", "stock_quantity"]}},
            {"type": "cast_types",          "config": {}},
        ],
        "expected_columns": ["product_id", "name", "price", "category", "stock"],
        "quality_checks":   ["price > 0", "no null names", "valid categories"],
        "estimated_time":   "~10s",
    },
    "log_analytics": {
        "id":          "log_analytics",
        "name":        "Log Data Processor",
        "description": "Process server/application logs, parse timestamps, extract error patterns, aggregate by time window.",
        "icon":        "📝",
        "category":    "Engineering",
        "tags":        ["logs", "events", "analytics", "monitoring"],
        "use_cases":   ["Server logs", "App events", "Clickstream data"],
        "steps": [
            {"type": "snake_case_columns",  "config": {}},
            {"type": "drop_duplicates",     "config": {}},
            {"type": "cast_types",          "config": {}},
            {"type": "drop_missing",        "config": {"threshold_pct": 80}},
            {"type": "filter_rows",         "config": {"column": "level", "operator": "neq", "value": "DEBUG"}},
        ],
        "expected_columns": ["timestamp", "level", "message", "service", "user_id"],
        "quality_checks":   ["valid timestamps", "valid log levels", "no null messages"],
        "estimated_time":   "~8s",
    },
    "ml_features": {
        "id":          "ml_features",
        "name":        "ML Feature Engineering",
        "description": "Prepare data for machine learning. Normalize all numerics, encode categoricals, remove high-correlation features.",
        "icon":        "🤖",
        "category":    "Data Science",
        "tags":        ["ml", "features", "modeling", "ai"],
        "use_cases":   ["Training datasets", "Feature stores", "Model inputs"],
        "steps": [
            {"type": "snake_case_columns",  "config": {}},
            {"type": "drop_duplicates",     "config": {}},
            {"type": "drop_missing",        "config": {"threshold_pct": 40}},
            {"type": "fill_missing",        "config": {"strategy": "median"}},
            {"type": "remove_outliers",     "config": {"method": "iqr", "threshold": 1.5}},
            {"type": "standardize",         "config": {}},
            {"type": "normalize",           "config": {}},
        ],
        "expected_columns": ["label", "feature_1", "feature_2"],
        "quality_checks":   ["no nulls", "normalized range", "no high-variance outliers"],
        "estimated_time":   "~20s",
    },
    "survey_data": {
        "id":          "survey_data",
        "name":        "Survey Response Cleaner",
        "description": "Clean survey data, standardize Likert scales, remove incomplete responses, normalize text answers.",
        "icon":        "📋",
        "category":    "Research",
        "tags":        ["survey", "research", "responses", "likert"],
        "use_cases":   ["NPS surveys", "Employee surveys", "Research data"],
        "steps": [
            {"type": "snake_case_columns",  "config": {}},
            {"type": "drop_duplicates",     "config": {}},
            {"type": "drop_missing",        "config": {"threshold_pct": 50}},
            {"type": "fill_missing",        "config": {"strategy": "mode"}},
            {"type": "cast_types",          "config": {}},
        ],
        "expected_columns": ["respondent_id", "score", "response", "date"],
        "quality_checks":   ["score in valid range", "no null IDs", "valid dates"],
        "estimated_time":   "~8s",
    },
}


@templates_bp.get("/")
@login_required
def list_templates():
    """List all available pipeline templates."""
    category = request.args.get("category")
    search   = request.args.get("q", "").lower()

    result = list(TEMPLATES.values())

    if category:
        result = [t for t in result if t["category"] == category]
    if search:
        result = [t for t in result if
                  search in t["name"].lower() or
                  search in t["description"].lower() or
                  any(search in tag for tag in t["tags"])]

    categories = list({t["category"] for t in TEMPLATES.values()})
    return jsonify({"templates": result, "categories": categories, "total": len(result)})


@templates_bp.get("/<template_id>")
@login_required
def get_template(template_id):
    """Get a specific template."""
    tpl = TEMPLATES.get(template_id)
    if not tpl:
        return jsonify({"error": "Template not found"}), 404
    return jsonify(tpl)


@templates_bp.post("/<template_id>/apply")
@login_required
def apply_template(template_id):
    """
    Apply a template to a dataset — runs the pipeline immediately.
    """
    tpl  = TEMPLATES.get(template_id)
    if not tpl:
        return jsonify({"error": "Template not found"}), 404

    body  = request.get_json(force=True)
    ds_id = body.get("ds_id")

    from backend.models import Dataset
    ds = Dataset.query.filter_by(ds_id=ds_id, user_id=current_user.id).first_or_404()

    # Trigger pipeline with template steps
    import requests as req_lib
    import flask
    from flask import current_app

    # Call pipeline run internally
    with current_app.test_client() as client:
        resp = client.post("/api/pipeline/run",
            json={
                "ds_id":           ds_id,
                "steps":           tpl["steps"],
                "use_spark":       False,
                "trigger_airflow": False,
                "generate_dbt":    True,
            },
            headers={"Content-Type": "application/json"},
        )
        data = resp.get_json()

    return jsonify({
        "success":       True,
        "template_used": template_id,
        "template_name": tpl["name"],
        **data,
    })


@templates_bp.post("/smart-recommend")
@login_required
def smart_recommend():
    """
    Recommend the best template for a given dataset
    based on column names and profile.
    """
    body  = request.get_json(force=True)
    ds_id = body.get("ds_id")

    from backend.models import Dataset
    ds      = Dataset.query.filter_by(ds_id=ds_id, user_id=current_user.id).first_or_404()
    schema  = ds.schema_json or {}
    cols    = [c.lower() for c in schema.keys()]

    scores = {}
    keyword_map = {
        "hr_data":       ["salary","employee","hire","department","staff","payroll","hr","name","performance"],
        "sales_data":    ["sale","revenue","order","amount","product","customer","invoice","price","deal"],
        "financial_audit":["transaction","account","debit","credit","gl","budget","expense","ledger"],
        "customer_data": ["customer","email","phone","contact","client","crm","address","user"],
        "product_catalog":["product","sku","inventory","stock","category","catalog","item","price"],
        "log_analytics": ["log","event","timestamp","level","error","service","request","response"],
        "ml_features":   ["feature","label","target","class","score","prediction","model","train"],
        "survey_data":   ["survey","response","question","answer","rating","nps","feedback","score"],
    }

    for tpl_id, keywords in keyword_map.items():
        score = sum(1 for kw in keywords if any(kw in col for col in cols))
        scores[tpl_id] = score

    best_id    = max(scores, key=scores.get)
    best_score = scores[best_id]

    if best_score == 0:
        return jsonify({"recommended": None, "message": "No strong match found. Browse all templates."})

    return jsonify({
        "recommended": TEMPLATES[best_id],
        "confidence":  "high" if best_score >= 3 else "medium" if best_score >= 1 else "low",
        "score":       best_score,
        "all_scores":  {k: v for k, v in sorted(scores.items(), key=lambda x: -x[1]) if v > 0},
    })