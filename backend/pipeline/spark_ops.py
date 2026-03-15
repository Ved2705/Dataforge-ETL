"""
spark_ops.py — PySpark ETL transforms
Uses local[*] Spark session. Falls back to pandas if PySpark unavailable.
config = {
    "operation": "aggregate|join|window|sql",
    "sql": "SELECT ...",          # for sql mode
    "group_by": [...],
    "agg": {"col": "sum|avg|count|max|min"},
    "window": {"partition_by": [...], "order_by": [...], "fn": "row_number|rank|lag|lead"}
}
"""
import pandas as pd
import numpy as np


def run_spark_transform(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, dict]:
    try:
        return _spark_transform(df, config)
    except ImportError:
        return _pandas_fallback(df, config)


# ─────────────────────────────────────────────────────
# REAL PYSPARK
# ─────────────────────────────────────────────────────
def _spark_transform(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, dict]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    spark = SparkSession.builder \
        .appName("DataForge-ETL") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    sdf = spark.createDataFrame(df)
    op  = config.get("operation", "passthrough")
    log = {"engine": "pyspark", "operation": op}

    if op == "sql":
        sdf.createOrReplaceTempView("dataforge_table")
        sdf = spark.sql(config["sql"])
        log["sql"] = config["sql"]

    elif op == "aggregate":
        group_by = config.get("group_by", [])
        agg_map  = config.get("agg", {})
        agg_exprs = []
        for col, fn in agg_map.items():
            agg_exprs.append(getattr(F, fn)(col).alias(f"{col}_{fn}"))
        sdf = sdf.groupBy(*group_by).agg(*agg_exprs)
        log["grouped_by"] = group_by

    elif op == "window":
        part  = config.get("window", {}).get("partition_by", [])
        order = config.get("window", {}).get("order_by", [])
        fn    = config.get("window", {}).get("fn", "row_number")
        col   = config.get("window", {}).get("column")
        w     = Window.partitionBy(*part).orderBy(*order)
        fn_map = {
            "row_number": F.row_number(),
            "rank":       F.rank(),
            "dense_rank": F.dense_rank(),
            "lag":        F.lag(col, 1) if col else F.lag(order[0], 1),
            "lead":       F.lead(col, 1) if col else F.lead(order[0], 1),
        }
        sdf = sdf.withColumn(f"{fn}_col", fn_map.get(fn, F.row_number()).over(w))
        log["window_fn"] = fn

    elif op == "deduplicate":
        subset = config.get("subset")
        sdf = sdf.dropDuplicates(subset) if subset else sdf.dropDuplicates()

    elif op == "drop_nulls":
        cols = config.get("columns")
        sdf = sdf.dropna(subset=cols) if cols else sdf.dropna()

    result_df = sdf.toPandas()
    log["rows_out"] = len(result_df)
    spark.stop()
    return result_df, log


# ─────────────────────────────────────────────────────
# PANDAS FALLBACK
# ─────────────────────────────────────────────────────
def _pandas_fallback(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, dict]:
    op  = config.get("operation", "passthrough")
    log = {"engine": "pandas_fallback", "operation": op,
           "note": "PySpark not available — using pandas"}

    if op == "aggregate":
        group_by = config.get("group_by", [])
        agg_map  = config.get("agg", {})
        if group_by and agg_map:
            df = df.groupby(group_by).agg(agg_map).reset_index()

    elif op == "deduplicate":
        subset = config.get("subset")
        df = df.drop_duplicates(subset=subset)

    elif op == "drop_nulls":
        cols = config.get("columns")
        df = df.dropna(subset=cols) if cols else df.dropna()

    elif op == "window":
        # simple row-number via pandas groupby cumcount
        part  = config.get("window", {}).get("partition_by", [])
        fn    = config.get("window", {}).get("fn", "row_number")
        if part:
            df[f"{fn}_col"] = df.groupby(part).cumcount() + 1
        else:
            df[f"{fn}_col"] = range(1, len(df)+1)

    log["rows_out"] = len(df)
    return df, log
