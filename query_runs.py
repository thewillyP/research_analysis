import psycopg2
import polars as pl
import os
from typing import List, Dict, Any, Union


def normalize_field(f: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(f, str):
        return {"field": f, "type": "json" if "." in f else "row"}
    elif isinstance(f, dict):
        result = f.copy()
        if "fn" in result:
            result.setdefault("type", "function")
        else:
            result.setdefault("type", "json" if "." in result["field"] else "row")
        return result
    else:
        raise ValueError(f"Invalid field format: {f}")


def query_runs(
    select_fields: List[Union[str, Dict[str, Any]]],
    filter_conditions: List[Union[str, Dict[str, Any]]],
    group_by_fields: List[Union[str, Dict[str, Any]]],
) -> pl.DataFrame:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("PGPORT"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    # Normalize input
    select_fields = [normalize_field(f) for f in (select_fields or ["id"])]
    filter_conditions = [normalize_field(f) for f in (filter_conditions or [])]
    group_by_fields = [normalize_field(f) for f in (group_by_fields or [])]

    # Build expressions for SELECT
    select_exprs = []
    parquet_selects = []
    expr_to_alias = {}

    for sel in select_fields:
        if sel["type"] == "row":
            expr = sel["field"]
        elif sel["type"] == "json":
            parts = sel["field"].split(".", 1)
            expr = f"{parts[0]} ->> '{parts[1]}'"
        elif sel["type"] == "function":
            expr = "id"
            parquet_selects.append(sel)
        else:
            continue
        # Set a clean alias
        alias = expr.replace("->>", "_").replace("'", "").replace(".", "_").replace(" ", "")
        expr_to_alias[expr] = alias
        select_exprs.append(expr)

    select_exprs.append("file_path")
    expr_to_alias["file_path"] = "file_path"

    # WHERE clause
    where_clause = []
    params = []
    parquet_filters = []

    for filt in filter_conditions:
        operator = filt.get("operator", "=")
        if filt["type"] == "row":
            where_clause.append(f"{filt['field']} {operator} %s")
            params.append(filt["value"])
        elif filt["type"] == "json":
            parts = filt["field"].split(".", 1)
            where_clause.append(f"{parts[0]} ->> '{parts[1]}' {operator} %s")
            params.append(filt["value"])
        elif filt["type"] == "function":
            parquet_filters.append(filt)

    where_clause_str = " AND ".join(where_clause) if where_clause else "1=1"

    # GROUP BY clause
    group_by_clause = ""
    grouped_exprs = set()
    parquet_groups = []

    if group_by_fields:
        group_items = []
        for g in group_by_fields:
            if g["type"] == "row":
                expr = g["field"]
            elif g["type"] == "json":
                parts = g["field"].split(".", 1)
                expr = f"{parts[0]} ->> '{parts[1]}'"
            else:
                expr = "id"
                parquet_groups.append(g)
            group_items.append(expr)
            grouped_exprs.add(expr)
        group_by_clause = f"GROUP BY {', '.join(group_items)}"

    # Final SELECT clause
    final_selects = []
    for expr in select_exprs:
        alias = expr_to_alias[expr]
        if group_by_fields and expr not in grouped_exprs:
            final_selects.append(f"ARRAY_AGG({expr}) AS {alias}_agg")
        else:
            final_selects.append(f"{expr} AS {alias}")

    select_clause_str = ", ".join(final_selects)

    query = f"""
        SELECT {select_clause_str}
        FROM runs
        WHERE {where_clause_str}
        {group_by_clause};
    """

    cur.execute(query, params)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    df = pl.DataFrame(rows, schema=columns, orient="row")

    # Parquet logic
    if parquet_selects or parquet_filters or parquet_groups:
        result_rows = []
        for row in rows:
            file_path = row[columns.index("file_path")]
            run_id = row[columns.index("id")] if "id" in columns else None
            try:
                parquet_df = pl.read_parquet(file_path)

                filtered_df = parquet_df
                for filt in parquet_filters:
                    result = filt["fn"](filtered_df)
                    if not isinstance(result, bool) or not result:
                        filtered_df = filtered_df.filter(pl.lit(False))

                row_data = {col: val for col, val in zip(columns, row)}
                for sel in parquet_selects:
                    result = sel["fn"](filtered_df)
                    row_data[sel["field"]] = result

                result_rows.append(row_data)
            except Exception as e:
                print(f"Error processing parquet for run {run_id}: {str(e)}")

        df = pl.DataFrame(result_rows)

        if parquet_groups:
            group_cols = [g["field"] for g in group_by_fields if g["type"] in ["row", "json"]]
            parquet_group_cols = [g["field"] for g in parquet_groups]
            if parquet_group_cols:
                df = df.group_by(group_cols + parquet_group_cols).agg(
                    **{col: pl.col(col).list() for col in df.columns if col not in group_cols + parquet_group_cols}
                )

    cur.close()
    conn.close()
    return df


if __name__ == "__main__":
    result = query_runs(
        select_fields=[
            "id",
            "entity",
            "project",
            "summary.test_accuracy",
            "config.meta_optimizer",
        ],
        filter_conditions=[{"field": "summary.test_accuracy", "value": "0.85", "operator": "!="}],
        group_by_fields=[],
    )
    print(result)
