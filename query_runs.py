import psycopg2
import polars as pl
import json
from typing import List, Dict, Callable, Any
import os


def query_runs(
    select_fields: List[Dict[str, Any]], filter_conditions: List[Dict[str, Any]], group_by_fields: List[Dict[str, Any]]
) -> pl.DataFrame:
    """
    Query runs with flexible selection, filtering, and grouping on row, JSONB, or parquet-derived fields.

    Args:
        select_fields: List of dicts with 'field' (row/json path), 'type' (row/json/function), optional 'fn' for function
        filter_conditions: List of dicts with 'field', 'type', 'value', optional 'fn' for function
        group_by_fields: List of dicts with 'field', 'type', optional 'fn' for function

    Returns:
        Polars DataFrame with query results
    """
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("PGPORT"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    # Default fields if none specified
    select_fields = select_fields or [{"field": "id", "type": "row"}]

    # Build SELECT clause
    select_clause = []
    parquet_selects = []
    for sel in select_fields:
        if sel["type"] == "row":
            select_clause.append(sel["field"])
        elif sel["type"] == "json":
            parts = sel["field"].split(".", 1)
            select_clause.append(f"{parts[0]} ->> '{parts[1]}' AS {sel['field'].replace('.', '_')}")
        elif sel["type"] == "function":
            select_clause.append("id")  # Need ID to correlate with parquet results
            parquet_selects.append(sel)

    select_clause = ", ".join(select_clause) or "id"

    # Build WHERE clause
    where_clause = []
    params = []
    parquet_filters = []
    for filt in filter_conditions or []:
        if filt["type"] in ["row", "json"]:
            if filt["type"] == "row":
                where_clause.append(f"{filt['field']} = %s")
            else:
                parts = filt["field"].split(".", 1)
                where_clause.append(f"{parts[0]} ->> '{parts[1]}' = %s")
            params.append(filt["value"])
        elif filt["type"] == "function":
            parquet_filters.append(filt)

    where_clause_str = " AND ".join(where_clause) if where_clause else "1=1"

    # Build GROUP BY clause
    group_by_clause = ""
    parquet_groups = []
    if group_by_fields:
        group_by_clause = "GROUP BY " + ", ".join(
            g["field"] if g["type"] in ["row", "json"] else "id" for g in group_by_fields
        )
        select_clause = ", ".join(
            f"ARRAY_AGG({s}) AS {s}_array"
            if any(s == g["field"] for g in group_by_fields if g["type"] in ["row", "json"])
            else s
            for s in select_clause.split(", ")
        )
        parquet_groups = [g for g in group_by_fields if g["type"] == "function"]

    # Execute SQL query
    query = f"""
        SELECT {select_clause}, file_path
        FROM runs
        WHERE {where_clause_str}
        {group_by_clause};
    """

    cur.execute(query, params)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    # Convert to Polars DataFrame
    df = pl.DataFrame(rows, schema=columns)

    # Process parquet-based operations
    if parquet_selects or parquet_filters or parquet_groups:
        result_rows = []
        for row in rows:
            file_path = row[-1]  # file_path is last column
            run_id = row[columns.index("id")]
            try:
                parquet_df = pl.read_parquet(file_path)

                # Apply parquet filters
                filtered_df = parquet_df
                for filt in parquet_filters:
                    result = filt["fn"](filtered_df)
                    if not isinstance(result, bool) or not result:
                        filtered_df = filtered_df.filter(pl.lit(False))

                # Apply parquet selects
                row_data = {col: val for col, val in zip(columns[:-1], row[:-1])}
                for sel in parquet_selects:
                    result = sel["fn"](filtered_df)
                    row_data[sel["field"]] = result

                result_rows.append(row_data)
            except Exception as e:
                print(f"Error processing parquet for run {run_id}: {str(e)}")

        df = pl.DataFrame(result_rows)

        # Apply parquet grouping
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
            {"field": "id", "type": "row"},
            {"field": "entity", "type": "row"},
            {"field": "project", "type": "row"},
            {"field": "summary.test_accuracy", "type": "json"},
            {"field": "config.lr", "type": "json"},
        ],
        filter_conditions=[],
        group_by_fields=[],
    )
    print(result)
