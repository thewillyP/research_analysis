import argparse
import wandb
import polars as pl
import psycopg2
from psycopg2.extras import Json
from concurrent.futures import ThreadPoolExecutor
import json
import os


def create_table():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("PGPORT"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            id TEXT PRIMARY KEY,
            entity TEXT,
            project TEXT,
            group_name TEXT,
            url TEXT,
            summary JSONB,
            config JSONB,
            file_path TEXT
        );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_summary_gin ON runs USING GIN (summary);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_config_gin ON runs USING GIN (config);")

    conn.commit()
    cur.close()
    conn.close()


def store_run(run, file_path):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("PGPORT"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    # Convert summary and config to JSON-serializable dictionaries
    summary_dict = run.summary._json_dict
    config_dict = dict(run.config)

    cur.execute(
        """
        INSERT INTO runs (id, entity, project, group_name, url, summary, config, file_path)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE 
        SET entity = EXCLUDED.entity,
            project = EXCLUDED.project,
            group_name = EXCLUDED.group_name,
            url = EXCLUDED.url,
            summary = EXCLUDED.summary,
            config = EXCLUDED.config,
            file_path = EXCLUDED.file_path;
    """,
        (
            run.id,
            run.entity,
            run.project,
            run.group,
            run.url,
            Json(summary_dict),
            Json(config_dict),
            file_path,
        ),
    )

    conn.commit()
    cur.close()
    conn.close()


def process_run(run, root_dir):
    try:
        api = wandb.Api()
        artifact_name = f"{run.entity}/{run.project}/run-{run.id}-history:v0"
        artifact = api.artifact(artifact_name)

        # Download to a run-specific directory, only the 0000.parquet file
        run_dir = os.path.join(root_dir, f"run-{run.id}")
        os.makedirs(run_dir, exist_ok=True)
        download_path = artifact.download(root=run_dir, path_prefix="0000.parquet")
        store_run(run, f"{download_path}/0000.parquet")
        print(f"Processed run {run.id}")
    except Exception as e:
        print(f"Error processing run {run.id}: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description="Download and process wandb runs")
    parser.add_argument("entity", help="Wandb entity name")
    parser.add_argument("project", help="Wandb project name")
    parser.add_argument("--group", help="Wandb group name", default=None)
    args = parser.parse_args()

    # Create table if it doesn't exist
    create_table()

    api = wandb.Api()
    filters = {}
    if args.group:
        filters["group"] = args.group
    runs = api.runs(f"{args.entity}/{args.project}", filters)

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(process_run, run, "/wandb_data") for run in runs]
        for future in futures:
            future.result()


if __name__ == "__main__":
    main()
