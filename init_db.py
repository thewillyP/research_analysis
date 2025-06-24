import psycopg2
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


if __name__ == "__main__":
    create_table()
