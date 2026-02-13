import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import List
from config import Config


def create_engine_from_config(config: Config) -> Engine:
    """
    Create and return a SQLAlchemy engine using configuration.
    """
    connection_url = (
        f"postgresql+psycopg2://{config.db_user}:"
        f"{config.db_password}@"
        f"{config.db_host}:"
        f"{config.db_port}/"
        f"{config.db_name}"
    )

    engine = create_engine(connection_url, pool_pre_ping=True)
    return engine


def ensure_table_exists(engine: Engine, config: Config, columns: List[str]) -> None:
    """
    Create raw_matches table if it does not exist.
    All CSV columns are stored as TEXT.
    match_id is PRIMARY KEY.
    ingested_at is TIMESTAMPTZ.
    """

    # Build dynamic column definitions
    column_definitions = []

    for col in columns:
        if col == "ingested_at":
            column_definitions.append(f"{col} TIMESTAMPTZ")
        elif col == "match_id":
            continue  # handled separately
        else:
            column_definitions.append(f"{col} TEXT")

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {config.table_name} (
        match_id TEXT PRIMARY KEY,
        {', '.join(column_definitions)}
    );
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))


def upsert_batch(engine: Engine, config: Config, df: pd.DataFrame) -> None:
    """
    Perform UPSERT (insert or update) for a batch of rows.
    Overwrites all columns on conflict.
    Runs inside a transaction.
    """

    if df.empty:
        return

    columns = list(df.columns)

    # Build SET clause for update (exclude match_id)
    update_columns = [col for col in columns if col != "match_id"]
    set_clause = ", ".join(
        [f"{col} = EXCLUDED.{col}" for col in update_columns]
    )

    column_list = ", ".join(columns)
    placeholders = ", ".join([f":{col}" for col in columns])

    sql = f"""
    INSERT INTO {config.table_name} ({column_list})
    VALUES ({placeholders})
    ON CONFLICT (match_id)
    DO UPDATE SET
    {set_clause};
    """

    # Convert DataFrame to records
    records = df.to_dict(orient="records")

    # Transaction per batch
    with engine.begin() as conn:
        conn.execute(text(sql), records)