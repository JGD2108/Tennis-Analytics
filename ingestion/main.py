import pandas as pd
import sqlalchemy as sa

#Steps of our script
#1 Read CSV
#2 Normalize Columns
#3 Genereate Match ID (Primary Key), Synthetic prmary key match_id=hash(tourney_id,match_num)
#4 Add metadata (source_file, ingested_at)
#5 Insert Rows
#6 Database enforces uniqueness 

# To handle correctness we need: Idempotent + updates (UPSERT/ON CONFLICT DO UPDATE) 
# Idempotent upsert with full overwrite
import sys
from pathlib import Path

from config import load_config
from db import create_engine_from_config, ensure_table_exists, upsert_batch
from file_processor import process_file, upload_raw_to_minio
from dotenv import load_dotenv
load_dotenv()



BATCH_SIZE = 5000


def main():
    try:
        # 1. Load configuration
        config = load_config()

        # 2. Create DB engine
        engine = create_engine_from_config(config)

        # 3. Scan CSV directory
        csv_files = sorted(config.csv_dir.glob("*.csv"))

        if not csv_files:
            print("No CSV files found.")
            return

        print(f"Found {len(csv_files)} CSV files.")

        table_initialized = False
        total_rows = 0

        # 4. Process each file
        for file_path in csv_files:
            print(f"\nProcessing file: {file_path.name}")

            try:
                upload_raw_to_minio(file_path)
                df = process_file(file_path)

                # 5. Ensure table exists (only once)
                if not table_initialized:
                    ensure_table_exists(engine, config, df.columns.tolist())
                    table_initialized = True

                # 6. Insert in batches
                for start in range(0, len(df), BATCH_SIZE):
                    batch = df.iloc[start:start + BATCH_SIZE]
                    upsert_batch(engine, config, batch)

                print(f"Inserted/Updated {len(df)} rows.")
                total_rows += len(df)

            except Exception as file_error:
                print(f"Error processing {file_path.name}: {file_error}")
                print("Skipping file...")

        print(f"\nDone. Total rows processed: {total_rows}")

    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
