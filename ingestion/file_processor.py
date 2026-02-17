import pandas as pd 
import hashlib
from datetime import datetime,timezone
from pathlib import Path
from typing import List
import boto3
import os

REQUIRED_COLUMNS = ["tourney_id", "match_num"]

def upload_raw_to_minio(file_path:Path) ->None:
    """
    Upload raw file to MinIO with timestamped object key. 
    Returns the object key used
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD")
    )
    
    bucket_name = "tennis-data"
    
    #ISO timestamp without special character for safety
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    object_key = f"raw/{timestamp}_{file_path.name}"
    s3.upload_file(str(file_path), bucket_name, object_key)
    print(f"Uploaded {file_path.name} to {bucket_name}/{object_key}")
    
    return object_key

def normalize_columns(df:pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to lowercase and strip whitespace.
    """
    df = df.copy()
    df.columns = [col.strip().lower() for col in df.columns]
    return df

def validate_required_columns(df:pd.DataFrame, file_path:Path)-> None:
    """
    Validate that all required columns are present in the DataFrame.
    Raises ValueError if any are missing.
    """
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"File {file_path.name} missing required columns {', '.join(missing)}"
        )

def validate_no_null_keys(df: pd.DataFrame, file_path:Path) ->None:
    '''
        Ensure no null or empty values in key columns
    '''
    for col in REQUIRED_COLUMNS:
        if df[col].isnull().any():
            raise ValueError(
                f"File {file_path.name} contains NULL values in required column '{col}'"
            )

        if (df[col].astype(str).str.strip() == "").any():
            raise ValueError(
                f"File {file_path.name} contains empty values in required column '{col}'"
            )

def generate_match_id(row:pd.Series) -> str:
    """
        Deterministic hash based on tourney_id and match_num. 
        Uses delimiter to avoid string collision 
        Example: 01 | 2021
    """
    raw_string = f"{row['tourney_id']}|{row['match_num']}"
    return hashlib.sha256(raw_string.encode("utf-8")).hexdigest()


def process_file(file_path:Path) -> pd.DataFrame:
    """
    Read, validate, and enrich a CSV file.
    Returns a DataFrame ready for database upsert.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    df = pd.read_csv(file_path, low_memory=False)
    df = normalize_columns(df)

    validate_required_columns(df, file_path)
    validate_no_null_keys(df, file_path)

    df = df.copy()
    df["match_id"] = df.apply(generate_match_id, axis=1)
    df["source_file"] = file_path.name
    df["ingested_at"] = datetime.now(timezone.utc)

    return df