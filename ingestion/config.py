import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    csv_dir: Path
    table_name: str
    

def load_config() -> Config:
    dotenv_path = Path(__file__).with_name(".env")
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)
    else:
        load_dotenv()
    
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    csv_dir = os.getenv("CSV_DIR")
    table_name = os.getenv("TABLE_NAME")
    
    required_vars = {
        "DB_HOST": db_host,
        "DB_PORT": db_port,
        "DB_NAME": db_name,
        "DB_USER": db_user,
        "DB_PASSWORD": db_password,
        "CSV_DIR": csv_dir,
        "TABLE_NAME": table_name
    }
    
    missing = [key for key, value in required_vars.items() if not value]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    try:
        db_port = int(db_port)
    except ValueError:
        raise ValueError("DB_PORT must be an integer")

    # Resolve CSV_DIR relative to this file if a relative path is provided.
    csv_path = Path(csv_dir)
    if not csv_path.is_absolute():
        csv_path = (Path(__file__).resolve().parent / csv_path).resolve()
    if not csv_path.exists() or not csv_path.is_dir():
        raise ValueError(f"CSV_DIR does not exist or is not a directory: {csv_dir}")
    
    return Config(
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
        db_user=db_user,
        db_password=db_password,
        csv_dir=csv_path,
        table_name=table_name
    )
        
    
