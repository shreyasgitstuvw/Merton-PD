import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
    "dbname": os.getenv("DB_NAME", "merton_db"),
    "user": os.getenv("DB_USER", "merton_user"),
    "password": os.getenv("DB_PASSWORD", "merton_dev_pw"),
}
