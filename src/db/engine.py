# db/engine.py
from sqlalchemy import create_engine

ENGINE = create_engine(
    "postgresql+psycopg2://merton_user:merton_dev_pw@localhost:5432/merton_db"
)
