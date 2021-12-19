"""Database connection management"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

DB_CONNECTION_STRING = f"{os.getenv('DB_CONNECTION_DRIVER')}:///{os.getenv('SQLITE_DB_FILE')}"

db_engine = create_engine(DB_CONNECTION_STRING)

Session = sessionmaker(bind=db_engine)
