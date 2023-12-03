from sqlalchemy import create_engine
from database.models import Base
import os


def setup_database(engine):
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    DATABASE_URL = os.getenv("DATABASE_URL")
    print(DATABASE_URL)
    engine = create_engine(DATABASE_URL, connect_args={"ssl": {"ssl-mode": "REQUIRED"}})

    setup_database(engine)
