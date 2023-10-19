from sqlalchemy import create_engine
from database.models import Base
from common.config import (
    DATABASE_URL,
)  # Import from the consolidated config file

engine = create_engine(DATABASE_URL)


def setup_database():
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    setup_database()
