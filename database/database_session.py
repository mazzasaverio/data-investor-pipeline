from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from loguru import logger
from dotenv import load_dotenv
import os

load_dotenv()


def initialize_db(connection_type):
    """
    Initialize the database connection.

    Args:
    connection_type (str): Type of connection ('local' or 'production').
    """

    if connection_type == "PROD":
        # Connection details for production database (MySQL)
        db_url = f"mysql+pymysql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        ssl_args = {"ssl": {"ca": "/etc/ssl/certs/ca-certificates.crt"}}
        connect_args = ssl_args
    elif connection_type == "LOCAL":
        # Connection details for local database (PostgreSQL)
        db_url = f"postgresql://{os.getenv('DB_USERNAME_LOCAL')}:{os.getenv('DB_PASSWORD_LOCAL')}@{os.getenv('DB_HOST_LOCAL')}/{os.getenv('DB_NAME_LOCAL')}"
        connect_args = {}
    else:
        raise ValueError("Invalid connection type specified")

    try:
        engine = create_engine(db_url, connect_args=connect_args)
        session_local = sessionmaker(autoflush=False, bind=engine)
        logger.info(f"Successfully initialized the {connection_type} database session.")
        return session_local, engine
    except Exception as e:
        logger.exception(
            f"An error occurred while initializing the {connection_type} database: {e}"
        )
