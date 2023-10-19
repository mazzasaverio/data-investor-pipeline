from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from loguru import logger
from common.config import DATABASE_URL

try:
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    logger.info("Successfully initialized the database session.")
except Exception as e:
    logger.exception(f"An error occurred while initializing the database: {e}")
