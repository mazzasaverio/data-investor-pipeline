from dotenv import load_dotenv
import os
from loguru import logger  # Using Loguru for logging

# Load environment variables
load_dotenv()

try:
    DATABASE_URL = os.getenv("DATABASE_URL")
    FMP_API_KEY = os.getenv("FMP_SECRET_KEY")

    if DATABASE_URL is None:
        logger.warning("DATABASE_URL environment variable is missing.")
    if FMP_API_KEY is None:
        logger.warning("FMP_SECRET_KEY environment variable is missing.")

    logger.info("Successfully loaded environment variables.")
except Exception as e:
    logger.exception("Failed to load environment variables: {}", e)
