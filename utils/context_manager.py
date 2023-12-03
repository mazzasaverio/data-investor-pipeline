from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
from loguru import logger


@contextmanager
def session_scope(SessionLocal, run_id):
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
        logger.bind(run_id=run_id).info("Session committed successfully.")
    except Exception as e:
        session.rollback()
        logger.bind(run_id=run_id).exception(f"An error occurred: {e}")
    finally:
        session.close()
